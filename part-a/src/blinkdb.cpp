#include "blinkdb.h"

#include <sqlite3.h>
#include <zstd.h>

#include <algorithm>
#include <chrono>
#include <charconv>
#include <exception>
#include <iostream>
#include <limits>
#include <mutex>
#include <random>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace {

constexpr int BLINKDB_APPLICATION_ID = 0x424C4442;
constexpr std::string_view RAW_MARKER = "BLK2R:";
constexpr std::string_view ZSTD_MARKER = "BLK2Z:";
constexpr std::size_t COMPRESSION_THRESHOLD = 256;

std::string encodeValue(std::string_view value) {
    const std::string raw_header =
        std::string(RAW_MARKER) + std::to_string(value.size()) + ":";
    if (value.size() >= COMPRESSION_THRESHOLD) {
        std::string compressed(ZSTD_compressBound(value.size()), '\0');
        const std::size_t compressed_size = ZSTD_compress(
            compressed.data(), compressed.size(), value.data(), value.size(), 3);
        if (ZSTD_isError(compressed_size)) {
            throw std::runtime_error(std::string("Compressing value: ") +
                                     ZSTD_getErrorName(compressed_size));
        }
        if (compressed_size < value.size()) {
            compressed.resize(compressed_size);
            return std::string(ZSTD_MARKER) + std::to_string(value.size()) + ":" + compressed;
        }
    }
    return raw_header + std::string(value);
}

std::string decodeValue(std::string_view stored, std::uint64_t max_size) {
    const bool compressed =
        stored.substr(0, ZSTD_MARKER.size()) == ZSTD_MARKER;
    const bool raw = stored.substr(0, RAW_MARKER.size()) == RAW_MARKER;
    if (raw || compressed) {
        const std::size_t length_end = stored.find(':', RAW_MARKER.size());
        if (length_end == std::string_view::npos || length_end == RAW_MARKER.size()) {
            throw std::runtime_error("Stored value has a malformed length header");
        }
        std::uint64_t declared_size = 0;
        const char* begin = stored.data() + RAW_MARKER.size();
        const char* end = stored.data() + length_end;
        const auto parsed = std::from_chars(begin, end, declared_size);
        if (parsed.ec != std::errc{} || parsed.ptr != end || declared_size > max_size ||
            declared_size > std::numeric_limits<std::size_t>::max()) {
            throw std::runtime_error("Stored value has an invalid or oversized length header");
        }
        stored.remove_prefix(length_end + 1);
        if (raw) {
            if (stored.size() != declared_size) {
                throw std::runtime_error("Stored raw value length does not match its header");
            }
            return std::string(stored);
        }
        const unsigned long long frame_size =
            ZSTD_getFrameContentSize(stored.data(), stored.size());
        if (frame_size == ZSTD_CONTENTSIZE_ERROR || frame_size == ZSTD_CONTENTSIZE_UNKNOWN ||
            frame_size != declared_size ||
            frame_size > std::numeric_limits<std::size_t>::max()) {
            throw std::runtime_error("Stored compressed value has an invalid or oversized frame");
        }
        std::string result(static_cast<std::size_t>(frame_size), '\0');
        const std::size_t decoded_size =
            ZSTD_decompress(result.data(), result.size(), stored.data(), stored.size());
        if (ZSTD_isError(decoded_size) || decoded_size != result.size()) {
            throw std::runtime_error("Stored compressed value is corrupt");
        }
        return result;
    }
    if (stored.size() > max_size) {
        throw std::length_error("Stored value exceeds the configured decompressed size limit");
    }
    return std::string(stored);
}

std::runtime_error sqliteError(sqlite3* db, const std::string& operation, int code) {
    return std::runtime_error(operation + ": " +
                              (db ? sqlite3_errmsg(db) : sqlite3_errstr(code)) +
                              " (SQLite code " + std::to_string(code) + ")");
}

void checkSqlite(sqlite3* db, int code, const std::string& operation) {
    if (code != SQLITE_OK) {
        throw sqliteError(db, operation, code);
    }
}

void executeSql(sqlite3* db, const std::string& sql) {
    char* error_message = nullptr;
    const int code = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
    if (code != SQLITE_OK) {
        const std::string detail = error_message ? error_message : sqlite3_errmsg(db);
        sqlite3_free(error_message);
        throw std::runtime_error("SQLite statement failed: " + detail +
                                 " (SQLite code " + std::to_string(code) + ")");
    }
}

class Statement {
public:
    Statement(sqlite3* db, const char* sql) : db_(db) {
        checkSqlite(db_, sqlite3_prepare_v2(db_, sql, -1, &statement_, nullptr),
                    "Preparing SQLite statement");
    }

    ~Statement() {
        sqlite3_finalize(statement_);
    }

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    sqlite3_stmt* get() const noexcept {
        return statement_;
    }

private:
    sqlite3* db_;
    sqlite3_stmt* statement_ = nullptr;
};

void bindText(sqlite3* db, sqlite3_stmt* statement, int index, std::string_view value) {
    checkSqlite(db,
                sqlite3_bind_text64(statement, index, value.data(),
                                    static_cast<sqlite3_uint64>(value.size()),
                                    SQLITE_TRANSIENT, SQLITE_UTF8),
                "Binding SQLite text");
}

void bindBlob(sqlite3* db, sqlite3_stmt* statement, int index, std::string_view value) {
    const int code = value.empty()
                         ? sqlite3_bind_zeroblob(statement, index, 0)
                         : sqlite3_bind_blob64(statement, index, value.data(),
                                               static_cast<sqlite3_uint64>(value.size()),
                                               SQLITE_TRANSIENT);
    checkSqlite(db, code, "Binding SQLite blob");
}

void bindMutation(sqlite3* db, sqlite3_stmt* statement, const StorageMutation& mutation) {
    bindText(db, statement, 1, mutation.dataset);
    bindBlob(db, statement, 2, mutation.key);
    if (mutation.kind == StorageMutation::Kind::Set) {
        bindBlob(db, statement, 3, mutation.value);
    }
}

int readIntegerPragma(sqlite3* db, const char* sql) {
    Statement statement(db, sql);
    const int code = sqlite3_step(statement.get());
    if (code != SQLITE_ROW) {
        throw sqliteError(db, "Reading SQLite setting", code);
    }
    return sqlite3_column_int(statement.get(), 0);
}

void runBackup(sqlite3* destination, sqlite3* source) {
    sqlite3_backup* backup = sqlite3_backup_init(destination, "main", source, "main");
    if (!backup) {
        throw sqliteError(destination, "Starting SQLite backup", sqlite3_errcode(destination));
    }

    int code = SQLITE_OK;
    int retries = 0;
    do {
        code = sqlite3_backup_step(backup, -1);
        if (code == SQLITE_BUSY || code == SQLITE_LOCKED) {
            if (++retries > 500) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    } while (code == SQLITE_OK || code == SQLITE_BUSY || code == SQLITE_LOCKED);

    const int finish_code = sqlite3_backup_finish(backup);
    if (code != SQLITE_DONE) {
        throw sqliteError(destination, "Copying SQLite database", code);
    }
    if (finish_code != SQLITE_OK) {
        throw sqliteError(destination, "Finishing SQLite backup", finish_code);
    }
}

std::string sqlitePath(const std::filesystem::path& path) {
    return path.u8string();
}

} // namespace

class StorageEngine::Impl {
public:
    explicit Impl(StorageEngineOptions configured_options)
        : options(std::move(configured_options)) {
        if (options.data_directory.empty()) {
            throw std::invalid_argument("The database data directory must not be empty");
        }
        if (options.database_filename.empty() ||
            options.database_filename == "." ||
            options.database_filename == ".." ||
            options.database_filename.find('\0') != std::string::npos ||
            std::filesystem::path(options.database_filename).filename() !=
                std::filesystem::path(options.database_filename)) {
            throw std::invalid_argument("The database filename must be a single path component");
        }
        if (options.busy_timeout_ms < 0) {
            throw std::invalid_argument("The SQLite busy timeout must not be negative");
        }
        if (options.memory_limit_bytes == 0) {
            throw std::invalid_argument("The SQLite memory limit must be greater than zero");
        }
        if (options.max_dataset_name_bytes == 0 || options.max_key_bytes == 0 ||
            options.max_value_bytes == 0) {
            throw std::invalid_argument("Dataset, key, and value limits must be greater than zero");
        }

        std::filesystem::create_directories(options.data_directory);
        path = std::filesystem::absolute(options.data_directory / options.database_filename);

        const std::string utf8_path = sqlitePath(path);
        const int open_code = sqlite3_open_v2(
            utf8_path.c_str(), &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            nullptr);
        if (open_code != SQLITE_OK) {
            const auto error = sqliteError(db, "Opening database " + utf8_path, open_code);
            if (db) {
                sqlite3_close_v2(db);
                db = nullptr;
            }
            throw error;
        }

        try {
            checkSqlite(db, sqlite3_extended_result_codes(db, 1),
                        "Enabling extended SQLite errors");
            checkSqlite(db, sqlite3_busy_timeout(db, options.busy_timeout_ms),
                        "Setting SQLite busy timeout");
            const std::uint64_t sqlite_limit = std::numeric_limits<int>::max();
            std::uint64_t row_limit =
                std::min(options.max_dataset_name_bytes, sqlite_limit);
            row_limit += std::min(options.max_key_bytes, sqlite_limit - row_limit);
            row_limit += std::min(options.max_value_bytes, sqlite_limit - row_limit);
            row_limit += std::min<std::uint64_t>(1024, sqlite_limit - row_limit);
            sqlite3_limit(db, SQLITE_LIMIT_LENGTH, static_cast<int>(row_limit));
            initializeSchema();
            configureConnection();
        } catch (...) {
            sqlite3_close_v2(db);
            db = nullptr;
            throw;
        }
    }

    ~Impl() {
        if (db) {
            sqlite3_close_v2(db);
        }
    }

    void configureConnection() {
        std::string journal_mode_value;
        {
            Statement journal_mode(db, "PRAGMA journal_mode=WAL");
            const int code = sqlite3_step(journal_mode.get());
            if (code != SQLITE_ROW) {
                throw sqliteError(db, "Enabling SQLite WAL mode", code);
            }
            const auto* journal_text =
                reinterpret_cast<const char*>(sqlite3_column_text(journal_mode.get(), 0));
            if (journal_text) {
                journal_mode_value = journal_text;
            }
            const int done_code = sqlite3_step(journal_mode.get());
            if (done_code != SQLITE_DONE) {
                throw sqliteError(db, "Finishing SQLite WAL configuration", done_code);
            }
        }
        if (journal_mode_value != "wal") {
            throw std::runtime_error("SQLite did not enable WAL journal mode");
        }
        executeSql(db, "PRAGMA synchronous=FULL");
        if (readIntegerPragma(db, "PRAGMA synchronous") != 2) {
            throw std::runtime_error("SQLite did not enable FULL synchronous commits");
        }
        executeSql(db, "PRAGMA foreign_keys=ON");
        executeSql(db, "PRAGMA mmap_size=0");

        const std::uint64_t cache_kib = std::min<std::uint64_t>(
            std::max<std::uint64_t>(1, options.memory_limit_bytes / 1024),
            std::numeric_limits<int>::max());
        const std::string cache_sql =
            "PRAGMA cache_size=-" + std::to_string(cache_kib);
        executeSql(db, cache_sql);

        if (options.max_database_size_bytes > 0) {
            const int page_size = readIntegerPragma(db, "PRAGMA page_size");
            const std::uint64_t pages =
                options.max_database_size_bytes / static_cast<std::uint64_t>(page_size);
            if (pages == 0) {
                throw std::invalid_argument(
                    "The database size quota must allow at least one SQLite page");
            }
            const std::uint64_t bounded_pages =
                std::min<std::uint64_t>(pages, std::numeric_limits<int>::max());
            executeSql(db, "PRAGMA max_page_count=" + std::to_string(bounded_pages));
            if (static_cast<std::uint64_t>(
                    readIntegerPragma(db, "PRAGMA max_page_count")) > bounded_pages) {
                throw std::runtime_error(
                    "Existing SQLite database exceeds the configured page quota");
            }
        }
    }

    void initializeSchema() {
        const int version = readIntegerPragma(db, "PRAGMA user_version");
        if (version != 0 && version != 1 && version != 2) {
            throw std::runtime_error("Unsupported BlinkDB schema version: " +
                                     std::to_string(version));
        }
        const int application_id = readIntegerPragma(db, "PRAGMA application_id");
        if (application_id != 0 && application_id != BLINKDB_APPLICATION_ID) {
            throw std::runtime_error("The SQLite file belongs to a different application");
        }
        if (version != 0 && application_id != BLINKDB_APPLICATION_ID) {
            throw std::runtime_error("BlinkDB database is missing its application identifier");
        }
        if (version == 0) {
            int user_table_count = 0;
            {
                Statement existing_tables(
                    db,
                    "SELECT COUNT(*) FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%'");
                const int code = sqlite3_step(existing_tables.get());
                if (code != SQLITE_ROW) {
                    throw sqliteError(db, "Checking SQLite database identity", code);
                }
                user_table_count = sqlite3_column_int(existing_tables.get(), 0);
                const int done_code = sqlite3_step(existing_tables.get());
                if (done_code != SQLITE_DONE) {
                    throw sqliteError(db, "Finishing SQLite identity check", done_code);
                }
            }
            if (user_table_count != 0) {
                throw std::runtime_error(
                    "Refusing to initialize BlinkDB schema in a non-empty SQLite database");
            }

            executeSql(db, "BEGIN IMMEDIATE");
            try {
                executeSql(db, "PRAGMA application_id=" +
                                   std::to_string(BLINKDB_APPLICATION_ID));
                executeSql(db,
                           "CREATE TABLE kv_entries ("
                           "dataset TEXT NOT NULL,"
                           "key BLOB NOT NULL,"
                           "value BLOB NOT NULL,"
                           "PRIMARY KEY (dataset, key)"
                           ") WITHOUT ROWID");
                executeSql(db, "PRAGMA user_version=2");
                executeSql(db, "COMMIT");
            } catch (...) {
                const std::exception_ptr initialization_error = std::current_exception();
                try {
                    executeSql(db, "ROLLBACK");
                } catch (...) {
                    std::throw_with_nested(
                        std::runtime_error("Schema initialization and rollback both failed"));
                }
                std::rethrow_exception(initialization_error);
            }
        } else {
            Statement expected_schema(
                db,
                "SELECT dataset, key, value FROM kv_entries LIMIT 0");
            const int code = sqlite3_step(expected_schema.get());
            if (code != SQLITE_DONE) {
                throw sqliteError(db, "Validating BlinkDB schema", code);
            }
            if (version == 1) {
                executeSql(db, "BEGIN IMMEDIATE");
                try {
                    executeSql(
                        db,
                        "UPDATE kv_entries SET value=CAST("
                        "CAST('BLK2R:' || length(value) || ':' AS BLOB) || value AS BLOB)");
                    executeSql(db, "PRAGMA user_version=2");
                    executeSql(db, "COMMIT");
                } catch (...) {
                    const std::exception_ptr migration_error = std::current_exception();
                    try {
                        executeSql(db, "ROLLBACK");
                    } catch (...) {
                        std::throw_with_nested(
                            std::runtime_error("Schema migration and rollback both failed"));
                    }
                    std::rethrow_exception(migration_error);
                }
            }
        }
    }

    void validateDataset(std::string_view dataset) const {
        if (dataset.empty()) {
            throw std::invalid_argument("Dataset name must not be empty");
        }
        if (dataset.size() > options.max_dataset_name_bytes) {
            throw std::length_error("Dataset name exceeds the configured byte limit");
        }
    }

    void validateKey(std::string_view key) const {
        if (key.size() > options.max_key_bytes) {
            throw std::length_error("Key exceeds the configured byte limit");
        }
    }

    void validateValue(std::string_view value) const {
        if (value.size() > options.max_value_bytes) {
            throw std::length_error("Value exceeds the configured byte limit");
        }
    }

    void setLocked(std::string_view key,
                   std::string_view value,
                   std::string_view dataset) {
        validateDataset(dataset);
        validateKey(key);
        validateValue(value);
        Statement statement(
            db,
            "INSERT INTO kv_entries(dataset, key, value) VALUES(?1, ?2, ?3) "
            "ON CONFLICT(dataset, key) DO UPDATE SET value=excluded.value");
        bindText(db, statement.get(), 1, dataset);
        bindBlob(db, statement.get(), 2, key);
        const std::string stored_value = encodeValue(value);
        bindBlob(db, statement.get(), 3, stored_value);
        const int code = sqlite3_step(statement.get());
        if (code != SQLITE_DONE) {
            throw sqliteError(db, "Writing key/value", code);
        }
    }

    StorageEngineOptions options;
    std::filesystem::path path;
    sqlite3* db = nullptr;
    mutable std::mutex mutex;
};

StorageEngine::StorageEngine(StorageEngineOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

StorageEngine::~StorageEngine() = default;
StorageEngine::StorageEngine(StorageEngine&&) noexcept = default;
StorageEngine& StorageEngine::operator=(StorageEngine&&) noexcept = default;

void StorageEngine::set(std::string_view key,
                        std::string_view value,
                        std::string_view dataset) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->setLocked(key, value, dataset);
}

std::optional<std::string> StorageEngine::get(std::string_view key,
                                               std::string_view dataset) const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->validateDataset(dataset);
    impl_->validateKey(key);
    Statement statement(
        impl_->db,
        "SELECT value FROM kv_entries WHERE dataset=?1 AND key=?2");
    bindText(impl_->db, statement.get(), 1, dataset);
    bindBlob(impl_->db, statement.get(), 2, key);

    const int code = sqlite3_step(statement.get());
    if (code == SQLITE_DONE) {
        return std::nullopt;
    }
    if (code != SQLITE_ROW) {
        throw sqliteError(impl_->db, "Reading key/value", code);
    }

    const auto* data = static_cast<const char*>(sqlite3_column_blob(statement.get(), 0));
    const int size = sqlite3_column_bytes(statement.get(), 0);
    const std::string_view stored(data ? data : "", static_cast<std::size_t>(size));
    return decodeValue(stored, impl_->options.max_value_bytes);
}

bool StorageEngine::del(std::string_view key, std::string_view dataset) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->validateDataset(dataset);
    impl_->validateKey(key);
    Statement statement(
        impl_->db,
        "DELETE FROM kv_entries WHERE dataset=?1 AND key=?2");
    bindText(impl_->db, statement.get(), 1, dataset);
    bindBlob(impl_->db, statement.get(), 2, key);
    const int code = sqlite3_step(statement.get());
    if (code != SQLITE_DONE) {
        throw sqliteError(impl_->db, "Deleting key/value", code);
    }
    return sqlite3_changes(impl_->db) > 0;
}

void StorageEngine::transact(const std::vector<StorageMutation>& mutations) {
    for (const auto& mutation : mutations) {
        impl_->validateDataset(mutation.dataset);
        impl_->validateKey(mutation.key);
        if (mutation.kind == StorageMutation::Kind::Set) {
            impl_->validateValue(mutation.value);
        }
        if (mutation.kind != StorageMutation::Kind::Set &&
            mutation.kind != StorageMutation::Kind::Delete) {
            throw std::invalid_argument("Transaction contains an unknown mutation kind");
        }
    }
    if (mutations.empty()) {
        return;
    }

    std::lock_guard<std::mutex> lock(impl_->mutex);
    executeSql(impl_->db, "BEGIN IMMEDIATE");
    try {
        for (const auto& mutation : mutations) {
            if (mutation.kind == StorageMutation::Kind::Set) {
                impl_->setLocked(mutation.key, mutation.value, mutation.dataset);
            } else {
                Statement statement(
                    impl_->db,
                    "DELETE FROM kv_entries WHERE dataset=?1 AND key=?2");
                bindMutation(impl_->db, statement.get(), mutation);
                const int code = sqlite3_step(statement.get());
                if (code != SQLITE_DONE) {
                    throw sqliteError(impl_->db, "Deleting key/value in transaction", code);
                }
            }
        }
        executeSql(impl_->db, "COMMIT");
    } catch (...) {
        const std::exception_ptr transaction_error = std::current_exception();
        try {
            executeSql(impl_->db, "ROLLBACK");
        } catch (...) {
            std::throw_with_nested(
                std::runtime_error("Transaction failed and SQLite rollback also failed"));
        }
        std::rethrow_exception(transaction_error);
    }
}

void StorageEngine::backupTo(const std::filesystem::path& backup_file) const {
    if (backup_file.empty()) {
        throw std::invalid_argument("Backup path must not be empty");
    }
    const std::filesystem::path destination = std::filesystem::absolute(backup_file);
    std::filesystem::create_directories(destination.parent_path());
    if (std::filesystem::exists(destination)) {
        throw std::runtime_error("Refusing to overwrite existing backup: " +
                                 sqlitePath(destination));
    }

    std::lock_guard<std::mutex> lock(impl_->mutex);
    std::random_device random;
    const auto nonce = (static_cast<std::uint64_t>(random()) << 32) ^ random() ^
                       static_cast<std::uint64_t>(
                           std::chrono::steady_clock::now().time_since_epoch().count());
    const auto temporary =
        destination.parent_path() /
        (destination.filename().u8string() + ".tmp-" + std::to_string(nonce));

    sqlite3* backup_db = nullptr;
    const std::string temporary_path = sqlitePath(temporary);
    const int open_code = sqlite3_open_v2(
        temporary_path.c_str(), &backup_db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_EXCLUSIVE |
            SQLITE_OPEN_FULLMUTEX,
        nullptr);
    if (open_code != SQLITE_OK) {
        const auto error = sqliteError(backup_db, "Creating backup file", open_code);
        if (backup_db) {
            sqlite3_close_v2(backup_db);
        }
        throw error;
    }

    try {
        runBackup(backup_db, impl_->db);
        checkSqlite(backup_db, sqlite3_close(backup_db), "Closing backup database");
        backup_db = nullptr;
#if defined(_WIN32)
        if (!MoveFileExW(temporary.c_str(), destination.c_str(), MOVEFILE_WRITE_THROUGH)) {
            throw std::system_error(static_cast<int>(GetLastError()),
                                    std::system_category(),
                                    "Publishing SQLite backup");
        }
#else
        std::filesystem::create_hard_link(temporary, destination);
        std::error_code cleanup_error;
        const bool removed = std::filesystem::remove(temporary, cleanup_error);
        if (!removed || cleanup_error) {
            throw std::runtime_error(
                "Backup was published at " + sqlitePath(destination) +
                " but its staging file could not be removed: " + cleanup_error.message());
        }
#endif
    } catch (...) {
        if (backup_db) {
            sqlite3_close_v2(backup_db);
        }
        std::error_code ignored;
        std::filesystem::remove(temporary, ignored);
        throw;
    }
}

void StorageEngine::restoreFrom(const std::filesystem::path& backup_file) {
    if (backup_file.empty() || !std::filesystem::is_regular_file(backup_file)) {
        throw std::invalid_argument("Restore source must be an existing regular file");
    }
    const std::filesystem::path source_path = std::filesystem::absolute(backup_file);
    if (std::filesystem::equivalent(source_path, impl_->path)) {
        throw std::invalid_argument("Restore source must not be the active database file");
    }

    sqlite3* source_db = nullptr;
    const std::string utf8_source = sqlitePath(source_path);
    const int open_code =
        sqlite3_open_v2(utf8_source.c_str(), &source_db,
                        SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nullptr);
    if (open_code != SQLITE_OK) {
        const auto error = sqliteError(source_db, "Opening restore source", open_code);
        if (source_db) {
            sqlite3_close_v2(source_db);
        }
        throw error;
    }

    try {
        const int source_application_id =
            readIntegerPragma(source_db, "PRAGMA application_id");
        const int source_version = readIntegerPragma(source_db, "PRAGMA user_version");
        if (source_application_id != BLINKDB_APPLICATION_ID || source_version != 2) {
            throw std::runtime_error("Restore source is not a supported BlinkDB backup");
        }
        {
            Statement integrity(source_db, "PRAGMA integrity_check");
            const int code = sqlite3_step(integrity.get());
            const auto* result =
                code == SQLITE_ROW
                    ? reinterpret_cast<const char*>(sqlite3_column_text(integrity.get(), 0))
                    : nullptr;
            if (!result || std::string_view(result) != "ok") {
                throw std::runtime_error("Restore source failed SQLite integrity validation");
            }
        }
        std::lock_guard<std::mutex> lock(impl_->mutex);
        runBackup(impl_->db, source_db);
        impl_->configureConnection();
        impl_->initializeSchema();
    } catch (...) {
        sqlite3_close_v2(source_db);
        throw;
    }
    checkSqlite(nullptr, sqlite3_close(source_db), "Closing restore source");
}

const std::filesystem::path& StorageEngine::databasePath() const noexcept {
    return impl_->path;
}

void StorageEngine::printMemoryStatus() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    const int pages = readIntegerPragma(impl_->db, "PRAGMA page_count");
    const int page_size = readIntegerPragma(impl_->db, "PRAGMA page_size");
    std::cout << "SQLite page cache limit: " << impl_->options.memory_limit_bytes
              << " bytes; database pages: " << pages << "; page size: " << page_size
              << " bytes\n";
}
