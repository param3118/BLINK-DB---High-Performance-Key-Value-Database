#include "blinkdb.h"

#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
                ("blinkdb-tests-" + std::to_string(stamp));
        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

StorageEngineOptions optionsFor(const std::filesystem::path& data_directory) {
    StorageEngineOptions options;
    options.data_directory = data_directory;
    return options;
}

void testVersionOneMigration(const std::filesystem::path& root) {
    const auto directory = root / "legacy-database";
    std::filesystem::create_directories(directory);
    const auto file = directory / "blinkdb.sqlite3";
    sqlite3* db = nullptr;
    require(sqlite3_open(file.u8string().c_str(), &db) == SQLITE_OK,
            "Could not create legacy database fixture");
    char* error = nullptr;
    const int schema_code = sqlite3_exec(
        db,
        "PRAGMA application_id=1112294466;"
        "PRAGMA user_version=1;"
        "CREATE TABLE kv_entries(dataset TEXT NOT NULL,key BLOB NOT NULL,"
        "value BLOB NOT NULL,PRIMARY KEY(dataset,key)) WITHOUT ROWID;",
        nullptr, nullptr, &error);
    const std::string schema_error = error ? error : sqlite3_errmsg(db);
    sqlite3_free(error);
    require(schema_code == SQLITE_OK, "Could not create legacy schema: " + schema_error);

    std::string legacy_value("\0", 1);
    legacy_value.push_back(static_cast<char>(0xFF));
    legacy_value += "BLK1raw";
    sqlite3_stmt* insert = nullptr;
    require(sqlite3_prepare_v2(
                db, "INSERT INTO kv_entries(dataset,key,value) VALUES(?1,?2,?3)",
                -1, &insert, nullptr) == SQLITE_OK,
            "Could not prepare legacy data fixture");
    sqlite3_bind_text(insert, 1, "legacy", -1, SQLITE_STATIC);
    sqlite3_bind_blob(insert, 2, "key", 3, SQLITE_STATIC);
    sqlite3_bind_blob(insert, 3, legacy_value.data(),
                      static_cast<int>(legacy_value.size()), SQLITE_TRANSIENT);
    const int insert_code = sqlite3_step(insert);
    sqlite3_finalize(insert);
    const int close_code = sqlite3_close(db);
    require(insert_code == SQLITE_DONE && close_code == SQLITE_OK,
            "Could not populate legacy database fixture");

    StorageEngine engine(optionsFor(directory));
    require(engine.get("key", "legacy") == legacy_value,
            "Schema migration must preserve old binary values and marker prefixes");
}

void testBinaryEmptyAndDatasetIsolation(const std::filesystem::path& root) {
    StorageEngine engine(optionsFor(root / "database"));
    const std::string binary_value("A\0B\xFF", 4);

    engine.set("empty", "");
    engine.set("binary", binary_value, "project-a");
    engine.set("binary", "different", "project-b");

    const auto empty = engine.get("empty");
    require(empty.has_value(), "An empty value must not be treated as a missing key");
    require(empty->empty(), "An empty value must round-trip");
    require(engine.get("missing") == std::nullopt, "Missing keys must be distinguishable");
    require(engine.get("binary", "project-a") == binary_value,
            "Binary values must round-trip without text conversion");
    require(engine.get("binary", "project-b") == std::string("different"),
            "Datasets must have independent key spaces");
    const std::string compressible_value(12000, 'z');
    engine.set("compressed", compressible_value, "project-a");
    require(engine.get("compressed", "project-a") == compressible_value,
            "Compressed values must round-trip");
    require(engine.del("binary", "project-a"), "DEL must report a removed key");
    require(!engine.del("binary", "project-a"), "DEL must report a missing key");
    require(engine.get("binary", "project-b") == std::string("different"),
            "Deleting a key in one dataset must not affect another");
}

void testConfiguredValueLimit(const std::filesystem::path& root) {
    StorageEngineOptions options = optionsFor(root / "limits");
    options.max_value_bytes = 4;
    StorageEngine engine(options);
    engine.set("accepted", "1234");

    bool rejected = false;
    try {
        engine.set("too-large", "12345");
    } catch (const std::length_error&) {
        rejected = true;
    }
    require(rejected, "Values above the configured limit must be rejected");
    require(engine.get("accepted") == std::string("1234"),
            "A rejected value must not damage existing data");
}

void testTransactionAndBackupRestore(const std::filesystem::path& root) {
    const auto database_directory = root / "source";
    const auto backup_file = root / "backups" / "snapshot.sqlite3";
    {
        StorageEngine engine(optionsFor(database_directory));
        engine.set("existing", "before");
        engine.transact({
            {StorageMutation::Kind::Set, "project-a", "one", "1"},
            {StorageMutation::Kind::Set, "project-b", "two", "2"},
            {StorageMutation::Kind::Delete, "default", "existing", ""}
        });
        require(engine.get("existing") == std::nullopt,
                "A transaction must apply deletes");
        require(engine.get("one", "project-a") == std::string("1"),
                "A transaction must apply writes");

        sqlite3* raw_db = nullptr;
        const std::string db_path = engine.databasePath().u8string();
        require(sqlite3_open(db_path.c_str(), &raw_db) == SQLITE_OK,
                "Test could not open SQLite database to set up rollback trigger");
        char* trigger_error = nullptr;
        const int trigger_code = sqlite3_exec(
            raw_db,
            "CREATE TRIGGER test_reject_value BEFORE INSERT ON kv_entries "
            "WHEN NEW.key=X'72656A656374' "
            "BEGIN SELECT RAISE(ABORT, 'forced transaction failure'); END",
            nullptr, nullptr, &trigger_error);
        const std::string trigger_message =
            trigger_error ? trigger_error : sqlite3_errmsg(raw_db);
        sqlite3_free(trigger_error);
        const int close_code = sqlite3_close(raw_db);
        require(trigger_code == SQLITE_OK && close_code == SQLITE_OK,
                "Test could not install the rollback trigger: " + trigger_message);

        bool transaction_failed = false;
        try {
            engine.transact({
                {StorageMutation::Kind::Set, "default", "must-rollback", "value"},
                {StorageMutation::Kind::Set, "default", "reject", "failure"}
            });
        } catch (const std::runtime_error&) {
            transaction_failed = true;
        }
        require(transaction_failed, "A failed transaction must return an error");
        require(engine.get("must-rollback") == std::nullopt,
                "A failed transaction must roll back earlier mutations");

        engine.backupTo(backup_file);
    }

    StorageEngine restored(optionsFor(root / "restored"));
    restored.restoreFrom(backup_file);
    require(restored.get("one", "project-a") == std::string("1"),
            "Backup/restore must preserve values");
    require(restored.get("two", "project-b") == std::string("2"),
            "Backup/restore must preserve dataset separation");
    require(restored.get("existing") == std::nullopt,
            "Backup/restore must preserve deletions");
}

void testRestartAndConcurrentWrites(const std::filesystem::path& root) {
    const auto database_directory = root / "restart";
    {
        StorageEngine engine(optionsFor(database_directory));
        engine.set("persistent", "survives close");
    }
    {
        StorageEngine engine(optionsFor(database_directory));
        require(engine.get("persistent") == std::string("survives close"),
                "Committed values must survive a clean restart");

        std::vector<std::thread> writers;
        for (int thread_id = 0; thread_id < 6; ++thread_id) {
            writers.emplace_back([thread_id, &engine] {
                for (int index = 0; index < 100; ++index) {
                    const std::string key = std::to_string(thread_id) + ":" +
                                            std::to_string(index);
                    engine.set(key, "value");
                }
            });
        }
        for (auto& writer : writers) {
            writer.join();
        }

        for (int thread_id = 0; thread_id < 6; ++thread_id) {
            for (int index = 0; index < 100; ++index) {
                const std::string key = std::to_string(thread_id) + ":" +
                                        std::to_string(index);
                require(engine.get(key) == std::string("value"),
                        "Concurrent writes must remain readable");
            }
        }
    }
}

} // namespace

int main() {
    try {
        TemporaryDirectory temporary;
        testVersionOneMigration(temporary.path());
        testBinaryEmptyAndDatasetIsolation(temporary.path());
        testConfiguredValueLimit(temporary.path());
        testTransactionAndBackupRestore(temporary.path());
        testRestartAndConcurrentWrites(temporary.path());
        std::cout << "BlinkDB storage tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "BlinkDB storage tests failed: " << error.what() << '\n';
        return 1;
    }
}
