#ifndef BLINKDB_H
#define BLINKDB_H

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

struct StorageEngineOptions {
    std::filesystem::path data_directory = "blinkdb-data";
    std::string database_filename = "blinkdb.sqlite3";
    int busy_timeout_ms = 5000;
    std::uint64_t memory_limit_bytes = 64 * 1024 * 1024;
    std::uint64_t max_database_size_bytes = 0;
    std::uint64_t max_dataset_name_bytes = 255;
    std::uint64_t max_key_bytes = 1024 * 1024;
    std::uint64_t max_value_bytes = 64 * 1024 * 1024;
};

struct StorageMutation {
    enum class Kind {
        Set,
        Delete
    };

    Kind kind;
    std::string dataset;
    std::string key;
    std::string value;
};

class StorageEngine {
public:
    explicit StorageEngine(StorageEngineOptions options = {});
    ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) noexcept;
    StorageEngine& operator=(StorageEngine&&) noexcept;

    void set(std::string_view key,
             std::string_view value,
             std::string_view dataset = "default");
    std::optional<std::string> get(
        std::string_view key,
        std::string_view dataset = "default") const;
    bool del(std::string_view key, std::string_view dataset = "default");

    void transact(const std::vector<StorageMutation>& mutations);
    void backupTo(const std::filesystem::path& backup_file) const;
    void restoreFrom(const std::filesystem::path& backup_file);

    const std::filesystem::path& databasePath() const noexcept;
    void printMemoryStatus() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

#endif
