/**
 * @file main.cpp
 * @brief Multi-threaded test harness for the BlinkDB storage engine.
 *
 * This program spawns multiple threads that perform concurrent SET and GET
 * operations on a shared StorageEngine instance.
 *
 * Compile with:
 *   g++ -std=c++17 -O3 main.cpp blinkdb.cpp -o blinkdb -pthread
 */

 #include "blinkdb.h"
 #include <iostream>
 #include <thread>
 #include <vector>
 #include <string>
 #include <mutex>  
std::mutex db_mutex;
 /**
  * @brief Worker function for each thread.
  *
  * This function performs a series of SET and GET operations on the shared StorageEngine instance.
  *
  * @param db Reference to the StorageEngine instance.
  * @param thread_id ID of the thread executing the function.
  * @param num_operations Number of operations each thread will perform.
  */
  void worker(StorageEngine &db, int thread_id, int num_operations) {
    for (int i = 0; i < num_operations; ++i) {
        std::string key = "key_" + std::to_string(thread_id) + "_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);
        
        {
            std::lock_guard<std::mutex> lock(db_mutex); // Lock before modifying the database
            db.set(key, value);
        }
        
        std::string retrieved;
        {
            std::lock_guard<std::mutex> lock(db_mutex); // Lock before reading from the database
            retrieved = db.get(key);
        }
        
        if (!retrieved || *retrieved != value) {
            std::cerr << "Mismatch in thread " << thread_id << " for key " << key << std::endl;
        }
    }
}
 
 /**
  * @brief Main function to test BlinkDB with multi-threading.
  *
  * Creates a shared StorageEngine instance and spawns multiple threads
  * to perform concurrent SET and GET operations.
  *
  * @return int Program exit status.
  */
 int main() {
     // Create a shared StorageEngine instance
     StorageEngine db;
     
     /// Number of threads to spawn (adjust based on your machine)
     const int num_threads = 8;
     /// Number of operations each thread will perform
     const int num_operations = 10000;
     
     std::vector<std::thread> threads;
     
     // Spawn threads
     for (int i = 0; i < num_threads; ++i) {
         threads.emplace_back(worker, std::ref(db), i, num_operations);
     }
     
     // Wait for all threads to finish
     for (auto &t : threads) {
         t.join();
     }
     
     // Print final memory status for verification
     db.printMemoryStatus();
     
     std::cout << "Multi-threaded test completed." << std::endl;
     return 0;
 }
