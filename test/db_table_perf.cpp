#include <iostream>
#include <chrono>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <iomanip>
// ./db_table_perf
#include "src/sharding/db_table.hpp"

using namespace std::chrono;

namespace dfly {

std::vector<std::string> GenerateRandomKeys(size_t count, size_t key_length = 16) {
  std::vector<std::string> keys;
  keys.reserve(count);
  
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis('a', 'z');
  
  for (size_t i = 0; i < count; ++i) {
    std::string key(key_length, ' ');
    for (size_t j = 0; j < key_length; ++j) {
      key[j] = static_cast<char>(dis(gen));
    }
    keys.push_back(key);
  }
  return keys;
}

template<typename Func>
double MeasureTime(Func&& func, const std::string& operation, size_t iterations) {
  auto start = high_resolution_clock::now();
  func();
  auto end = high_resolution_clock::now();
  
  double duration_ms = duration<double, std::milli>(end - start).count();
  double ops_per_sec = (iterations * 1000.0) / duration_ms;
  
  std::cout << std::setw(25) << operation 
            << ": " << std::fixed << std::setprecision(2) 
            << duration_ms << " ms (" 
            << std::setprecision(0) << ops_per_sec << " ops/sec)" << std::endl;
  
  return duration_ms;
}

void RunPerformanceTest(size_t num_elements) {
  std::cout << "\n=== Performance Test with " << num_elements << " elements ===" << std::endl;
  
  auto keys = GenerateRandomKeys(num_elements);
  
  DbTable db_table(PMR_NS::get_default_resource(), 0);
  std::unordered_map<std::string, std::string> std_map;
  
  std::cout << "\n--- Insert Performance ---" << std::endl;
  
  double dt_insert = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      db_table.prime().InsertNew(keys[i], PrimeValue{});
    }
  }, "DashTable Insert", num_elements);
  
  double std_insert = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      std_map[keys[i]] = "value";
    }
  }, "std::unordered_map Insert", num_elements);
  
  std::cout << "\n--- Find Performance ---" << std::endl;
  
  double dt_find = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      auto it = db_table.prime().Find(keys[i]);
      (void)it;
    }
  }, "DashTable Find", num_elements);
  
  double std_find = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      auto it = std_map.find(keys[i]);
      (void)it;
    }
  }, "std::unordered_map Find", num_elements);
  
  std::cout << "\n--- Mixed Read/Write Performance ---" << std::endl;
  
  double dt_mixed = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      if (i % 3 == 0) {
        db_table.prime().InsertNew("new_key" + std::to_string(i), PrimeValue{});
      } else {
        auto it = db_table.prime().Find(keys[i % num_elements]);
        (void)it;
      }
    }
  }, "DashTable Mixed", num_elements);
  
  double std_mixed = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      if (i % 3 == 0) {
        std_map["new_key" + std::to_string(i)] = "value";
      } else {
        auto it = std_map.find(keys[i % num_elements]);
        (void)it;
      }
    }
  }, "std::unordered_map Mixed", num_elements);
  
  std::cout << "\n--- Erase Performance ---" << std::endl;
  
  double dt_erase = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      auto it = db_table.prime().Find(keys[i]);
      if (!it.is_done()) {
        db_table.prime().Erase(it);
      }
    }
  }, "DashTable Erase", num_elements);
  
  double std_erase = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      std_map.erase(keys[i]);
    }
  }, "std::unordered_map Erase", num_elements);
  
  std::cout << "\n--- Comparison Summary ---" << std::endl;
  std::cout << std::setw(25) << "Operation" 
            << std::setw(20) << "DashTable" 
            << std::setw(20) << "std::unordered_map" 
            << std::setw(15) << "Ratio (DT/Std)" << std::endl;
  std::cout << "------------------------------------------------------------------------" << std::endl;
  std::cout << std::setw(25) << "Insert" 
            << std::setw(20) << std::fixed << std::setprecision(2) << dt_insert << " ms"
            << std::setw(20) << std::fixed << std::setprecision(2) << std_insert << " ms"
            << std::setw(15) << std::fixed << std::setprecision(2) << (dt_insert / std_insert) << std::endl;
  std::cout << std::setw(25) << "Find" 
            << std::setw(20) << std::fixed << std::setprecision(2) << dt_find << " ms"
            << std::setw(20) << std::fixed << std::setprecision(2) << std_find << " ms"
            << std::setw(15) << std::fixed << std::setprecision(2) << (dt_find / std_find) << std::endl;
  std::cout << std::setw(25) << "Mixed" 
            << std::setw(20) << std::fixed << std::setprecision(2) << dt_mixed << " ms"
            << std::setw(20) << std::fixed << std::setprecision(2) << std_mixed << " ms"
            << std::setw(15) << std::fixed << std::setprecision(2) << (dt_mixed / std_mixed) << std::endl;
  std::cout << std::setw(25) << "Erase" 
            << std::setw(20) << std::fixed << std::setprecision(2) << dt_erase << " ms"
            << std::setw(20) << std::fixed << std::setprecision(2) << std_erase << " ms"
            << std::setw(15) << std::fixed << std::setprecision(2) << (dt_erase / std_erase) << std::endl;
}

}  // namespace dfly

int main() {
  std::cout << "=== DbTable Performance Benchmark ===" << std::endl;
  std::cout << "Comparing DashTable vs std::unordered_map" << std::endl;
  
  dfly::RunPerformanceTest(10000);
  dfly::RunPerformanceTest(100000);
  dfly::RunPerformanceTest(1000000);
  
  return 0;
}
