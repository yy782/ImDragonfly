#include <iostream>
#include <chrono>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <iomanip>
#include <functional>
// ./db_table_perf
#include "src/sharding/db_table.hpp"
#include "redis/dict.hpp"

using namespace std::chrono;

namespace dfly {

struct BasicDashPolicy {
  enum : uint8_t { kSlotNum = 14, kBucketNum = 56 };
  static constexpr bool kUseVersion = false;

  template <typename U> static void DestroyValue(const U&) {}
  template <typename U> static void DestroyKey(const U&) {}

  template <typename U, typename V> static bool Equal(U&& u, V&& v) {
    return u == v;
  }

  static uint64_t HashFn(uint64_t v) {
    return std::hash<uint64_t>{}(v);
  }
};

using Dash64 = DashTable<uint64_t, uint64_t, BasicDashPolicy>;

static uint64_t dictUint64Hash(const void *key) {
    return std::hash<uint64_t>{}(*(const uint64_t*)key);
}

static int dictUint64Compare(dict *d, const void *key1, const void *key2) {
    (void)d;
    return *(const uint64_t*)key1 == *(const uint64_t*)key2;
}

static dictType dictTypeUint64 = {
    dictUint64Hash,
    NULL,
    NULL,
    dictUint64Compare,
    NULL,
    NULL
};

std::vector<uint64_t> GenerateRandomKeys(size_t count) {
  std::vector<uint64_t> keys;
  keys.reserve(count);
  
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;
  
  for (size_t i = 0; i < count; ++i) {
    keys.push_back(dis(gen));
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
  
  Dash64 dt(1);
  dict *redis_dict = dictCreate(&dictTypeUint64);
  std::unordered_map<uint64_t, uint64_t> std_map;
  
  std::cout << "\n--- Insert Performance ---" << std::endl;
  
  double dt_insert = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      dt.InsertNew(keys[i], i);
    }
  }, "DashTable Insert", num_elements);
  
  double redis_insert = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      uint64_t k = keys[i];
      uint64_t v = i;
      dictAdd(redis_dict, &k, &v);
    }
  }, "Redis dict Insert", num_elements);
  
  double std_insert = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      std_map[keys[i]] = i;
    }
  }, "std::unordered_map Insert", num_elements);
  
  std::cout << "\n--- Find Performance ---" << std::endl;
  
  double dt_find = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      auto it = dt.Find(keys[i]);
      (void)it;
    }
  }, "DashTable Find", num_elements);
  
  double redis_find = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      uint64_t k = keys[i];
      dictEntry *e = dictFind(redis_dict, &k);
      (void)e;
    }
  }, "Redis dict Find", num_elements);
  
  double std_find = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      auto it = std_map.find(keys[i]);
      (void)it;
    }
  }, "std::unordered_map Find", num_elements);
  
  std::cout << "\n--- Erase Performance ---" << std::endl;
  
  double dt_erase = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      auto it = dt.Find(keys[i]);
      if (!it.is_done()) {
        dt.Erase(it);
      }
    }
  }, "DashTable Erase", num_elements);
  
  double redis_erase = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      uint64_t k = keys[i];
      dictDelete(redis_dict, &k);
    }
  }, "Redis dict Erase", num_elements);
  
  double std_erase = MeasureTime([&]() {
    for (size_t i = 0; i < num_elements; ++i) {
      std_map.erase(keys[i]);
    }
  }, "std::unordered_map Erase", num_elements);
  
  std::cout << "\n--- Comparison Summary ---" << std::endl;
  std::cout << std::setw(25) << "Operation" 
            << std::setw(18) << "DashTable" 
            << std::setw(18) << "Redis dict" 
            << std::setw(22) << "std::unordered_map" << std::endl;
  std::cout << "------------------------------------------------------------------------" << std::endl;
  std::cout << std::setw(25) << "Insert" 
            << std::setw(18) << std::fixed << std::setprecision(2) << dt_insert << " ms"
            << std::setw(18) << std::fixed << std::setprecision(2) << redis_insert << " ms"
            << std::setw(22) << std::fixed << std::setprecision(2) << std_insert << " ms" << std::endl;
  std::cout << std::setw(25) << "Find" 
            << std::setw(18) << std::fixed << std::setprecision(2) << dt_find << " ms"
            << std::setw(18) << std::fixed << std::setprecision(2) << redis_find << " ms"
            << std::setw(22) << std::fixed << std::setprecision(2) << std_find << " ms" << std::endl;
  std::cout << std::setw(25) << "Erase" 
            << std::setw(18) << std::fixed << std::setprecision(2) << dt_erase << " ms"
            << std::setw(18) << std::fixed << std::setprecision(2) << redis_erase << " ms"
            << std::setw(22) << std::fixed << std::setprecision(2) << std_erase << " ms" << std::endl;
  
  dictRelease(redis_dict);
}

}  // namespace dfly

int main() {
  std::cout << "=== DbTable Performance Benchmark ===" << std::endl;
  std::cout << "Comparing DashTable vs Redis dict vs std::unordered_map" << std::endl;
  
  dfly::RunPerformanceTest(10000);
  dfly::RunPerformanceTest(100000);
  dfly::RunPerformanceTest(1000000);
  
  return 0;
}