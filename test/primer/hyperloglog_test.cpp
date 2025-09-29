#include <limits>
#include <memory>
#include <string>
#include <thread>  //NOLINT
#include <vector>

#include <unordered_set>
#include <random>
#include <chrono>

#include "common/exception.h"
#include "gtest/gtest.h"
#include "primer/hyperloglog.h"
#include "primer/hyperloglog_presto.h"

namespace bustub {

TEST(HyperLogLogTest, BasicTest1) {
  auto obj = HyperLogLog<std::string>(static_cast<int16_t>(1));
  ASSERT_EQ(obj.GetCardinality(), 0);
  obj.AddElem("Welcome to CMU DB (15-445/645)");

  obj.ComputeCardinality();

  auto ans = obj.GetCardinality();
  ASSERT_EQ(ans, 2);

  for (uint64_t i = 0; i < 10; i++) {
    obj.AddElem("Andy");
    obj.AddElem("Connor");
    obj.AddElem("J-How");
    obj.AddElem("Kunle");
    obj.AddElem("Lan");
    obj.AddElem("Prashanth");
    obj.AddElem("William");
    obj.AddElem("Yash");
    obj.AddElem("Yuanxin");
    if (i == 0) {
      obj.ComputeCardinality();
      ans = obj.GetCardinality();
      ASSERT_EQ(ans, 6);
    }
  }

  obj.ComputeCardinality();
  ans = obj.GetCardinality();
  ASSERT_EQ(ans, 6);
}

TEST(HyperLogLogTest, BasicTest2) {
  auto obj = HyperLogLog<int64_t>(static_cast<int16_t>(3));

  ASSERT_EQ(obj.GetCardinality(), 0);

  obj.AddElem(0);

  obj.ComputeCardinality();
  auto ans = obj.GetCardinality();

  ASSERT_EQ(ans, 7);

  for (uint64_t i = 0; i < 10; i++) {
    obj.AddElem(10);
    obj.AddElem(122);
    obj.AddElem(200);
    obj.AddElem(911);
    obj.AddElem(999);
    obj.AddElem(1402);
    obj.AddElem(15445);
    obj.AddElem(15645);
    obj.AddElem(123456);
    obj.AddElem(312457);
    if (i == 0) {
      obj.ComputeCardinality();
      ans = obj.GetCardinality();
      ASSERT_EQ(ans, 10);
    }
  }

  for (uint64_t i = 0; i < 10; i++) {
    obj.AddElem(-1);
    obj.AddElem(-2);
    obj.AddElem(-3);
    obj.AddElem(-4);
    obj.AddElem(-5);
    obj.AddElem(-6);
    obj.AddElem(-7);
    obj.AddElem(-8);
    obj.AddElem(-9);
    obj.AddElem(-27);
    if (i == 0) {
      obj.ComputeCardinality();
      ans = obj.GetCardinality();
      ASSERT_EQ(ans, 10);
    }
  }
  obj.ComputeCardinality();
  ans = obj.GetCardinality();
  ASSERT_EQ(ans, 10);
}

TEST(HyperLogLogTest, EdgeTest1) {
  auto obj1 = HyperLogLog<int64_t>(static_cast<int16_t>(-2));
  obj1.ComputeCardinality();
  ASSERT_EQ(obj1.GetCardinality(), 0);
}

TEST(HyperLogLogTest, EdgeTest2) {
  auto obj = HyperLogLog<int64_t>(static_cast<int16_t>(0));
  obj.ComputeCardinality();
  ASSERT_EQ(obj.GetCardinality(), 0);

  obj.AddElem(1);
  obj.ComputeCardinality();
  ASSERT_EQ(obj.GetCardinality(), 1665180);

  obj.AddElem(-1);
  obj.ComputeCardinality();
  ASSERT_EQ(obj.GetCardinality(), 1665180);
}

TEST(HyperLogLogTest, BasicParallelTest) {
  auto obj = HyperLogLog<std::string>(static_cast<int16_t>(1));

  std::vector<std::thread> threads1;
  for (uint16_t i = 0; i < 10; i++) {
    threads1.emplace_back(std::thread([&]() { obj.AddElem("Welcome to CMU DB (15-445/645)"); }));
  }

  // Wait for all threads to finish execution
  for (auto &thread : threads1) {
    thread.join();
  }
  obj.ComputeCardinality();
  auto ans = obj.GetCardinality();
  ASSERT_EQ(ans, 2);

  std::vector<std::thread> threads2;
  for (uint16_t k = 0; k < 10; k++) {
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Andy"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Connor"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("J-How"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Kunle"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Lan"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Prashanth"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("William"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Yash"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Yuanxin"); }));
  }

  for (auto &thread : threads2) {
    thread.join();
  }

  obj.ComputeCardinality();
  ans = obj.GetCardinality();
  ASSERT_EQ(ans, 6);
}

TEST(HyperLogLogTest, ParallelTest1) {
  auto obj = HyperLogLog<std::string>(static_cast<int16_t>(14));

  std::vector<std::thread> threads1;
  for (uint16_t i = 0; i < 10; i++) {
    threads1.emplace_back(std::thread([&]() { obj.AddElem("Welcome to CMU DB (15-445/645)"); }));
  }

  // Wait for all threads to finish execution
  for (auto &thread : threads1) {
    thread.join();
  }
  obj.ComputeCardinality();
  auto ans = obj.GetCardinality();
  ASSERT_EQ(ans, 13009);

  std::vector<std::thread> threads2;
  for (uint64_t k = 0; k < 3000; k++) {
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Andy"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Connor"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("J-How"); }));
    threads2.emplace_back(std::thread([&]() { obj.AddElem("Kunle"); }));
  }

  for (auto &thread : threads2) {
    thread.join();
  }

  obj.ComputeCardinality();
  ans = obj.GetCardinality();
  ASSERT_EQ(ans, 13010);
}

TEST(HyperLogLogTest, PrestoBasicTest1) {
  auto obj = HyperLogLogPresto<std::string>(static_cast<int16_t>(2));
  ASSERT_EQ(obj.GetCardinality(), 0);

  std::string str1 = "Welcome to CMU DB (15-445/645)";

  obj.AddElem(str1);

  obj.ComputeCardinality();
  auto ans = obj.GetCardinality();
  ASSERT_EQ(ans, 3);
  for (uint64_t i = 0; i < 10; i++) {
    obj.AddElem("Andy");
    obj.AddElem("Connor");
    obj.AddElem("J-How");
    obj.AddElem("Kunle");
    obj.AddElem("Lan");
    obj.AddElem("Prashanth");
    obj.AddElem("William");
    obj.AddElem("Yash");
    obj.AddElem("Yuanxin");
    if (i == 0) {
      obj.ComputeCardinality();
      ans = obj.GetCardinality();
      ASSERT_EQ(ans, 4);
    }
  }

  obj.ComputeCardinality();
  ans = obj.GetCardinality();
  ASSERT_EQ(ans, 4);
}

TEST(HyperLogLogTest, PrestoCase1) {
  auto obj = HyperLogLogPresto<int64_t>(static_cast<int16_t>(1));
  auto ans = obj.GetCardinality();

  ASSERT_EQ(ans, 0);

  obj.AddElem(262144);

  obj.ComputeCardinality();
  ans = obj.GetCardinality();

  ASSERT_EQ(ans, 3);

  auto expected1 = obj.GetDenseBucket();
  ASSERT_EQ(2ULL, expected1[0].to_ullong());
  ASSERT_EQ(1, obj.GetOverflowBucketofIndex(0).to_ullong());

  obj.AddElem(0);

  obj.ComputeCardinality();
  ans = obj.GetCardinality();

  ASSERT_EQ(ans, 3);

  auto expected2 = obj.GetDenseBucket();
  ASSERT_EQ(15UL, expected2[0].to_ulong());
  ASSERT_EQ(3, obj.GetOverflowBucketofIndex(0).to_ullong());

  obj.AddElem(-9151314442816847872L);
  obj.ComputeCardinality();
  ans = obj.GetCardinality();

  ASSERT_EQ(ans, 227086569448168320UL);

  auto expected3 = obj.GetDenseBucket();
  ASSERT_EQ(8, expected3[1].to_ullong());
  ASSERT_EQ(3, obj.GetOverflowBucketofIndex(0).to_ullong());

  obj.AddElem(-1);

  obj.ComputeCardinality();
  ans = obj.GetCardinality();

  ASSERT_EQ(ans, 227086569448168320);
  auto expected4 = obj.GetDenseBucket();
  ASSERT_EQ(8, expected4[1].to_ullong());

  obj.AddElem(INT64_MIN);
  obj.ComputeCardinality();
  ans = obj.GetCardinality();

  ASSERT_EQ(ans, 14647083729406857216UL);
  auto expected5 = obj.GetDenseBucket();
  ASSERT_EQ(15UL, expected5[1].to_ulong());
}

TEST(HyperLogLogTest, PrestoCase2) {
  auto obj = HyperLogLogPresto<int64_t>(static_cast<int16_t>(0));
  auto ans = obj.GetCardinality();

  ASSERT_EQ(ans, 0);

  obj.AddElem(65536UL);
  ASSERT_EQ(obj.GetDenseBucket()[0].to_ullong(), 0);
  ASSERT_EQ(obj.GetOverflowBucketofIndex(0).to_ullong(), 1);

  obj.AddElem(INT64_MIN);
  obj.ComputeCardinality();
  ASSERT_EQ(obj.GetDenseBucket()[0].to_ullong(), 15);
  ASSERT_EQ(obj.GetOverflowBucketofIndex(0).to_ullong(), 3);

  obj.AddElem(0);
  obj.ComputeCardinality();
  ASSERT_EQ(obj.GetCardinality(), 14647083729406857216UL);
  ASSERT_EQ(obj.GetDenseBucket()[0].to_ullong(), 0);
  ASSERT_EQ(obj.GetOverflowBucketofIndex(0).to_ullong(), 4);
}

TEST(HyperLogLogTest, PrestoEdgeCase) {
  auto obj = HyperLogLogPresto<int64_t>(static_cast<int16_t>(-2));
  obj.ComputeCardinality();
  auto ans = obj.GetCardinality();

  ASSERT_EQ(ans, 0);
}

static void PrintMetricsI64(const char* title,
                            uint64_t true_card, uint64_t hll_est,
                            long long set_ms, long long hll_ms,
                            size_t N, int16_t n_bits) {
  const double rel_err = (true_card == 0) ? 0.0
      : std::abs(double(hll_est) - double(true_card)) / double(true_card);
  const double hll_miops = (hll_ms > 0) ? (double(N) / (1000.0 * hll_ms)) : 0.0;
  const double set_miops = (set_ms > 0) ? (double(N) / (1000.0 * set_ms)) : 0.0;
  const double m = (n_bits == 0 ? 1.0 : double(1ULL << n_bits));
  const double dense_bytes = (m * 4) / 8.0; // 4 bits/register (overflow extra not counted)

  std::cout << "\n=== " << title << " ===\n";
  std::cout << "N=" << N << ", n_bits=" << n_bits << " (m=" << uint64_t(m) << ")\n\n";
  std::cout << "[unordered_set] true=" << true_card << ", time=" << set_ms
            << " ms, throughput=" << set_miops << " M inserts/s\n";
  std::cout << "[Presto HLL]    est=" << hll_est << ", rel_err="
            << (rel_err * 100.0) << " %, time=" << hll_ms
            << " ms, throughput=" << hll_miops << " M inserts/s\n";
  std::cout << "Dense memory (registers): ~" << (dense_bytes / 1024.0) << " KiB\n";
  std::cout << "CSV,N," << N
            << ",n_bits," << n_bits
            << ",true," << true_card
            << ",est," << hll_est
            << ",rel_err," << rel_err
            << ",hll_ms," << hll_ms
            << ",set_ms," << set_ms
            << ",hll_Miops," << hll_miops
            << ",set_Miops," << set_miops
            << ",dense_bytes," << dense_bytes
            << "\n";

  // Reasonable bound for m=4096 under uniform distribution
  EXPECT_LT(rel_err, 0.15);
}

TEST(PrestoHLL_Int64_Bench, Full64BitRNG_1M_m4096) {
  const size_t N = 1'000'000;
  const int16_t n_bits = 12; // m = 4096
  const uint64_t seed = 42;

  std::mt19937_64 rng(seed);

  // Generate full 64-bit spread so top bits vary (important since int64 "hash" is identity)
  std::vector<int64_t> data;
  data.reserve(N);
  for (size_t i = 0; i < N; ++i) data.push_back(static_cast<int64_t>(rng()));

  // --- Presto HLL ---
  HyperLogLogPresto<int64_t> hll(n_bits);
  auto t0 = std::chrono::high_resolution_clock::now();
  for (auto v : data) hll.AddElem(v);
  hll.ComputeCardinality();
  auto t1 = std::chrono::high_resolution_clock::now();
  long long hll_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  uint64_t hll_est = hll.GetCardinality();

  // --- Exact baseline ---
  std::unordered_set<int64_t> truth;
  truth.reserve(N * 2);
  auto t2 = std::chrono::high_resolution_clock::now();
  for (auto v : data) truth.insert(v);
  auto t3 = std::chrono::high_resolution_clock::now();
  long long set_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();
  uint64_t true_card = truth.size();

  PrintMetricsI64("Presto HLL Benchmark (int64 full 64-bit RNG)", true_card, hll_est, set_ms, hll_ms, N, n_bits);
}
}  // namespace bustub
