#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

/** @brief Dense bucket size. */
#define DENSE_BUCKET_SIZE 4
/** @brief Overflow bucket size. */
#define OVERFLOW_BUCKET_SIZE 3

/** @brief Total bucket size. */
#define TOTAL_BUCKET_SIZE (DENSE_BUCKET_SIZE + OVERFLOW_BUCKET_SIZE)

namespace bustub {

template <typename KeyType>
class HyperLogLogPresto {
  /**
   * INSTRUCTIONS: Testing framework will use the GetDenseBucket and GetOverflow function,
   * hence SHOULD NOT be deleted. It's essential to use the dense_bucket_
   * data structure.
   */
  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disabling default constructor. */
  HyperLogLogPresto() = delete;

  /** @brief Parameterized constructor. */
  explicit HyperLogLogPresto(int16_t n_leading_bits);

  /** @brief Returns the dense_bucket_ data structure. */
  auto GetDenseBucket() const -> std::vector<std::bitset<DENSE_BUCKET_SIZE>> { return dense_bucket_; }

  /** @brief Returns overflow bucket of a specific given index. */
  auto GetOverflowBucketofIndex(uint16_t idx) { return overflow_bucket_[idx]; }

  /** @brief Return the cardinality of the set. */
  auto GetCardinality() const -> uint64_t { return cardinality_; }

  /** @brief Element is added for HLL calculation. */
  auto AddElem(KeyType val) -> void;

  /** @brief Function to compute cardinality. */
  auto ComputeCardinality() -> void;

 private:
  /** @brief Calculate Hash.
   *
   * @param[in] val
   *
   * @returns hash value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
      return bustub::HashUtil::HashValue(&val_obj);
    }
    if constexpr (std::is_same<KeyType, int64_t>::value) {
      return static_cast<hash_t>(val);
    }
    return 0;
  }

  /** @brief Structure holding dense buckets (or also known as registers). */
  std::vector<std::bitset<DENSE_BUCKET_SIZE>> dense_bucket_;

  /** @brief Structure holding overflow buckets. */
  std::unordered_map<uint16_t, std::bitset<OVERFLOW_BUCKET_SIZE>> overflow_bucket_;

  /** @brief Storing cardinality value */
  uint64_t cardinality_;

  int16_t n_bits_;
  // TODO(student) - can add more data structures as required
  std::mutex mutex_;
  bool elements_exits;

  auto GetBucketIndex(const std::bitset<64> &bset) -> int {
    // int index = 0;
    // if (n_bits_ == 0) {
    //   return 1;
    // }
    // // n=4 63 62 61 60
    // for (int i=63; i>=(64-n_bits_); i--) {
    //   index = index << 1;
    //   if (bset.test(i)) {
    //     index++;
    //   }
    // }
    // return index;
    if (n_bits_ == 0) {
      return 0;
    }
    const uint64_t ull = bset.to_ullong();
    int ans =  ull >> (64 - n_bits_);
    return ans;
  }



  auto GetContiguousZeroesFromRight(const std::bitset<64> &bset) {
    int zeroes = 0;
    for (int i=0;i<=(63-n_bits_);i++) {
      if (bset.test(i)) {
        break;
      }
      zeroes++;
    }
    return std::bitset<7>(zeroes);
  }

};

}  // namespace bustub
