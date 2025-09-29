// #include "primer/hyperloglog.h"
// #include <bitset>
// namespace bustub {
//
// template <typename KeyType>
// HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), n_bits_(n_bits), elementsExist(false) {
//   buckets_.resize( 1 + pow(2, n_bits_));
//   std::fill(buckets_.begin(), buckets_.end(), 0);
//   std::cout<<"HYPERLOGLOG Started xD!"<<std::endl;
// }
//
// /**
//  *
//  * @param hash
//  * @return
//  */
// template <typename KeyType>
// auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
//   /** @TODO(student) Implement this function! */
//   const std::bitset<BITSET_CAPACITY> result(hash);
//   return result;
// }
//
// template <typename KeyType>
// auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
//   /** @TODO(student) Implement this function! */
//   uint32_t n_bits = n_bits_;
//   for (int i = BITSET_CAPACITY-n_bits-1; i >= 0; --i) {
//     if (bset.test(i)) {
//       return static_cast<uint64_t>(BITSET_CAPACITY-n_bits-i);
//     }
//   }
//   return static_cast<uint64_t>(BITSET_CAPACITY+1); // No Ones in entire representation
// }
//
// template <typename KeyType>
// auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
//   /** @TODO(student) Implement this function! */
//   const hash_t hash = CalculateHash(val);
//
//   const std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash);
//   const int bucketIndex = GetBucketIndex(bset);
//   auto pValue = static_cast<uint32_t>(PositionOfLeftmostOne(bset));
//
//   if (pValue > BITSET_CAPACITY) {
//     return;
//   }
//   if (bucketIndex<1 || bucketIndex >=  static_cast<int>(buckets_.size())) {
//     return;
//   }
//   std::scoped_lock lock(mtx_);
//   elementsExist = true;
//   buckets_[bucketIndex] = std::max(buckets_[bucketIndex], pValue);
// }
//
// template <typename KeyType>
// auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
//   /** @TODO(student) Implement this function! */
//   if (!elementsExist) {return;}
//   double sm = 0.0;
//   std::scoped_lock lock(mtx_);
//   for (size_t i = 1; i < buckets_.size(); i++) {
//     if (buckets_[i]>0) {
//       const double cur = std::pow(2.0,-static_cast<double>(buckets_[i]));
//       sm += cur;
//     } else {
//       sm += 1.0;
//     }
//   }
//   const double m = static_cast<double>(buckets_.size())-1;
//   double intermediateResult = (CONSTANT * m * m)/sm;
//   intermediateResult = std::floor(intermediateResult);
//   cardinality_ = static_cast<uint64_t>(intermediateResult);
// }
//
// template class HyperLogLog<int64_t>;
// template class HyperLogLog<std::string>;
//
// }  // namespace bustub


// hyperloglog.cpp
#include "primer/hyperloglog.h"

#include <cmath>
#include <iostream>

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits)
    : cardinality_(0), n_bits_(n_bits), elementsExist(false) {
  // validate and clamp n_bits_
  if (n_bits_ < 0) {
    n_bits_ = 0;
  } else if (n_bits_ > 63) {
    n_bits_ = 63;
  }

  // number of buckets: 2^n_bits_ (use 1 when n_bits_ == 0)
  const size_t buckets = (n_bits_ == 0) ? 1u : (1u << n_bits_);
  buckets_.resize(buckets + 1);  // +1 if you intentionally use 1-based indexing in your code
  std::fill(buckets_.begin(), buckets_.end(), 0u);

  // Debug (optional)
  // std::cout << "HyperLogLog initialized: n_bits=" << n_bits_ << " buckets=" << buckets << std::endl;
}

/** Convert hash to bitset (64-bit) */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(static_cast<uint64_t>(hash));
}

/**
 * Compute rho = position of first 1 in suffix (leading zeros count + 1).
 * Suffix is the lower (64 - n_bits_) bits after removing the top n_bits_ used as index.
 * Returns 1..(64 - n_bits_) + 1. If suffix is all zeros, returns (64 - n_bits_) + 1.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  // Build 64-bit integer from bitset
  const uint64_t ull = bset.to_ullong();

  // Build suffix: shift left by n_bits_ so that the suffix's most-significant bit is at bit 63.
  uint64_t suffix = (n_bits_ == 0) ? ull : (ull << n_bits_);

  if (suffix == 0ull) {
    // All zeros in suffix → return max possible (suffix length + 1)
    return static_cast<uint64_t>((BITSET_CAPACITY - n_bits_) + 1);
  }

#if defined(__GNUC__) || defined(__clang__)
  uint32_t lz = static_cast<uint32_t>(__builtin_clzll(suffix));
#else
  // portable fallback (slower)
  uint32_t lz = 0;
  for (int i = 63; i >= 0; --i) {
    if (((suffix >> i) & 1ULL) == 0ULL) {
      ++lz;
    } else {
      break;
    }
  }
#endif

  // rho = number of leading zeros in suffix + 1
  return static_cast<uint64_t>(lz + 1);
}

/**
 * Add an element: compute hash, bucket index, rho; update bucket max.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  const hash_t h_raw = CalculateHash(val);
  const uint64_t h = static_cast<uint64_t>(h_raw);

  // safe index extraction: if n_bits_ == 0, index = 0
  const int bucket_count = (n_bits_ == 0) ? 1 : (1 << n_bits_);
  int index = 0;
  if (n_bits_ == 0) {
    index = 0;
  } else {
    index = static_cast<int>(h >> (BITSET_CAPACITY - n_bits_));
  }

  if (index < 0 || index >= bucket_count) {
    return;  // defensive
  }

  // build bitset and rho
  const std::bitset<BITSET_CAPACITY> bset(h);
  uint32_t rho = static_cast<uint32_t>(PositionOfLeftmostOne(bset));

  if (rho == 0) {
    return;  // shouldn't happen; PositionOfLeftmostOne returns >=1
  }

  // If you are using 1-based buckets in rest of code, adjust index handling appropriately.
  // Here we assume 0-based indexing for buckets_ and that buckets_.size() >= bucket_count
  {
    std::scoped_lock lock(mtx_);
    elementsExist = true;

    // If your buckets_ vector is sized as (buckets + 1) and uses 1-based indexing,
    // then you'd write to buckets_[index + 1]. Adjust accordingly.
    // Below we assume 0-based: buckets_[index]
    if (index >= static_cast<int>(buckets_.size())) {
      // defensive: if buckets_ was sized with +1 in constructor, it still should be okay.
      return;
    }

    buckets_[index] = std::max(buckets_[index], rho);
  }
}

/**
 * Compute cardinality using HLL estimator:
 * E = alpha * m^2 / sum(2^{-M[j]})
 * where m = #buckets (use the same as AddElem)
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  if (!elementsExist) {
    cardinality_ = 0;
    return;
  }

  const int number_of_buckets = (n_bits_ == 0) ? 1 : (1 << n_bits_);
  double sm = 0.0;

  {
    std::scoped_lock lock(mtx_);
    for (int i = 0; i < number_of_buckets; ++i) {
      uint32_t reg = buckets_[i];
      if (reg > 0) {
        sm += std::pow(2.0, -static_cast<double>(reg));
      } else {
        sm += 1.0;
      }
    }
  }

  const double m = static_cast<double>(number_of_buckets);
  const double intermediate = (CONSTANT * m * m) / sm;
  cardinality_ = static_cast<uint64_t>(std::floor(intermediate));
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
