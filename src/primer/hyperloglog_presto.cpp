#include "primer/hyperloglog_presto.h"

#include <sys/stat.h>

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0), n_bits_(n_leading_bits), elements_exits(false) {
  if (n_leading_bits < 0 || n_leading_bits > 64) {
    n_bits_ = 0;
    std::cout<<"Invalid N Bits!"<<std::endl;
  }
  std::cout<<"HLL Presto started xD "<<n_bits_<<std::endl;
  const int buckets_required = static_cast<int>((pow(2, n_leading_bits)));
  dense_bucket_.resize(buckets_required);
  constexpr std::bitset<DENSE_BUCKET_SIZE> zerobset(0);
  std::fill(dense_bucket_.begin(), dense_bucket_.end(), zerobset);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  const hash_t hash_value = CalculateHash(val);
  const std::bitset<64> bset(hash_value);
  int index = GetBucketIndex(bset);
  if (index < 0 || index >= static_cast<int>(dense_bucket_.size())) {
    return;
  }
  elements_exits = true;
  // 7 bits = 3 MSB(in overflow bucket) + 4LSB(in dense bucket)
  std::bitset<7> zeroes = GetContiguousZeroesFromRight(bset);

  auto new_value = static_cast<uint32_t>(zeroes.to_ulong());


  // compare new val with old val. to get old val check buckets + overflow
  uint16_t dense_bucket_value = 0, overflow_bucket_value = 0;
  uint32_t old_value = 0;
  std::scoped_lock lock(mutex_);
  dense_bucket_value = static_cast<uint16_t>(dense_bucket_[index].to_ulong());
  if (overflow_bucket_.find(index) != overflow_bucket_.end()) {
    overflow_bucket_value = static_cast<uint16_t>((overflow_bucket_[index].to_ulong())<<4);
  }
  old_value = static_cast<uint32_t>(overflow_bucket_value + dense_bucket_value);
  if (new_value > old_value) {
    // overflow_bucket_.erase(index);
    dense_bucket_[index] = std::bitset<4> (new_value & 0b1111);
    // if (static_cast<uint16_t>(zeroes.to_ulong()) > static_cast<uint16_t>(15)) {
    //   overflow_bucket_[index] = std::bitset<OVERFLOW_BUCKET_SIZE> (new_value >> 4);
    // }
    if ((new_value>>4) > 0) {
      overflow_bucket_[index] = std::bitset<OVERFLOW_BUCKET_SIZE> (new_value>>4);
    }
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (!elements_exits) {
    return;
  }
  int number_of_buckets = std::pow(2,n_bits_);
  double sm = 0.0;
  std::scoped_lock lock(mutex_);
  for (int i=0; i<number_of_buckets; i++) {
    const int dense_bucket_value = static_cast<int>(dense_bucket_[i].to_ulong());
    int overflow_bucket_value = 0;
    if (overflow_bucket_.find(static_cast<uint16_t>(i))!=overflow_bucket_.end()) {
      overflow_bucket_value = static_cast<int>((overflow_bucket_[static_cast<uint16_t>(i)].to_ulong()) << 4);
    }
    int total_value = dense_bucket_value + overflow_bucket_value;

    if (total_value > 0) {
      const double cur = std::pow(2.0,-static_cast<double>(total_value));
      sm += cur;
    } else {
      sm += 1.0;
    }
  }
  const auto m = static_cast<double>(number_of_buckets);
  const double numerator = CONSTANT * m * m;
  const double intermediate_result = numerator/sm;
  cardinality_ = static_cast<uint64_t>(std::floor(intermediate_result));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
