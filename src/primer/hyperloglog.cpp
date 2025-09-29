#include "primer/hyperloglog.h"
#include <bitset>
namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), n_bits_(n_bits), elementsExist(false) {
  buckets_.resize( 1 + pow(2, n_bits_));
  std::fill(buckets_.begin(), buckets_.end(), 0);
  std::cout<<"HYPERLOGLOG Started xD!"<<std::endl;
}

/**
 *
 * @param hash
 * @return
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  const std::bitset<BITSET_CAPACITY> result(hash);
  return result;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  uint32_t n_bits = n_bits_;
  for (int i = BITSET_CAPACITY-n_bits-1; i >= 0; --i) {
    if (bset.test(i)) {
      return static_cast<uint64_t>(BITSET_CAPACITY-n_bits-i);
    }
  }
  return static_cast<uint64_t>(BITSET_CAPACITY+1); // No Ones in entire representation
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  const hash_t hash = CalculateHash(val);

  const std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash);
  const int bucketIndex = GetBucketIndex(bset);
  auto pValue = static_cast<uint32_t>(PositionOfLeftmostOne(bset));

  if (pValue > BITSET_CAPACITY) {
    return;
  }
  if (bucketIndex<1 || bucketIndex >=  static_cast<int>(buckets_.size())) {
    return;
  }
  std::scoped_lock lock(mtx_);
  elementsExist = true;
  buckets_[bucketIndex] = std::max(buckets_[bucketIndex], pValue);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (!elementsExist) {return;}
  double sm = 0.0;
  std::scoped_lock lock(mtx_);
  for (size_t i = 1; i < buckets_.size(); i++) {
    if (buckets_[i]>0) {
      const double cur = std::pow(2.0,-static_cast<double>(buckets_[i]));
      sm += cur;
    } else {
      sm += 1.0;
    }
  }
  const double m = static_cast<double>(buckets_.size())-1;
  double intermediateResult = (CONSTANT * m * m)/sm;
  intermediateResult = std::floor(intermediateResult);
  cardinality_ = static_cast<uint64_t>(intermediateResult);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
