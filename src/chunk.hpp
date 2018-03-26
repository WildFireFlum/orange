#include "chunk.h"

template <typename Val>
Chunk<Val>::Chunk(const Key& min_key, const Chunk* parent)
    : m_min_key(&min_key),
      m_rebalance_status(Status::infant),
      m_rebalance_parent(nullptr) {}

template <typename Val>
bool Chunk<Val>::checkRebalance(const Key& key, const Val& val) {
  if (m_rebalance_status == Status::infant) {
    m_rebalance_parent->normalize();
    // put(key,val); // TODO should call global put (aka: locate target chunk C
    // and call C.put(key,val)
    return true;
  }
  if (m_current_index >= CHUNK_SIZE || m_rebalance_status == Status::frozen ||
      policy()) {
    if (!rebalance(key, val)) {
      // put(key,val); // TODO should call global put (aka: locate target chunk
      // C and call C.put(key,val)
    }
    return true;
  }
  return false;
}

template <typename Val>
void Chunk<Val>::normalize() {
  // TODO: skip it for now
}

template <typename Val>
bool Chunk<Val>::policy() {
  // TODO: skip it for now
  return false;
}

template <typename Val>
bool Chunk<Val>::rebalance(const Key& key, const Val& val) {
  // TODO: complete put first
  return false;
}

template <typename Val>
void Chunk<Val>::put(const Key& key, const Val& val) {
  if (checkRebalance(key, val)) {
    return;  // required rebalance completed the put
  }
  uint32_t i = m_current_index.fetch_add(1);
  m_v[i] = val;
  m_k[i].m_index = i;
  m_k[i].m_key = key;  // TODO: should this be a copy?

  // If size exceeded and rebalance failed - try again
  // TODO: make sure that this part does not loop infinitely,
  // Tzlil seems to have changes checkRebalance to do this, I dont get it
  if (i >= CHUNK_SIZE && (!rebalance(key, val))) {
    put(key, val);
  }

  // TODO should set pppa[thread id] atomically - think about using
  // atomic<unsigned long long>... see algorithm 2 KiWi

}
