#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <array>
#include <atomic>
#include <cstdint>
#include <utility>
#include "consts.h"
#include "rebalance_data.h"

// The Keys in the queue
using Key = uint32_t;

// The status of a chunk, indicating whether it is mutable or not
enum class Status { infant, normal, frozen };

/**
 * An element in the queue
 */
struct K_Element {
  Key m_key;
  uint32_t m_index;
  K_Element* m_next;
};

/**
 * Basically a pre-allocated collection of sorted list elements
 * @tparam Val - The value of the list elements
 */
template <typename Val>
class Chunk {
 public:
  /**
   * Creates a new Chunk
   * @param min_key The minimal key of the chunk
   * @param parent The previous chunk?
   */
  Chunk(const Key& min_key, const Chunk& parent);

  void put(const Key& key, Val& val);

 private:
  /**
   * Returns true if a re-balance should occur
   * @param key The key to be inserted
   * @param val The value to be inserted
   * @note Dependant on policy
   */
  bool checkRebalance(const Key& key, Val& val);

  /**
   * Rebalances a collection of chunks according to policy
   * @param key The key to be inserted
   * @param val The value to be inserted
   */
  bool rebalance(const Key& key, Val& val);

  /**
   * Does something
   */
  void normalize();

  /**
   * Determines whether a chunk should be added to a rebalance
   * @return
   */
  bool policy();

  /// The minimal key in the chunk
  const Key* m_min_key;

  /// The elements in the chunk
  K_Element m_k[CHUNK_SIZE];

  /// The values in the chunk
  Val m_v[CHUNK_SIZE];

  /// An auxiliary array which helps linearizing puts with rebalances
  std::atomic<int32_t>
      m_pppa[NUMBER_OF_THREADS];  // negative indices for mark as freeze

  // TODO: might not be needed
  std::atomic<uint32_t> m_count;

  /// The next chunk in the list
  std::atomic<Chunk*> m_next;

  Status m_rebalance_status;
  Chunk* m_rebalance_parent;
  RebalanceData m_rebalance_data;
};

#include "chunk.hpp"

#endif  //__CHUNK_H__
