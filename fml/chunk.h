#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>

#include "consts.h"
#include "rebalance_data.h"
#include "AtomicMarkableReference.h"

constexpr unsigned long long FREEZE_MASK = 1ULL;
constexpr unsigned long long DELETED_MASK = 2ULL;
constexpr unsigned long long FLAGS_MASK (FREEZE_MASK | DELETED_MASK);

enum Status { INFANT, NORMAL, FROZEN };

enum checkRebalncedResult { NOT_REQUIRED, ADDED_KEY_VAL, NOT_ADDED_KEY_VAL };

template <typename V>
struct K_Element {
  std::atomic<uint64_t> m_key{};
  AtomicMarkableReference<K_Element> m_next;
  V* m_value;

  K_Element();

  K_Element(uint64_t key, K_Element* next, V* value)
      : m_key(key), m_next(next, false), m_value(value) {}

  bool operator<(const struct K_Element& other) const;
};

bool K_Element::operator<(const struct K_Element& other) const {
  return this->m_key < other.m_key;
}

template<typename V>
K_Element<V>::K_Element(): m_key(0), m_next(nullptr, false), m_value(nullptr) {

}

static RebalanceData nullRebalancedData(nullptr, nullptr);
static RebalanceData* nullRebalancedDataPtr = &nullRebalancedData;

template <typename V>
struct Chunk {
  Chunk(uint64_t min_key, Chunk* rebalance_parent);

  enum checkRebalncedResult checkRebalance(uint64_t key, V& val);

  bool rebalance(K_Element* new_key);

  bool put(uint32_t key, V& val);

  bool delete_min(std::pair<uint32_t, V>* out);


  std::pair<K_Element*, K_Element*> find(uint64_t key);
  bool add_to_list(K_Element<V>* key);
  bool remove_from_list(K_Element<V>* key);

  void normalize();

  bool policy();

  // main list info
  uint64_t m_min_key;                       // the minimal key in the chunk
  AtomicMarkableReference<Chunk> m_next;    // next chunk

  // inner list info
  K_Element m_begin_sentinel;  // sorted list begin sentinel (-infinity)
  K_Element m_k[CHUNK_SIZE];   // keys sorted list
  K_Element m_end_sentinel;    // sorted list begin sentinel (infinity)

  // inner memory info
  std::atomic<uint32_t> m_count;
  V m_v[CHUNK_SIZE];  // values container

  // rebalance info
  enum Status m_rebalance_status;
  Chunk* m_rebalance_parent;
  std::atomic<RebalanceData*> m_rebalance_data;
};

Chunk::Chunk(uint64_t min_key, Chunk* rebalance_parent)
    : m_min_key(min_key),
      m_next(nullptr, false),
      m_end_sentinel(UINT_FAST64_MAX, nullptr, nullptr),
      m_begin_sentinel(0, &m_end_sentinel, nullptr),
      m_rebalance_status(INFANT),
      m_rebalance_parent(rebalance_parent),
      m_rebalance_data(&nullRebalancedData) {
  for (int i = 0; i < CHUNK_SIZE; i++) {
    this->m_k[i].m_value = &this->m_v[i];
    this->m_k[i].m_key = DELETED_MASK;
  }
}

template <typename V>
enum checkRebalncedResult Chunk<V>::checkRebalance(uint64_t key, V& val) {
  if (this->m_rebalance_status == INFANT) {
    this->m_rebalance_parent->normalize();
    return NOT_ADDED_KEY_VAL;
  }
  if (this->m_count >= CHUNK_SIZE || this->m_rebalance_status == FROZEN ||
      this->policy()) {
    return this->rebalance(key, val) ? ADDED_KEY_VAL : NOT_ADDED_KEY_VAL;
  }
  return NOT_REQUIRED;
}

template <typename V>
void Chunk<V>::normalize() {
  // TODO: skip it for now
}

template <typename V>
bool Chunk<V>::policy() {
  // TODO: skip it for now
  return false;
}

template <typename V>
bool Chunk<V>::rebalance(K_Element* new_key) {
  // 1. engage
  RebalanceData* ro = new RebalanceData(this, this->m_next.getRef());
  if (!this->m_rebalance_data.compare_exchange_strong(nullRebalancedDataPtr,
                                                      ro)) {
    free(ro);
  }
  ro = this->m_rebalance_data;
  Chunk* last = this;
  while (ro->m_next != nullptr) {
    Chunk* next = ro->m_next;
    if (next->policy()) {
      next->m_rebalance_data.compare_exchange_strong(nullRebalancedDataPtr, ro);

      if (next->m_rebalance_data == ro) {
        ro->m_next.compare_exchange_strong(next, next->m_next.getRef());
        last = next;
      } else {
        ro->m_next.compare_exchange_strong(next, nullptr);
      }
    } else {
      ro->m_next.compare_exchange_strong(next, nullptr);
    }
  }

  while (last->m_next.getRef()->m_rebalance_data == ro) {
      last = last->m_next.getRef();
  }

  // 2. freeze
  Chunk* chunk = ro->m_first;
  while (chunk) {
    chunk->m_rebalance_status = FROZEN;
    for (int i = 0; i < CHUNK_SIZE; i++) {
      // TODO: can and should be improved
      K_Element* k = &this->m_k[i];
      uint64_t z;
      while (!((z = k->m_key) & FREEZE_MASK) &&
             !k->m_key.compare_exchange_strong(z, z | FREEZE_MASK));
    }
  }

  // 3. pick minimal version - (we don't have any ^^)

  // 4. build:
  chunk = ro->m_next;
    // TODO: size can be calculated - we need to check if it is more efficient or not...
  std::vector<K_Element> v;

  // add the new key, val
    if (new_key) {
        v.push_back(*new_key);
    }

  // add all non deleted keys in the range
  do {
    for (int i = 0; i < CHUNK_SIZE;
         i++) {  // TODO: probably i < min(CHUNK_SIZE, this->m_count) is better
                 // than that
      if (!(chunk->m_k[i].m_key & DELETED_MASK)) {
        v.push_back(chunk->m_k[i]);  // TODO: maybe we don't want to copy them
                                     // at this point
      }
    }
  } while ((chunk != last) && (chunk = chunk->m_next.getRef()));

  // sort the keys
  std::sort(v.begin(), v.end());

  // create new chunk TODO: should use memory allocation mechanism
  Chunk* Cn = new Chunk(v[0].m_key, this);
  Cn->m_begin_sentinel.m_next.set(&Cn->m_k[0], false);
  Chunk* Cf = Cn;

  // arrange the keys (and values) in a list of new chunks
  for (auto& k : v) {
    if (Cn->m_count > (CHUNK_SIZE / 2)) {
      // more than half full - create new one
      Cn->m_k[Cn->m_count - 1].m_next.set(&Cn->m_end_sentinel, false);
      Cn->m_next.set(new Chunk(k.m_key, this), false);
      Cn = Cn->m_next.getRef();
      Cn->m_begin_sentinel.m_next.set(&Cn->m_k[0], false);
    }
    uint64_t tmp = k.m_key;
      K_Element& k_element = Cn->m_k[Cn->m_count];
      k_element.m_key = tmp & ~FLAGS_MASK;                    // set key without flags
      *k_element.m_value = *k.m_value;                        // copy the value
      k_element.m_next.set(&Cn->m_k[Cn->m_count + 1], false); // set list pointer
    Cn->m_count++;
  }

  // 5. replace

    bool curr_mark = false;
    Chunk* curr_ref;
    do {
        curr_ref = last->m_next.getMarkAndRef(curr_mark);
    } while (!last->m_next.compareAndSet(curr_ref, curr_ref, false, true));

  Chunk* pred = this predecessor;  // TODO: read in the paper how they get the
                                   // predecessor...

  do {
    if (pred->m_next.compareAndSet(this, Cf, false, false)) {
      this->normalize();
      return true;
    }

    if (pred->m_next.getRef()->m_rebalance_parent == this) {
      this->normalize();
      return false;
    }
    // TODO: support rebalance without key, val
    pred->rebalance(nullptr);
  } while (true);
}





template <typename V>
bool Chunk<V>::add_to_list(K_Element<V>* key) {
  // TODO: required atomic marakable reference
}

template <typename V>
bool Chunk<V>::put(uint32_t key, V& val) {
  switch (this->checkRebalance(key, val)) {
    case NOT_REQUIRED:
      break;
    case ADDED_KEY_VAL:
      return true;
    case NOT_ADDED_KEY_VAL:
      return false;
  }

  // create unique key: 32 for key | 30 random bits | 2 flags bits (set to zero)
  // TODO: init random seed...
  uint64_t unique_key = (key << 32 | (uint32_t)random()) & ~FLAGS_MASK;
  uint32_t i = this->m_count.fetch_add(1);
  if (i >= CHUNK_SIZE) {
    // no more free space - trigger rebalance
    return this->rebalance(unique_key, val);
  }
  this->m_v[i] = val;  // TODO: CAS is not needed but we need something like
                       // volatile in java here
  uint64_t z = this->m_k[i].m_key;

  if ((z & FREEZE_MASK) ||
      !this->m_k[i].m_key.compare_exchange_strong(z, unique_key)) {
    return this->rebalance(unique_key, val);
  }

    this->add_to_list(&this->m_k[i]);

  return true;
}

template <typename V>
bool Chunk<V>::remove_from_list(K_Element<V>* key) {
    while (true) {
        auto p = this->find(key->m_key);
        K_Element* pred = p.first;
        K_Element* curr = p.second;

        if (curr->m_key != key->m_key) {
            return false;
        } else {
            K_Element* succ = curr->m_next.getRef();
            if (!curr->m_next.attemptMark(succ, true)) {
                continue;
            }
            pred->m_next.compareAndSet(curr, succ, false, false);
            return true;
        }
    }
}

template <typename V>
bool Chunk<V>::delete_min(std::pair<uint32_t, V>* out) {
  if (this->m_rebalance_status == FROZEN) {
    return false;  // consider joining the rebalance
  }

  uint64_t z;
  K_Element* curr = this->m_begin_sentinel.m_next.getRef();
  while (curr < &this->m_end_sentinel) {
    if (((z = curr->m_key) & FLAGS_MASK) &&
        curr->m_key.compare_exchange_strong(z, z | DELETED_MASK)) {
      out->first = (uint32_t)(z >> 32);
      out->second = *curr->m_value;
      this->remove_from_list(curr);
      return true;
    }
    curr = curr->m_next.getRef();
  }

  return false;
}

template<typename V>
std::pair<K_Element*, K_Element*> Chunk<V>::find(uint64_t key) {
    K_Element* pred = nullptr;
    K_Element* curr = nullptr;
    K_Element* succ = nullptr;

    retry:
    while (true) {
        pred = &this->m_begin_sentinel;
        curr = pred->m_next.getRef();
        while (true) {
            succ = curr->m_next.getRef();
            bool marked = true;
            while (marked) {
                if (!pred->m_next.compareAndSet(curr, succ, false, false)) {
                    goto retry;
                }
                curr = succ;
                succ = curr->m_next.getMarkAndRef(marked);
            }
            if (curr->m_key >= key) {
                return std::pair<K_Element*, K_Element*>(pred, curr);
            }
            pred = curr;
            curr = succ;
        }
    }
}

#include "chunk.hpp"

#endif  //__CHUNK_H__
