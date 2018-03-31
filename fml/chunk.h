#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <cstdint>
#include <utility>
#include "AtomicMarkableReference.h"
#include "rebalance_data.h"
#include "consts.h"

constexpr unsigned long long FREEZE_MASK = 1ULL;
constexpr unsigned long long DELETED_MASK = 2ULL;
constexpr unsigned long long ALL_FLAGS_MASK(FREEZE_MASK | DELETED_MASK);

enum class Status { INFANT, NORMAL, FROZEN };

enum class RebalncedCheckResult {
    NOT_REQUIRED,
    ADDED_KEY_VAL,
    NOT_ADDED_KEY_VAL
};

template <typename V>
struct KeyElement {
    std::atomic<uint64_t> m_key{};
    AtomicMarkableReference<KeyElement> m_next;
    V* m_value;

    KeyElement();

    KeyElement(uint64_t key, KeyElement* next, V* value)
        : m_key(key), m_next(next, false), m_value(value) {}

    bool operator<(const struct KeyElement& other) const;
};

bool KeyElement::operator<(const struct KeyElement& other) const {
    return m_key < other.m_key;
}

template <typename V>
KeyElement<V>::KeyElement()
    : m_key(0), m_next(nullptr, false), m_value(nullptr) {}

static RebalanceData nullRebalancedData(nullptr, nullptr);
static RebalanceData* nullRebalancedDataPtr(&nullRebalancedData);

template <typename V>
struct Chunk {
    Chunk(uint64_t min_key, Chunk* rebalance_parent);

    enum RebalncedCheckResult checkRebalance(uint64_t key, V& val);

    bool rebalance(uint64_t key, V& val);

    /**
     * Create unique key: 32 for key | 30 random bits | 2 flags bits (set to
     * zero)
     * @param key
     * @param val
     * @return
     */
    bool put(uint32_t key, V& val);

    bool delMin(std::pair<uint32_t, V>& out);

    std::pair<KeyElement*, KeyElement*> find(uint64_t key);
    bool addToList(KeyElement<V>* key);
    bool remove_from_list(KeyElement<V>* key);

    void normalize();

    bool policy();

    // main list info
    uint64_t m_min_key;          // the minimal key in the chunk
    std::atomic<Chunk*> m_next;  // TODO: should be atomic markable refernce

    // inner list info
    KeyElement m_begin_sentinel;  // sorted list begin sentinel (-infinity)
    KeyElement m_k[CHUNK_SIZE];   // keys sorted list
    KeyElement m_end_sentinel;    // sorted list begin sentinel (infinity)

    // inner memory info
    std::atomic<uint32_t> m_count;
    V m_v[CHUNK_SIZE];  // values container

    // rebalance info
    enum Status m_rebalance_status;
    Chunk* m_rebalance_parent;
    std::atomic<RebalanceData*> m_rebalance_data;
};


#include "chunk.hpp"

#endif  //__CHUNK_H__
