#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <cstdint>
#include <utility>
#include <limits>
#include "AtomicMarkableReference.h"
#include "rebalance_data.h"
#include "consts.h"


enum class ChunkStatus { INFANT, NORMAL, FROZEN };


template <typename V>
struct KeyElement {
    uint64_t m_key;
    AtomicMarkableReference<KeyElement> m_next;
    V* m_value;

    KeyElement();

    KeyElement(uint64_t key, KeyElement* next, V* value)
        : m_key(key), m_next(next, false), m_value(value) {}
};

template <typename V>
KeyElement<V>::KeyElement()
        : m_key(0), m_next(nullptr, false), m_value(nullptr) {}

template <typename V>
KeyElement<V>::KeyElement(uint64_t key, KeyElement* next, V* value)
        : m_key(key), m_next(next, false), m_value(value) {}



enum class PendingStatus {
    BOTTOM,
    WORKING,
    FROZEN
};

struct PendingPut {
    PendingStatus m_status :1;
    uint64_t m_idx  :7;

    PendingPut(): m_status(PendingStatus::BOTTOM), m_idx(UINT64_MAX) {};
    PendingPut(PendingStatus stat, uint64_t idx): m_status(stat), m_idx(idx) {};
};


template <typename V>
struct Chunk {
    Chunk(uint64_t min_key, Chunk<V>* rebalance_parent);


    std::pair<KeyElement*, KeyElement*> findInList(uint64_t key);
    bool addToList(KeyElement& item);
    bool removeFromList(KeyElement& item);


    // main list info
    uint64_t m_min_key;                       // the minimal key in the chunk
    AtomicMarkableReference<Chunk> m_next;    // next chunk

    // inner list info
    KeyElement m_begin_sentinel;  // sorted list begin sentinel (-infinity)
    KeyElement m_k[CHUNK_SIZE];   // keys sorted list
    KeyElement m_end_sentinel;    // sorted list begin sentinel (infinity)

    // inner memory info
    std::atomic<uint32_t> m_count;
    V m_v[CHUNK_SIZE];  // values container

    // rebalance info
    std::atomic<ChunkStatus> m_rebalance_status;
    Chunk* m_rebalance_parent;
    std::atomic<RebalanceObject*> m_rebalance_object;

    std::atomic<PendingPut> ppa[NUMBER_OF_THREADS];
};



template <typename V>
Chunk<V>::Chunk(uint64_t min_key, Chunk<V>* rebalance_parent)
        : m_min_key(min_key),
          m_next(nullptr, false),
          KeyElement(std::numeric_limits<uint64_t>::max(), nullptr, nullptr),
          KeyElement(std::numeric_limits<uint64_t>::min(),
                     &m_end_sentinel,
                     nullptr),
          m_rebalance_status(ChunkStatus::INFANT),
          m_rebalance_parent(rebalance_parent),
          m_rebalance_object(nullptr) {
    for (uint32_t i = 0; i < CHUNK_SIZE; i++) {
        m_k[i].m_value = &m_v[i];
    }
}


template <typename V>
std::pair<KeyElement*, KeyElement*> Chunk<V>::findInList(uint64_t key) {
    KeyElement* pred = nullptr;
    KeyElement* curr = nullptr;
    KeyElement* succ = nullptr;

    retry:
    while (true) {
        pred = &m_begin_sentinel;
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
                return std::pair<KeyElement*, KeyElement*>(pred, curr);
            }
            pred = curr;
            curr = succ;
        }
    }
}

template <typename V>
bool Chunk<V>::removeFromList(KeyElement& item) {
    auto key = item.m_key;
    while (true) {
        auto elemWithKeyWindow = findInList(key);
        auto pred = elemWithKeyWindow.first;
        auto curr = elemWithKeyWindow.second;

        if (curr->m_key != key) {
            return false;
        } else {
            auto succ = curr->m_next.getRef();
            if (!curr->m_next.attemptMark(succ, true)) {
                continue;
            }
            pred->m_next.compareAndSet(curr, succ, false, false);
            return true;
        }
    }
}

template <typename V>
bool Chunk<V>::addToList(KeyElement& item) {
    auto key = item.m_key;
    while (true) {
        auto elemWithKeyWindow = findInList(key);
        auto pred = elemWithKeyWindow.first;
        auto curr = elemWithKeyWindow.second;

        if (curr->m_key == key) {
            return false;
        } else {
            item.m_next.set(curr, false);
            if (pred->m_next.compareAndSet(curr, &item, false, false)) {
                return true;
            }
        }
    }
}

#endif  //__CHUNK_H__
