#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <cstdint>
#include <utility>
#include <limits>
#include "AtomicMarkableReference.h"
#include "rebalance_data.h"
#include "consts.h"


enum class ChunkStatus { INFANT, NORMAL, FROZEN };


template <typename K>
struct KeyElement {
    K* m_key;
    AtomicMarkableReference<KeyElement> m_next;

    KeyElement();

    KeyElement(K* key, KeyElement* next);
};

template <typename K>
KeyElement<K>::KeyElement()
        : m_key(nullptr), m_next(nullptr, false) {}

template <typename K>
KeyElement<K>::KeyElement(K* key, KeyElement* next)
        : m_key(key), m_next(next, false) {}



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


template <typename K>
struct Chunk {
    Chunk(const K& min_key, Chunk<K>* rebalance_parent);


    std::pair<KeyElement*, KeyElement*> findInList(const K& key);
    bool addToList(KeyElement& item);
    bool removeFromList(KeyElement& item);

    // main list info
    K* m_min_key;                           // the minimal key in the chunk
    AtomicMarkableReference<Chunk> m_next;  // next chunk

    // inner list info
    KeyElement<K> m_begin_sentinel;  // sorted list begin sentinel (-infinity)
    KeyElement<K> m_k[CHUNK_SIZE];   // keys sorted list
    KeyElement<K> m_end_sentinel;    // sorted list begin sentinel (infinity)

    // inner memory info
    std::atomic<uint32_t> m_count;

    // rebalance info
    std::atomic<ChunkStatus> m_rebalance_status;
    Chunk<K>* m_rebalance_parent;
    std::atomic<RebalanceObject*> m_rebalance_object{};

    std::atomic<PendingPut> ppa[NUMBER_OF_THREADS];
};



template <typename K>
struct RebalanceObject {
    RebalanceObject(Chunk<K>& first, Chunk* next);
    Chunk<K>& m_first;
    std::atomic<Chunk<K>*> m_next;
};

template<typename K>
RebalanceObject<K>::RebalanceObject(Chunk<K>& first, Chunk<K> *next) : m_first(first), m_next(next) {}

template <typename K>
Chunk<K>::Chunk(K& key, Chunk<K>* rebalance_parent)
        : m_min_key(key),
          m_next(nullptr, false),
          m_end_sentinel(),
          m_begin_sentinel(nullptr, &m_end_sentinel, nullptr),
          m_count(0),
          m_rebalance_status(ChunkStatus::INFANT),
          m_rebalance_parent(rebalance_parent),
          m_rebalance_object(nullptr) {}


template <typename K>
std::pair<KeyElement*, KeyElement*> Chunk<K>::findInList(const K& key) {
    KeyElement<K>* pred = nullptr;
    KeyElement<K>* curr = nullptr;
    KeyElement<K>* succ = nullptr;

    retry:
    while (true) {
        pred = &m_begin_sentinel;
        curr = pred->m_next.getRef();
        while (true) {
            bool marked = true;
            succ = curr->m_next.getMarkAndRef(marked);
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

template <typename K>
bool Chunk<K>::removeFromList(KeyElement& item) {
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

template <typename K>
bool Chunk<K>::addToList(KeyElement& item) {
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
