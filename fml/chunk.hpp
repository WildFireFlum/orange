#include <limits>
#include <cstdlib>
#include <vector>
#include <algorithm>

#include "chunk.h"

template <typename V>
Chunk<V>::Chunk(uint64_t min_key, Chunk* rebalance_parent)
        : m_min_key(min_key),
          m_next(nullptr, false),
          KeyElement(std::numeric_limits<uint64_t>::max(), nullptr, nullptr),
          KeyElement(std::numeric_limits<uint64_t>::min(),
                     &m_end_sentinel,
                     nullptr),
          m_rebalance_status(Status::INFANT),
          m_rebalance_parent(rebalance_parent),
          m_rebalance_data(&nullRebalancedData) {
    for (uint32_t i = 0; i < CHUNK_SIZE; i++) {
        m_k[i].m_value = &m_v[i];
        m_k[i].m_key = DELETED_MASK;
    }
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
PutStatus Chunk<V>::put(uint32_t key, V& val) {

    uint32_t i = m_count.fetch_add(1);
    uint64_t unique_key = (key << KEY_OFFSET) | ((uint32_t)(i << FLAGS_BIT_OFFSET));

    if (i >= CHUNK_SIZE) {
        // no more free space - trigger rebalance
        return PutStatus::NEED_REBALANCE;
    }
    m_v[i] = val;
    uint64_t ithKey = m_k[i].m_key;

    if ((ithKey & FREEZE_MASK) ||
        !m_k[i].m_key.compare_exchange_strong(ithKey, unique_key)) {
        return PutStatus::NEED_REBALANCE;
    }

    addToList(&m_k[i]);
    return PutStatus::SUCCESS;
}

template <typename V>
bool Chunk<V>::remove_from_list(KeyElement<V>* key) {
    while (true) {
        KeyElement* pred;
        KeyElement* curr;

        {
            auto elemWithKeyWindow = this->find(key->m_key);
            pred = elemWithKeyWindow.first;
            curr = elemWithKeyWindow.second;
        }

        if (curr->m_key != key->m_key) {
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
bool Chunk<V>::delMin(std::pair<uint32_t, V>& out) {
    if (m_rebalance_status == Status::FROZEN) {
        return false;  // consider joining the rebalance
    }

    auto curr = m_begin_sentinel.m_next.getRef();
    while (curr < &m_end_sentinel) {
        uint64_t currKey = curr->m_key;
        if (!(currKey & ALL_FLAGS_MASK) &&
            curr->m_key.compare_exchange_strong(currKey,
                                                currKey | DELETED_MASK)) {
            out.first = (uint32_t)(currKey >> 32);
            out.second = *curr->m_value;
            remove_from_list(curr);
            return true;
        }
        currKey = curr->m_key;
        if (currKey & FREEZE_MASK) {
            return false;  // consider joining the rebalance
        }

        // skip deleted
        curr = curr->m_next.getRef();
    }

    return false;
}

template <typename V>
std::pair<KeyElement*, KeyElement*> Chunk<V>::find(uint64_t key) {
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
void Chunk<V>::addToList(KeyElement<V>* item) {
    auto key = item->m_key.load();
    while (true) {
        auto prevNextPair(this->find(key));
        KeyElement<V>* pred = prevNextPair.first;
        KeyElement<V>* curr = prevNextPair.second;

        item->m_next.set(curr, false);
        if (pred->m_next.compareAndSet(curr, item, false, false)) {
            break;
        }
    }
}