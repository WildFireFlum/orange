#ifndef __PQ_H__
#define __PQ_H__

#include "chunk.h"

template <typename Val>
struct PQ {

    PQ();

    void put(uint32_t key, Val& val);

    bool delete_min(std::pair<uint32_t, Val&>* out);

    Chunk m_begin_sentinel;
    Chunk m_end_sentinel;
};

template<typename Val>
PQ<Val>::PQ() : m_begin_sentinel(0, nullRebalancedDataPtr), m_end_sentinel(UINT64_MAX, nullptr) {
    this->m_begin_sentinel.m_next = &this->m_end_sentinel;
}

template<typename Val>
void PQ<Val>::put(uint32_t key, Val &val) {
    Chunk* chunk;
    do {
        //TODO this is not good enough since we can't rebalance m_begin_sentinal
        chunk = &this->m_begin_sentinel;
        while (chunk->m_min_key > key) chunk = chunk->m_next;
    } while (!chunk->put(key, val));
}

template<typename Val>
bool PQ<Val>::delete_min(std::pair<uint32_t, Val &> *out) {

    return false;
}

#endif //__PQ_H__
