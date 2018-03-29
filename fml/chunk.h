#ifndef __CHUNK_H__
#define __CHUNK_H__


#include <cstdint>
#include <utility>
#include <array>
#include <atomic>
#include "consts.h"
#include "rebalance_data.h"

using Key = uint32_t;

enum Status {
    infant, normal, frozen
};


struct K_Element {
    Key m_key;
    uint32_t m_index;
    K_Element* m_next;
};

template <typename Val>
struct Chunk {

    Chunk(const Key& min_key, const Chunk& parent);

    bool checkRebalance(const Key& key, Val& val);

    bool rebalance(const Key& key, Val& val);

    void put(const Key& key, Val& val);

    void normalize();

    bool policy();

    // Membros
    Key m_min_key;
    K_Element m_k[CHUNK_SIZE];
    Val m_v[CHUNK_SIZE];
    std::atomic<int> m_pppa[NUMBER_OF_THREADS]; //negative indices for mark as freeze

    std::atomic<uint32_t> m_count;
    std::atomic<Chunk*> m_next;

    enum Status m_rebalance_status;
    Chunk* m_rebalance_parent;
    RebalanceData m_rebalance_data;
};

#include "chunk.hpp"

#endif //__CHUNK_H__
