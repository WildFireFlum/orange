//
// Created by tzlil on 24/03/18.
//

#ifndef FML_REBALANCE_DATA_H
#define FML_REBALANCE_DATA_H


#include "chunk.h"

template <typename T>
struct RebalanceData {
    RebalanceData(Chunk* first, Chunk* next);
    Chunk* m_first;
    std::atomic<Chunk*> m_next;
};

template<typename T>
RebalanceData<T>::RebalanceData(Chunk *first, Chunk *next) : m_first(first), m_next(next) {}

#endif //FML_REBALANCE_DATA_H
