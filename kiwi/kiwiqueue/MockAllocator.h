//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_MOCKALLOCATOR_H
#define KIWI_MOCKALLOCATOR_H

#include <iostream>
#include "Allocator.h"

// 100 MBS
#define MOCK_ALLOC_SIZE (1024 * 1024 * 100)

/**
 * A fixed size non-releasable synchronized buffer
 */
class MockAllocator : public Allocator {
public:

    MockAllocator() : m_buf(new char[MOCK_ALLOC_SIZE]), m_offset(0), m_chunk_allocations(0), m_ro_allocations(0) {}

    void* allocate(unsigned int numOfBytes, unsigned int listIndex) {
        unsigned int old_offset = __sync_fetch_and_add(&m_offset, numOfBytes);
        if (listIndex == 0) {
            __sync_fetch_and_add(&m_chunk_allocations, 1);
            std::cout << "TID: " << getThreadId() << " Allocating the " << m_chunk_allocations <<  " chunk" << "\n";
        }
        else if (listIndex == 1) {
            __sync_fetch_and_add(&m_ro_allocations, 1);
            std::cout << "TID: " << getThreadId() << " Allocating the " << m_ro_allocations <<  " chunk" << "\n";
        }
        else {
            std::cout << "Error, allocating to an unidentified list" << std::endl;
            throw;
        }
        return reinterpret_cast<void *>(m_buf + old_offset);
    }

    void deallocate(void* ptr, unsigned int listIndex) {
        // Do not release memory in mock
    }

    void reclaim(void* ptr, unsigned int listIndex) {
        // Do not release memory in mock
    }

    unsigned int getNumOfChunkAllocs() {
        return m_chunk_allocations;
    }

    unsigned int getNumOfRoAllocs() {
        return m_chunk_allocations;
    }

    char *m_buf;
    volatile unsigned int m_offset;
    volatile unsigned int m_chunk_allocations;
    volatile unsigned int m_ro_allocations;
};


#endif //KIWI_MOCKALLOCATOR_H
