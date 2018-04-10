//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_MOCKALLOCATOR_H
#define KIWI_MOCKALLOCATOR_H

#include "Allocator.h"

// 10 MBS
#define MOCK_ALLOC_SIZE (1024 * 1024 * 10)

/**
 * A fixed size non-releasable synchronized buffer
 */
class MockAllocator : public Allocator {
public:

    MockAllocator() : m_buf(new char[MOCK_ALLOC_SIZE]), m_offset(0) {}

    void* allocate(unsigned int numOfBytes, unsigned int listIndex) {
        unsigned int old_offset = __sync_fetch_and_add(&m_offset, numOfBytes);
        return reinterpret_cast<void *>(m_buf + old_offset);
    }

    void deallocate(void* ptr, unsigned int listIndex) {
        // Do not release memory in mock
    }

    char *m_buf;
    volatile unsigned int m_offset;
};


#endif //KIWI_MOCKALLOCATOR_H
