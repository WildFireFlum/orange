#ifndef __KIWI_MOCK_ALLOCATOR_H__
#define __KIWI_MOCK_ALLOCATOR_H__

#include <iostream>
#include "Allocator.h"

// 100 MBS
#define MOCK_ALLOC_SIZE (1024 * 1024 * 100)

/**
 * A fixed size non-releasable synchronized buffer
 */
template <uint32_t N=24>
class MockAllocator : public Allocator {
public:

    MockAllocator() : m_buf{0}, m_offset(0), m_allocations{0} {}

    void* allocate(unsigned int numOfBytes, unsigned int listIndex) {
        unsigned int old_offset = __sync_fetch_and_add(&m_offset, numOfBytes);
        if (listIndex < N) {
            ATOMIC_FETCH_AND_INC_FULL(&m_allocations[listIndex]);
            std::cout << getThreadId() << ") counters["<<listIndex <<"] = " << m_allocations[listIndex] <<  std::endl;
        } else {
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

    unsigned int getNumOfAllocs(unsigned int listIndex) {
        return m_allocations[listIndex];
    }

    void clear() {
        memset(this, 0, sizeof(*this));
    }

    char m_buf[MOCK_ALLOC_SIZE];
    volatile unsigned int m_offset;
    volatile unsigned int m_allocations[N];
};


#endif //__KIWI_MOCK_ALLOCATOR_H__
