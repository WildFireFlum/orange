//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_MOCKALLOCATOR_H
#define KIWI_MOCKALLOCATOR_H

#include "Allocator.h"

class MockAllocator : public Allocator {
public:

    MockAllocator() {}

    void* allocate(unsigned int numOfBytes, unsigned int listIndex) {
        return reinterpret_cast<void*>(new char[numOfBytes]);
    }

    void deallocate(void* ptr, unsigned int listIndex) {
        // Do not release memory in mock
    }
};


#endif //KIWI_MOCKALLOCATOR_H
