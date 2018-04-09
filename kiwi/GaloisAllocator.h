//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_GALOISALLOCATOR_H
#define KIWI_GALOISALLOCATOR_H

#include "Allocator.h"

class GaloisAllocator : public Allocator {
public:

    GaloisAllocator() : term(Runtime::getSystemTermination()) {}

    void* allocate(unsigned int numOfBytes, unsigned int listIndex) {
        int e = term.getEpoch() % 3;
        // Manage free list of ros separately
        T* allocated = reinterpret_cast<T*>(heap[e].allocate(sizeof(T), listIndex));
        return allocated;
    }

    void deallocate(void* ptr, unsigned int listIndex) {
        int e = (term.getEpoch() + 2) % 3;
        heap[e].deallocate(ptr, listIndex);
    }

private:
    // memory reclamation mechanism
    Runtime::MM::ListNodeHeap heap[3];
    Runtime::TerminationDetection& term;



};


#endif //KIWI_GALOISALLOCATOR_H
