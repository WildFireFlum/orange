//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_GALOISALLOCATOR_H
#define KIWI_GALOISALLOCATOR_H

#include "Allocator.h"
#include <iostream>

class GaloisAllocator : public Allocator {
public:

    GaloisAllocator() : term(Runtime::getSystemTermination()) {}

    void* allocate(unsigned int numOfBytes, unsigned int listIndex) {
        int e = term.getEpoch();
        std::cout << "TID: " << Galois::Runtime::LL::getTID() << " Current allocate epoch: " << e << "\n";
        e %= 3;
        // Manage free list of ros separately
        return reinterpret_cast<void*>(heap[e].allocate(numOfBytes, listIndex));
    }

    void deallocate(void* ptr, unsigned int listIndex) {
        int e = term.getEpoch();
        std::cout << "TID: " << Galois::Runtime::LL::getTID() << " Current deallocate epoch: " << e << "\n";
        e %= 3;
        heap[e].deallocate(ptr, listIndex);
    }

    void reclaim(void* ptr, unsigned int listIndex) {
        int e = (term.getEpoch() + 2);
        std::cout << "TID: " << Galois::Runtime::LL::getTID() << " Current reclaim epoch: " << e << "\n";
        e %= 3;
        heap[e].deallocate(ptr, listIndex);
    }

private:
    // memory reclamation mechanism
    static Runtime::MM::ListNodeHeap heap[3];
    Runtime::TerminationDetection& term;
};

Runtime::MM::ListNodeHeap GaloisAllocator::heap[3];

#endif //KIWI_GALOISALLOCATOR_H
