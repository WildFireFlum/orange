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

        if (listIndex == 0) {
            return reinterpret_cast<void*>(chunkHeap[e].allocate(numOfBytes, listIndex));
        }
        return reinterpret_cast<void*>(roHeap[e].allocate(numOfBytes, listIndex));
    }

    void deallocate(void* ptr, unsigned int listIndex) {
        int e = term.getEpoch();
        std::cout << "TID: " << Galois::Runtime::LL::getTID() << " Current deallocate epoch: " << e << "\n";
        e %= 3;
        if (listIndex == 0) {
            chunkHeap[e].deallocate(ptr, 0);
        }
        else {
            roHeap[e].deallocate(ptr, 0);
        }
    }

    void reclaim(void* ptr, unsigned int listIndex) {
        int e = (term.getEpoch() + 2);
        std::cout << "TID: " << Galois::Runtime::LL::getTID() << " Current reclaim epoch: " << e << "\n";
        e %= 3;
        if (listIndex == 0) {
            chunkHeap[e].deallocate(ptr, 0);
        }
        else {
            roHeap[e].deallocate(ptr, 0);
        }
    }

private:
    // memory reclamation mechanism
    static Runtime::MM::ListNodeHeap chunkHeap[3];
    static Runtime::MM::ListNodeHeap roHeap[3];
    Runtime::TerminationDetection& term;
};

Runtime::MM::ListNodeHeap GaloisAllocator::chunkHeap[3];
Runtime::MM::ListNodeHeap GaloisAllocator::roHeap[3];

#endif //KIWI_GALOISALLOCATOR_H
