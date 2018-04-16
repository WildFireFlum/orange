#ifndef __KIWI_GALOIS_ALLOCATOR_H__
#define __KIWI_GALOIS_ALLOCATOR_H__

#include "Allocator.h"

template <uint32_t N=2>
class GaloisAllocator : public Allocator {
public:

    GaloisAllocator() : term(Runtime::getSystemTermination()) {}

    inline void* allocate(unsigned int numOfBytes, unsigned int listIndex) {
        int e = term.getEpoch();
        e %= 3;

        if (listIndex == 0) {
            return reinterpret_cast<void*>(chunkHeap[e].allocate(numOfBytes, listIndex));
        }
        return reinterpret_cast<void*>(roHeap[e].allocate(numOfBytes, listIndex));
    }

    inline void deallocate(void* ptr, unsigned int listIndex) {
        int e = term.getEpoch();
        e %= 3;
        if (listIndex == 0) {
            chunkHeap[e].deallocate(ptr, 0);
        }
        else {
            roHeap[e].deallocate(ptr, 0);
        }
    }

    inline void reclaim(void* ptr, unsigned int listIndex) {
        int e = (term.getEpoch() + 2);
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
    static Runtime::MM::ListNodeHeap heaps[N][3];
    Runtime::TerminationDetection& term;
};

Runtime::MM::ListNodeHeap GaloisAllocator::heaps[N][3];

#endif //__KIWI_GALOIS_ALLOCATOR_H__
