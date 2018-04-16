#include "Utils.h"

#define FAKETID 0xdeaddaed

class NextId {
public:
    unsigned next();
};

unsigned numberOfThreads = 1;
unsigned nextID = 0;
static NextId next;
static __thread unsigned TID = FAKETID;

inline unsigned NextId::next() {
    return ATOMIC_FETCH_AND_INC_FULL(&nextID);
}


unsigned int getNumOfThreads() {
#ifdef GALOIS
    return Galois::Runtime::activeThreads;
#else
    return numberOfThreads;
#endif
}

unsigned int getThreadId() {
#ifdef GALOIS
    return Galois::Runtime::LL::getTID();
#else
    if (TID == FAKETID) {
        TID = next.next();
    }
    return TID;
#endif
}