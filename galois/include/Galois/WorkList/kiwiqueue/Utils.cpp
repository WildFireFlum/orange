//
// Created by Ynon on 10/04/2018.
//

#include "Utils.h"

unsigned nextID = 0;
NextId next;
__thread unsigned TID = FAKETID;

unsigned int getNumOfThreads() {
#ifdef GALOIS
    return Galois::Runtime::activeThreads;
#else
    return NUMOFTHREADS;
#endif
}

unsigned NextId::next() {
    return __sync_fetch_and_add(&nextID, 1);
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