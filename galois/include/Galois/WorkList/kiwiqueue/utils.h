//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_UTILS_H
#define KIWI_UTILS_H

#define NUMOFTHREADS 1


unsigned int getNumOfThreads() {
#ifdef GALOIS
    return Galois::Runtime::activeThreads;
#else
    return NUMOFTHREADS;
#endif
}

#define FAKETID 0xdeaddaed

static unsigned nextID = 0;

struct AtomicNextId {
    unsigned next() {
        return __sync_fetch_and_add(&nextID, 1);
    }
};
typedef AtomicNextId NextId;

static NextId next;
__thread unsigned TID = FAKETID;

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

#endif //KIWI_UTILS_H
