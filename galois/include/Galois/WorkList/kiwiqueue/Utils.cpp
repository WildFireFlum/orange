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
    return numberOfThreads;
}

unsigned int getThreadId() {
    if (TID == FAKETID) {
        TID = next.next();
    }
    return TID;
}