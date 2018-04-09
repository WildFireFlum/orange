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

unsigned int getThreadId() {
#ifdef GALOIS
    return Galois::Runtime::LL::getTID();
#else
    return 0;
#endif
}

#endif //KIWI_UTILS_H
