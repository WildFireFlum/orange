//
// Created by Ynon on 10/04/2018.
//


#ifndef KIWI_UTILS_H
#define KIWI_UTILS_H

#define NUMOFTHREADS 1
#define FAKETID 0xdeaddaed

class NextId {
public:
    unsigned next();
};

extern unsigned nextID;
extern NextId next;

unsigned int getNumOfThreads();

unsigned int getThreadId();

#endif //KIWI_UTILS_H
