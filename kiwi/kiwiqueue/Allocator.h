//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_ALLOCATOR_H
#define KIWI_ALLOCATOR_H

class Allocator {
public:

    Allocator() {}

    virtual void* allocate(unsigned int numOfBytes, unsigned int listIndex) = 0;

    virtual void deallocate(void *ptr, unsigned int listIndex) = 0;

    virtual void reclaim(void *ptr, unsigned int listIndex) = 0;
};


#endif //KIWI_ALLOCATOR_H
