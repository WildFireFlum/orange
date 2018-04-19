#ifndef __KIWI_ALLOCATOR_H__
#define __KIWI_ALLOCATOR_H__

class Allocator {
public:

    Allocator() {}

    virtual void* allocate(unsigned int numOfBytes, unsigned int listIndex) = 0;

    virtual void deallocate(void *ptr, unsigned int listIndex) = 0;

    virtual void reclaim(void *ptr, unsigned int listIndex) = 0;
};


#endif //__KIWI_ALLOCATOR_H__
