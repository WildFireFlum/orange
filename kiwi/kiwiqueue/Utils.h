#ifndef __KIWI_UTILS_H__
#define __KIWI_UTILS_H__

#include <cstdint>
#include <stdint-gcc.h>
#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <fcntl.h>

#define INDEX_SKIPLIST_LEVELS	20
#define RO_LIST_LEVEL           21
#define CHUNK_LIST_LEVEL        22

#define ATOMIC_CAS_MB(p, o, n) __sync_bool_compare_and_swap(p, o, n)
#define ATOMIC_FETCH_AND_INC_FULL(p) __sync_fetch_and_add(p, 1)


template <typename T>
static inline bool is_marked(T* i)
{
    return ((uintptr_t)i & (uintptr_t)0x01) != 0;
}

template <typename T>
static inline bool is_dead(T* i)
{
    return ((uintptr_t)i & (uintptr_t)0x02) == 2;
}

template <typename T>
static inline T* unset_mark(T* i)
{
    return (T *)((uintptr_t)i & ~(uintptr_t)0x03);
}

template <typename T>
static inline T* set_mark(T* i)
{
    return (T *)((uintptr_t)i | (uintptr_t)0x01);
}

template <typename T>
static inline T * set_dead(T * i)
{
    return (T *)((uintptr_t)i | (uintptr_t)0x02);
}

#ifndef GALOIS

extern unsigned nextID;
extern unsigned numberOfThreads;
unsigned int getNumOfThreads();
unsigned int getThreadId();

#else

#include "Galois/Runtime/ll/TID.h"
#include "Galois/Threads.h"

inline unsigned int getNumOfThreads() {
    return Galois::Runtime::activeThreads;
}

inline unsigned int getThreadId() {
    return Galois::Runtime::LL::getTID();
}
#endif


static __thread unsigned long seeds[3];
static __thread bool seeds_init;

//Marsaglia's xorshf generator
inline unsigned long xorshf96(unsigned long* x, unsigned long* y, unsigned long* z)  //period 2^96-1
{
    unsigned long t;
    (*x) ^= (*x) << 16;
    (*x) ^= (*x) >> 5;
    (*x) ^= (*x) << 1;

    t = *x;
    (*x) = *y;
    (*y) = *z;
    (*z) = t ^ (*x) ^ (*y);

    return *z;
}

inline long rand_range(long r)
{
    if (!seeds_init) {
        int fd = open("/dev/urandom", O_RDONLY);
        if (read(fd, seeds, 3 * sizeof(unsigned long)) < 0) {
            perror("read");
            exit(1);
        }
        close(fd);
        seeds_init = true;
    }
    long v = xorshf96(seeds, seeds + 1, seeds + 2) % r;
    v++;
    return v;
}

inline bool flip_a_coin(int p) {
    return (rand_range(100)-1) < p;
}

#endif //__KIWI_UTILS_H__
