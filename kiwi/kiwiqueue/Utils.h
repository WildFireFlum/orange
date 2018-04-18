#ifndef __KIWI_UTILS_H__
#define __KIWI_UTILS_H__

#include <cstdint>
#include <stdint-gcc.h>

#define KIWI_DEFAULT_CHUNK_SIZE 1024
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

extern unsigned nextID;
extern unsigned numberOfThreads;

unsigned int getNumOfThreads();

unsigned int getThreadId();

#endif //__KIWI_UTILS_H__
