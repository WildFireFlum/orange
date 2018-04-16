#ifndef __KIWI_UTILS_H__
#define __KIWI_UTILS_H__

#include <cstdint>
#include <stdint-gcc.h>

#define KIWI_DEFAULT_CHUNK_SIZE 1024
#define ATOMIC_CAS_MB(p, o, n) __sync_bool_compare_and_swap(p, o, n)
#define ATOMIC_FETCH_AND_INC_FULL(p) __sync_fetch_and_add(p, 1)


template <typename T>
inline bool is_marked(T* j) {
    return ((uintptr_t)j & (uintptr_t)0x01) != 0;
}

template <typename T>
inline T* unset_mark(T* j) {
    return reinterpret_cast<T*>((uintptr_t)j & ~(uintptr_t)0x01);
}

template <typename T>
inline T* set_mark(T* j) {
    return reinterpret_cast<T*>((uintptr_t)j | (uintptr_t)0x01);
}

extern unsigned nextID;
extern unsigned numberOfThreads;

unsigned int getNumOfThreads();

unsigned int getThreadId();

#endif //__KIWI_UTILS_H__
