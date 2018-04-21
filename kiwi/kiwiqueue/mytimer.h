#ifndef __KIWI_TIMER_H__
#define __KIWI_TIMER_H__

#include <stdint-gcc.h>
#include <ctime>
#include <cstdio>
#include "Utils.h"

enum ListIndex{
    LI_NORMALIZE = 0,
    LI_REBALANCE,
    LI_PUSH,
    LI_POP,
    LI_MAX
};

static __thread clock_t start_time[LI_MAX];
static __thread uint64_t count[LI_MAX];
static __thread uint64_t sum[LI_MAX];

static const char* enum_name(enum ListIndex idx) {
    switch (idx) {
        case LI_NORMALIZE:
            return "NORMALIZE";
        case LI_REBALANCE:
            return "REBALANCE";
        case LI_PUSH:
            return "PUSH";
        case LI_POP:
            return "POP";
        default:
            return "UNKNOWN";
    }
}

static void start(enum ListIndex idx) {
    start_time[idx] = clock();
}

static void end(enum ListIndex idx) {
    sum[idx] += clock() - start_time[idx];
    count[idx]++;
}

static void print(enum ListIndex idx) {
    printf("%d) %s, %lu msec\n", getThreadId(), enum_name(idx) , sum[idx] / count[idx]);
}

static void print() {
    for (int i = 0; i < LI_MAX; i++) {
        if (count[i]) print((enum ListIndex)i);
    }
}

#endif //__KIWI_TIMER_H__
