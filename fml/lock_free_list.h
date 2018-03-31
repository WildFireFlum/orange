#ifndef __LOCK_FREE_LIST_H__
#define __LOCK_FREE_LIST_H__

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>

#include "consts.h"
#include "rebalance_data.h"


template <typename Node>
struct Window {

};

template <typename Node>
struct LockFreeList {

    LockFreeList();

    Window<Node> find(Node* node);

    void add(Node* node);

    void remove(Node* node);
};


#endif  //__LOCK_FREE_LIST_H__
