//
// Created by tzlil on 04/04/18.
//

#ifndef GALOIS_KIWICHUNKINDEX_H
#define GALOIS_KIWICHUNKINDEX_H

#include "KiWiChunk.h"

template <typename K>
struct Index {
    void deleteConditional(K& key, Chunk<K>& C);

    bool putConditional(K& key, Chunk<K>& prev, Chunk<K>& C);

    Chunk<K>& loadPrev(K& key);

    Chunk<K>& loadChunk(K& key);
};

template<typename K>
void Index<K>::deleteConditional(K &key, Chunk<K> &C) {

}

template<typename K>
bool Index<K>::putConditional(K &key, Chunk<K> &prev, Chunk<K> &C) {
    return false;
}

template<typename K>
Chunk<K> &Index<K>::loadPrev(K &key) {
    return <#initializer#>;
}

template<typename K>
Chunk<K> &Index<K>::loadChunk(K &key) {
    return <#initializer#>;
}


#endif //GALOIS_KIWICHUNKINDEX_H
