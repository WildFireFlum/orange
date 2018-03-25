#include "chunk.h"

Chunk::Chunk(const Key& min_key, const Chunk& parent) :
 m_min_key(min_key), m_rebalance_status(infant), m_rebalance_parent(NULL) {

}

template<typename Val>
bool Chunk<Val>::checkRebalance(const Key &key, Val &val) {
    if (this->m_rebalance_status == infant) {
        this->m_rebalance_parent->normalize();
        //put(key,val); // TODO should call global put (aka: locate target chunk C and call C.put(key,val)
        return true;
    }
    if (this->m_count >= CHUNK_SIZE ||
            this->m_rebalance_status == frozen ||
            this->policy()) {
        if (!this->rebalance(key, val)) {
            //put(key,val); // TODO should call global put (aka: locate target chunk C and call C.put(key,val)
        }
        return true;
    }
    return false;
}

template<typename Val>
void Chunk<Val>::normalize() {
    // TODO: skip it for now
}

template<typename Val>
bool Chunk<Val>::policy() {
    // TODO: skip it for now
    return false;
}

template<typename Val>
bool Chunk<Val>::rebalance(const Key &key, Val &val) {
    // TODO: complete put first
    return false;
}

template<typename Val>
void Chunk<Val>::put(const Key& key, Val& val) {
    if (this->checkRebalance(key, val)) {
        return; // required rebalance completed the put
    }
    uint32_t i = this->m_count.fetch_add(1);
    this->m_v[i] = val;
    this->m_k[i].m_index = i;
    this->m_k[i].m_key = key;
    // TODO should set pppa[thread id] atomically - think about using atomic<unsigned long long>...
    // see algorithm 2 KiWi
}
