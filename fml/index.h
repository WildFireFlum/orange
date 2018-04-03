#include "chunk.h"

struct Index {
    void deleteConditional(uint64_t key, Chunk& chunk);

    bool putConditional(uint64_t key, Chunk& prev, Chunk& chunk);

    Chunk& loadPrev(uint64_t key);

    Chunk& loadChunk(uint64_t key);
};

void Index::deleteConditional(uint64_t key, Chunk &chunk) {

}

bool Index::putConditional(uint64_t key, Chunk &prev, Chunk &chunk) {
    return false;
}

Chunk &Index::loadPrev(uint64_t key) {
    return <#initializer#>;
}

Chunk &Index::loadChunk(uint64_t key) {
    return <#initializer#>;
}
