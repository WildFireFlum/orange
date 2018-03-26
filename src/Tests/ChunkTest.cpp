//
// Created by Ynon on 26/03/2018.
//

#include <gtest/gtest.h>
#include "../chunk.h"

class ChunkTest : public testing::Test {

};


TEST_F(ChunkTest, TestPut) {
    Chunk<int> chunk1(1, nullptr);
    chunk1.put(1, 1);
}
