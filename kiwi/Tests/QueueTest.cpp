//
// Created by Ynon on 14/04/2018.
//

#include "QueueTest.h"

MockAllocator s_allocator;

QueueTest::QueueTest() : m_pq(nullptr) {}

void QueueTest::SetUp() {
    s_allocator.clear();
}

void QueueTest::TearDown() {
    m_pq = nullptr;
    nextID = 0;
}

kiwipq_t& QueueTest::getQueue() { return *m_pq.get(); }