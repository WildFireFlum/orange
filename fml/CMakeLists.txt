cmake_minimum_required(VERSION 3.9)
project(fml)

set(CMAKE_CXX_STANDARD 14)

add_executable(fml main.cpp pq.h chunk.h consts.h rebalance_data.h chunk.hpp AtomicMarkableReference.h)