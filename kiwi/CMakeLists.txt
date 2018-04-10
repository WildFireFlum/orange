cmake_minimum_required(VERSION 3.6)
project(kiwi)

set(CMAKE_CXX_STANDARD 11)
add_subdirectory(lib/googletest-master)
include_directories(lib/googletest-master/googletest/include)
include_directories(lib/googletest-master/googlemock/include)
remove_definitions(-D__STRICT_ANSI__)
add_executable(kiwi main.cpp kiwiqueue/Kiwi.inl kiwiqueue/Utils.h kiwiqueue/Utils.cpp kiwiqueue/Allocator.h kiwiqueue/MockAllocator.h
        Tests/QueueTest.h Tests/SequentialQueueTest.cpp Tests/ConcurrentQueueTest.cpp)
target_link_libraries(kiwi gtest gtest_main)