#include <iostream>
#include <string>
#include "AtomicMarkableReference.h"

int main() {
    AtomicMarkableReference<int> a((int*)1, true);
    return 0;
}