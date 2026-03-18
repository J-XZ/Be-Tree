#!/usr/bin/fish

thirdparty_libs/thread_safe_print/build.fish
mkdir -p build
cd build
cmake .. -G "Ninja" -DCMAKE_BUILD_TYPE=Debug
ninja
