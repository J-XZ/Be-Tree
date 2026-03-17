# CXXFLAGS=-Wall -std=c++11 -g -O3 
#CXXFLAGS=-Wall -std=c++11 -g -pg
 CXXFLAGS=-Wall -std=c++11 -g -pg -DDEBUG -Ithirdparty_libs/thread_safe_print/include
CC=g++

test: test.cpp betree.hpp swap_space.o backing_store.o
swap_space.o: swap_space.cpp swap_space.hpp backing_store.hpp
backing_store.o: backing_store.hpp backing_store.cpp
clean:
	$(RM) *.o test
LDFLAGS=-Lthirdparty_libs/thread_safe_print/build -lthread_safe_print
CC=g++

.PHONY: all prebuild clean

all: prebuild test

prebuild:
	cd thirdparty_libs/thread_safe_print && ./build.fish

test: test.cpp betree.hpp swap_space.o backing_store.o
	$(CC) $(CXXFLAGS) -o $@ test.cpp swap_space.o backing_store.o $(LDFLAGS) thirdparty_libs/thread_safe_print/build/libthread_safe_print.so

swap_space.o: swap_space.cpp swap_space.hpp backing_store.hpp
	$(CC) $(CXXFLAGS) -c swap_space.cpp

backing_store.o: backing_store.hpp backing_store.cpp
	$(CC) $(CXXFLAGS) -c backing_store.cpp

clean:
	cd thirdparty_libs/thread_safe_print && ./clean.fish
	$(RM) *.o test
