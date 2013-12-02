CXX=g++
CFLAGS=-Wall -g
LDFLAGS= -lm
OBJS=storage.o buffer.o Timer.o btree.o error.o fdtree.o lruque.o main.o model.o pageTable.o
ALL_TARGETS=fdtreetest
CC=$(CXX)

CXXFLAGS=$(CFLAGS)
SRCS=$(OBJS:.o=.cpp)

all:depend $(ALL_TARGETS)

$(ALL_TARGETS): $(OBJS)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS)

	
depend .depend dep:
	$(CC) $(CFLAGS) -MM $(SRCS) > .depend

#ifeq (.depend,$(wildcard .depend))
#include .depend
#endif

clean: execclean objclean

execclean: 
	-rm -f $(ALL_TARGETS)

objclean:
	-rm -f *.o .depend
