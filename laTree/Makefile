CXX=g++
GOOGLE_SPARSEHASH_INCLUDE=./googleSparseHash
CFLAGS=-Wall -g -m32 -I$(GOOGLE_SPARSEHASH_INCLUDE) -I. -DBIG_KEY_SIZE=1 -DNO_PRINTF
LDFLAGS= -lm
OBJS=bmgr.o buf_address.o buffer.o flash.o ftl_hash.o list_buffer.o tree_structures.o util.o rooted_bplus_tree_index.o flash_split_up_bplustree.o node_cache.o subtree_tree.o index_workload.o
ALL_TARGETS=lasubtreetest
IOTRACE_TARGETS=ssd_reader benchmark_ssd
CC=$(CXX)

CXXFLAGS=$(CFLAGS)
SRCS=$(OBJS:.o=.cc)

all:depend $(ALL_TARGETS) $(IOTRACE_TARGETS)

$(ALL_TARGETS): $(OBJS)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS)

ssd_reader: raw_metadata.h run_ssd_trace.cc util.cc util.h
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ run_ssd_trace.cc util.cc 

benchmark_ssd: util.cc benchmark_ssd.cc
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ benchmark_ssd.cc util.cc 
	
depend .depend dep:
	$(CC) $(CFLAGS) -MM $(SRCS) > .depend

ifeq (.depend,$(wildcard .depend))
include .depend
endif

clean: execclean objclean

execclean: 
	-rm -f $(ALL_TARGETS) $(IOTRACE_TARGETS)

objclean:
	-rm -f *.o .depend
