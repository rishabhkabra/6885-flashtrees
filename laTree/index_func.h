#ifndef INDEX_FUNC_T
#define INDEX_FUNC_T

#include "tree_structures.h"

struct index_func_t{
	/* Dummy index */
	int indexSize;
	int (*put_value)(void* self, index_key_t key, location_t* loc, int* abortReason);
	int (*delete_value)(void* self, index_key_t key, int* abortReason);
	int (*get_value)(void* self, index_key_t key, location_t* loc);
	int (*init_index)(void* self, int nodeSize, int bufferSize, int numNodes, int nkeys, char* additional);
	int (*destroy_index)(void* self);
	int (*flush_index)(void* self);
	int (*get_deferred_query_result)(void* self, query_answer_t* answer);
	int (*flush_query_buffers)(void* self, bool partialOk);
	int (*verify_tree)(void* self);
	int (*get_height)(void* self);
	int (*get_nkeys)(void* self);
	int (*get_num_buffers)(void* self);
    void (*get_range)(void* selfPtr, range_t* range, std::vector<flash_bt_data_t>& keys);
    void (*loading_done)(void* selfPtr);
    int (*get_nbufs_used)(void* selfPtr);
};

#endif
