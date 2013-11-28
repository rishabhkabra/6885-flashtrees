#ifndef TREE_FUNC_T
#define TREE_FUNC_T

#include <types.h>
#include <subtreeInfo.h>
#include <tree_structures.h>
#include <vector>
using namespace std;

struct tree_func_t{
	/* Dummy index */
	int indexSize;
	int (*put_same_height)(void* self, index_key_t key, location_t* loc, int* abortReason, subTreeInfo_t* info, vector<node_key_pair_t>* forParent);
	node_t (*route_key)(void* self, index_key_t, index_key_t keyToUpdate, subTreeInfo_t* info, node_t* foundLeafNode, index_key_t* pivotWentTo, node_t* nodeToGoNext, bool doParenting);
	int (*init_index)(void* self, int fanout, int nodeSize, int numNodes);
	int (*destroy_index)(void* self);
	int (*verify_tree)(void* self, subTreeInfo_t* info, int heightToGoDown);
	int (*open_tree)(void* self, subTreeInfo_t* info);
	int (*close_tree)(void* self, subTreeInfo_t* info);
	int (*release_tree)(void* self, node_t root, int height, int rootLevel);
	void (*for_each_child)(void* selfPtr, node_t root, int curHeight, leaf_iterator_func_t callback, void* state, bool cheatMode, bool doParenting);
	node_t (*new_node)(void* selfPtr, subTreeInfo_t* info, int level, int newHeight);
	int (*get_fanout)(void* selfPtr, int nodeSize);
	int (*get_size_for_fanout)(void* selfPtr, int fanout);
	int (*handle_split_up)(void* selfPtr, node_t nodeToInsertTo, vector<node_key_pair_t>* forMe, vector<node_key_pair_t>* forParent);
	node_t (*get_parent)(void* selfPtr, node_t node);
	int (*flush_tree)(void* selfPtr, node_t rootNode);
	int (*get_nkeys)(void* self);
	int (*clear_parent_table)(void* self, node_t root);
	int (*for_all_nodes_matching_level)(void* self, tree_node_callback_t func, node_t node, void* state, int maxHeight);
    void (*get_range)(void* selfPtr, range_t* range, std::vector<flash_bt_data_t>& keys, subTreeInfo_t* info);
	int (*num_bufs_actual)(void* selfPtr);
};

#endif
