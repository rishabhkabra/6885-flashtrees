#ifndef _FLASH_BPLUS_TREE_H_
#define _FLASH_BPLUS_TREE_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <vector>

#include <types.h>
#include <node_cache.h>
#include <subtreeInfo.h>
#include <tree_structures.h>
#include <map>
#include <google/dense_hash_map>

using namespace std;

using google::dense_hash_map;
typedef dense_hash_map<node_t, node_t> nodeToNode_t;

struct flash_btree_t{
  	short int nodeptroff;
	short int maxKeys;
	nodeToNode_t* parentTable;
	nodeToNode_t* siblingTable; /* Right sibling */
};

struct bplustreenode_t{
	int16_t level;
	int16_t n;
} __attribute__((__packed__));

int flash_btree_findkindex(flash_btree_t *btr, node_cache_t* cache, bplustreenode_t* x, index_key_t k, int *r);
flash_btree_t* allocbtree();
int flash_btree_btreesplitchild(flash_btree_t *btr, node_cache_t* cache, node_t x, int i, node_t y, subTreeInfo_t* info, index_key_t* pivot);
int flash_btree_bt_init(flash_btree_t* btr, int size, int fanout = -1);
int flash_btree_bt_destroy(flash_btree_t* btr);
int flash_btree_bt_insert(flash_btree_t *btr, node_cache_t* cache, index_key_t key, location_t* loc, subTreeInfo_t* info, vector<node_key_pair_t>* toInsertUp);
node_t flash_btree_bt_find(flash_btree_t *btr, node_cache_t* cache, index_key_t key, index_key_t keyToUpdate, subTreeInfo_t* info, node_t* foundLeafNode, index_key_t* pivotWentTo, node_t* nodeToGoNext, bool doParenting, bool fromInsert, bool freeToLock);
int flash_btree_bt_insert_up(flash_btree_t* btr, node_cache_t* cache, node_t nodeToInsertTo, vector<node_key_pair_t>* forMe, vector<node_key_pair_t>* forParent);
int flash_btree_btreeinsertnonfull(flash_btree_t *btr, node_cache_t* cache, node_t x, index_key_t k, location_t loc, index_key_t* constrainingKey, subTreeInfo_t* info);
int flash_btree_release_tree(flash_btree_t* btr, node_cache_t* cache, node_t root, int height, int rootLevel);
node_t flash_btree_create_node(flash_btree_t* btr, node_cache_t* cache, subTreeInfo_t* info, int level, int newHeight);
node_t flash_btree_new_root(flash_btree_t* btr, node_cache_t* cache, subTreeInfo_t* info);
int flash_btree_clear_parent_table(flash_btree_t* btr, node_t root);
void flash_btree_for_each_leaf_entry(flash_btree_t* btr, node_cache_t* cache, node_t root, int curHeight, leaf_iterator_func_t callback, void* state, bool cheatMode, bool doParenting);
void flash_btree_verify(flash_btree_t* btr, node_cache_t* cache, subTreeInfo_t* info, int heightToGoDown = -1);
int flash_btree_get_fanout(flash_btree_t* btr, int nodeSize);
void flash_btree_set_parent(flash_btree_t* btr, node_t node, node_t parent);
node_t flash_btree_get_parent(flash_btree_t* btr, node_t node);
int flash_btree_find_buffer_nodes(flash_btree_t* btr, node_cache_t* cache, tree_node_callback_t func, node_t root, void* state, int maxHeight);
int flash_btree_get_size_for_fanout(flash_btree_t* btr, int fanout);
void flash_btree_bt_range_query(flash_btree_t *btr, node_cache_t* cache, range_t* range, subTreeInfo_t* info, vector<flash_bt_data_t>& keys);
extern int COMPARE_KEY(index_key_t k1, index_key_t k2);

extern int numRangeQueries;
extern int pagesWentDownForRangeQueries;
extern int pagesScannedForRangeQueries;

#define MIN_FANOUT 2 /* No fanout less than 2 makes sense */

#endif 
