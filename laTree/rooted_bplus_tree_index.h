#ifndef _SUBTREE_B_TREE_INDEX_H_
#define _SUBTREE_B_TREE_INDEX_H_

#include <flash_split_up_bplustree.h>
#include <write_metadata.h>
#include <tree_func.h>

struct btree_index_t{
    flash_btree_t btr;
    writeMetadata_t* metadata;
    node_cache_t cache;
};

struct btree_start_t{
  node_cache_root_t node_cache_on_disk;
  node_t root;
  short int maxKeys;
};

void b_tree_describe_tree(tree_func_t* f);
buf_manager_t* b_tree_get_buffer_manager(void* selfPtr);


#endif
