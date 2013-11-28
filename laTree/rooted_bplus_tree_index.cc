#include <rooted_bplus_tree_index.h>
using namespace std;

buf_manager_t* b_tree_get_buffer_manager(void* selfPtr){
    btree_index_t* self = (btree_index_t*)selfPtr;
    assert(self);
    return &(self->cache.buf_mgr);
}

int b_tree_init_index(void* selfPtr, int fanout, int nodeSize, int numNodes){
  btree_index_t* self = (btree_index_t*)selfPtr;
  writeMetadata_t* m = new writeMetadata_t(-1, NODE_VOLUME, FLASH_LATREE_NODE, true, false, false);
  assert(m);
  node_cache_init(&self->cache, nodeSize, numNodes, m, true); 
  self->metadata = m;
  int ret = STATUS_FAILED;
  ret = flash_btree_bt_init(&self->btr, nodeSize, fanout);
  return STATUS_OK;
}

int b_tree_destroy_index(void* selfPtr){
  btree_index_t* self = (btree_index_t*)selfPtr;
  flash_btree_bt_destroy(&self->btr);
  if (self->metadata){
	delete self->metadata;
  	self->metadata = NULL;
  }
  node_cache_destroy(&self->cache);
  return STATUS_OK;
}

int b_tree_verify(void* selfPtr, subTreeInfo_t* info, int heightToGoDown){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	flash_btree_verify(&self->btr, &self->cache, info, heightToGoDown);
	return STATUS_OK;
}

int b_tree_put_value_same_height(void* selfPtr, index_key_t key, location_t* loc, int* abortReason, subTreeInfo_t* info, vector<node_key_pair_t>* forParent){
  assert(selfPtr && loc && info);
  btree_index_t* self = (btree_index_t*)selfPtr;
 
  int ret = STATUS_FAILED;
  ret = flash_btree_bt_insert(&self->btr, &self->cache, key, loc, info, forParent);
  b_tree_verify(self, info, info->rootLevel + 1);
  return ret;
}

node_t b_tree_route_key(void* selfPtr, index_key_t key, index_key_t keyToUpdate, subTreeInfo_t* info, node_t* foundLeafNode, index_key_t* pivotWentTo, node_t* nodeToGoNext, bool doParenting){
  btree_index_t* self = (btree_index_t*)selfPtr;
  node_t node = flash_btree_bt_find(&self->btr, &self->cache, key, keyToUpdate, info, foundLeafNode, pivotWentTo, nodeToGoNext, doParenting, false, true);
  return node;
}

node_t b_tree_release_tree(void* selfPtr, node_t newRoot, int height, int rootLevel){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_release_tree(&self->btr, &self->cache, newRoot, height, rootLevel);
}


int b_tree_close(void* selfPtr, subTreeInfo_t* info){
	assert(info->root >= 0);
	b_tree_release_tree(selfPtr, info->root, info->curHeight, info->rootLevel);
	return STATUS_OK;
}

int b_tree_open(void* selfPtr, subTreeInfo_t* info){
	assert(info->root >= 0);
	return STATUS_OK;
}

node_t b_tree_create_new_node(void* selfPtr, subTreeInfo_t* info, int level, int newHeight){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	node_t node = INVALID_NODE;
    node = flash_btree_create_node(&self->btr, &self->cache, info, level, newHeight);
	return node;
}

void b_tree_for_each_child(void* selfPtr, node_t root, int curHeight, leaf_iterator_func_t callback, void* state, bool cheatMode, bool doParenting){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_for_each_leaf_entry(&self->btr, &self->cache, root, curHeight, callback, state, cheatMode, doParenting);
}

int b_tree_handle_split_up(void* selfPtr, node_t nodeToInsertTo, vector<node_key_pair_t>* forMe, vector<node_key_pair_t>* forParent){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_bt_insert_up(&self->btr, &self->cache, nodeToInsertTo, forMe, forParent);
}

int b_tree_get_size_for_fanout(void* selfPtr, int fanout){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_get_size_for_fanout(&self->btr, fanout);
}

int b_tree_get_fanout(void* selfPtr, int nodeSize){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_get_fanout(&self->btr, nodeSize);
}

node_t b_tree_get_parent(void* selfPtr, node_t node){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_get_parent(&self->btr, node);
}

int b_tree_flush_index(void* selfPtr, node_t rootNode){
  btree_index_t* self = (btree_index_t*)selfPtr;
  btree_start_t r; 
  r.root = rootNode;
  r.maxKeys = self->btr.maxKeys;
  node_cache_flush(&self->cache, &r.node_cache_on_disk);
  /* Not returning this btree_start_t thing for now */
  return STATUS_OK;
}

int b_tree_get_nkeys(void* selfPtr){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return self->btr.maxKeys;
}

int b_tree_clear_parent_table(void* selfPtr, node_t root){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_clear_parent_table(&self->btr, root);
}

int b_tree_find_buffer_nodes(void* selfPtr, tree_node_callback_t func, node_t root, void* state, int maxHeight){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return flash_btree_find_buffer_nodes(&self->btr, &self->cache, func, root, state, maxHeight);
}

void b_tree_range_query(void* selfPtr, range_t* range, vector<flash_bt_data_t>& keys, subTreeInfo_t* info){
  	btree_index_t* self = (btree_index_t*)selfPtr;
    flash_btree_bt_range_query(&self->btr, &self->cache, range, info, keys);
}

int b_tree_num_bufs_used_actually(void* selfPtr){
  	btree_index_t* self = (btree_index_t*)selfPtr;
	return self->cache.buf_mgr.nbufs;
}

void b_tree_describe_tree(tree_func_t* f){
	f->indexSize = sizeof(btree_index_t);
	f->put_same_height = b_tree_put_value_same_height;
	f->route_key = b_tree_route_key;
	f->init_index = b_tree_init_index;
	f->destroy_index = b_tree_destroy_index;
	f->verify_tree = b_tree_verify; /* Not defined yet */
	f->open_tree = b_tree_open;
	f->close_tree = b_tree_close;
	f->new_node = b_tree_create_new_node;
	f->handle_split_up = b_tree_handle_split_up;
	f->release_tree = b_tree_release_tree;
	f->for_each_child = b_tree_for_each_child;
	f->get_fanout = b_tree_get_fanout;
	f->get_size_for_fanout = b_tree_get_size_for_fanout;
	f->get_parent = b_tree_get_parent;
	f->flush_tree = b_tree_flush_index;
	f->get_nkeys = b_tree_get_nkeys;
	f->clear_parent_table = b_tree_clear_parent_table;
	f->for_all_nodes_matching_level = b_tree_find_buffer_nodes;
    f->get_range = b_tree_range_query;
	f->num_bufs_actual = b_tree_num_bufs_used_actually;
}
