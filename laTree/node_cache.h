#ifndef NODE_CACHE_H
#define NODE_CACHE_H

#include <ftl.h>
#include <buf_address.h>
#include <bmgr.h>
#include <vector>
#include <flash.h>
#include <set>
#include <my_debug.h>

struct node_cache_root_t{
	ftl_root_t ftl_on_flash;
	node_t curNodeNumber;
	int buf_size;
	bool swap;
};

struct node_cache_t{
	writeMetadata_t* m;
	int nodeSize;
	ftl_t ftl;
	int numHits;
	int countToShutdown;
	int numAccessed;
	int curNodeNumber;
	writeMetadata_t* shadowM;
	ftl_t shadow_ftl;
	buf_manager_t buf_mgr;
};

void* node_cache_get_node(node_cache_t* n, node_t nodeNumber);
void node_cache_mark_dirty(node_cache_t* n, node_t nodeNumber);
void node_cache_init(node_cache_t* n, int nodeSize, int maxNumberOfBufs, writeMetadata_t* m, bool swap);
bool node_cache_isfull(node_cache_t* n);
void node_cache_release_node(node_cache_t* n, node_t nodeNumber);
void* node_cache_alloc_node(node_cache_t* n, node_t* newNodeNumber, node_t splitSibling = INVALID_NODE);
void node_cache_flush(node_cache_t* n, node_cache_root_t* r);
void node_cache_clean(node_cache_t* n);
int node_cache_load(node_cache_t* n, int maxNumberOfBufs, void* m, node_cache_root_t* r, int expectedNodeSize);
void node_cache_destroy(node_cache_t* n);
void node_cache_init_root(node_cache_root_t* root);
void node_cache_root_print(node_cache_root_t* root);
int node_cache_total_dirty_pages(node_cache_t* n, int nNewBufs = 0);
int node_cache_self_pages(node_cache_t* n, int additionalFtlItems = 0);
int node_cache_any_pinned(node_cache_t* n);
void node_cache_premark_dirty(node_cache_t* n, std::vector<node_t>& listOfNodes);
int node_cache_delete_node(node_cache_t* cache, node_t node);
int node_cache_total_pages_required(node_cache_t* self, int nNewBufs = 0, int additionalFtlItems = 0);
bool node_cache_is_cached(node_cache_t* n, node_t nodeNumber);
node_buf_t* node_cache_get_node_in_memory_and_in_use(node_cache_t* n, node_t nodeNumber);
void* node_cache_get_aux_node(node_cache_t* n, node_t nodeNumber, bool cheatMode = true);
void node_cache_release_aux_node(node_cache_t* n, node_t nodeNumber, void* ptr);
bool node_cache_is_valid(node_cache_t* cache, node_t nodeNumber);
void node_cache_write_release_aux_node(node_cache_t* n, node_t nodeNumber, void* dasNode);
void* node_cache_alloc_aux_node(node_cache_t* n, node_t* newNodeNumber);
void node_cache_take_sample(node_cache_t* cache);
void node_cache_nodes_list(node_cache_t* cache, vector<node_t>& nodesList);

#define  KEYS_TO_SHUTDOWN 500 /* For taking the cache stats sample */

#endif
