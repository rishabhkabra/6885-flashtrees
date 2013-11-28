#ifndef BUFFER_INTERFACE
#define BUFFER_INTERFACE

#include "tree_structures.h"
#include "write_metadata.h"
#include <google/dense_hash_map>

using namespace std;
using google::dense_hash_map;
typedef dense_hash_map<int, int> hangingBuffers_t;

struct subtreeDetails_t;

int tree_buffer_init(writeMetadata_t* m);
int tree_buffer_destroy();

tree_buffer_t tree_buffer_alloc();
int tree_buffer_node_destroy(tree_buffer_t* buffer);
int tree_buffer_load(tree_buffer_t* buf, subtreeDetails_t* details, int startPage, bufferInfo_t* bufInfo);

int tree_buffer_start_adding(tree_buffer_t* buf);
int tree_buffer_add_delta(tree_buffer_t* buf, tree_delta_t* delta, node_t beingEmptied = INVALID_NODE);
int tree_buffer_emptied(tree_buffer_t* buf, node_t beingEmptied, subtreeDetails_t* details);
bool tree_buffer_is_full(tree_buffer_t* buf);
int tree_buffer_end_adding(tree_buffer_t* buf);
int tree_buffer_print(tree_buffer_t* buf);
int tree_buffer_info_print(bufferInfo_t* bufferInfo);
int tree_buffer_info_init(bufferInfo_t* bufferInfo);

tree_buffer_iterator_t tree_buffer_iterator_create(tree_buffer_t* buf, node_t beingEmptied = INVALID_NODE, subtreeDetails_t* details = NULL);
void tree_buffer_iterator_destroy(tree_buffer_iterator_t* iterator);
bool tree_buffer_iterator_has_next(tree_buffer_iterator_t* iterator);
int tree_buffer_iterator_get_next(tree_buffer_iterator_t* iterator, tree_delta_t* delta, bool dontAdvance);
int tree_buffer_number_entries(tree_buffer_t* buf);
int tree_buffer_number_insert_entries(tree_buffer_t* buf);
int tree_buffer_get_max_deltas(int maxMem);
int tree_buffer_find_key(tree_buffer_t* buf, index_key_t key, location_t* loc, double *costWithoutCaching, double* realCost, subtreeDetails_t* details);
int tree_buffer_flush();
int tree_buffer_num_buffers();
void tree_buffer_read_cost(tree_buffer_t* buf, double *costWithoutCaching, double* realCost, bool  doAccount, subtreeDetails_t* details);
int tree_buffer_deltas_in_last_page(tree_buffer_t* buf);
double tree_buffer_write_cost(tree_buffer_t* buf);
void tree_buffer_write_cost_this(tree_buffer_t* buf, double* costWithoutCaching, double* costWithCaching);
int tree_buffer_find_keys_in_range(tree_buffer_t* buf, std::vector<flash_bt_data_t>& keys, range_t rangeToFind, double* costNoCaching, double* realCost, subtreeDetails_t* details);

extern bool canWeEvictThisDeltaBuffer(int startPageOfBuffer);
extern void forceEvictingBuffer(int startPageOfBuffer);
extern void markBufferForEmptying(int startPageOfBuffer);

#endif
