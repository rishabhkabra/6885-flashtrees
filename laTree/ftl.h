#ifndef _FTL_H_
#define _FTL_H_

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include "statuscodes.h"
#include "types.h"
#include "write_metadata.h"
#include <vector>
#include <map>
#include "buf_address.h"
#include <google/dense_hash_map>

using namespace std;

using google::dense_hash_map;
typedef dense_hash_map<node_t, address_t> ftl_hash_t;

struct ftl_t{
	ftl_hash_t* hash;
};

struct ftl_entry_t{
	node_t node;
	address_t address;
};

struct ftl_root_t{
	address_t address;
	int sizeOnFlash; /* in bytes */
	int nItems;
};

int ftl_init(ftl_t* ftl, int initial_size);
int ftl_add_entry(ftl_t* ftl, node_t node, address_t address);
address_t ftl_get_page(ftl_t* ftl, node_t node);
int ftl_destroy(ftl_t* ftl);
int ftl_clean(ftl_t* ftl);
void ftl_flush(int caller, ftl_t* ftl, ftl_root_t* root, writeMetadata_t* m);
void ftl_load(int caller, ftl_t* ftl, ftl_root_t* root, writeMetadata_t* m);
int ftl_nItems(ftl_t* ftl);
void ftl_root_init(ftl_root_t* ftl_on_flash);
void ftl_print(ftl_t* ftl);
int ftl_self_pages(ftl_t* ftl, int additionalItems = 0);
bool ftl_is_empty(ftl_t* ftl);
int ftl_delete_node(ftl_t* ftl, node_t node);
void ftl_print_root(ftl_root_t* root);
vector<node_t> ftl_get_all_nodes(ftl_t* ftl);

#endif
