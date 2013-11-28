#include "ftl.h"

bool ftl_entry_is_equal(void* e1, void* e2){
	ftl_entry_t* f1 = (ftl_entry_t*)e1;	
	ftl_entry_t* f2 = (ftl_entry_t*)e2;	
	return (f1->node == f2->node);
}

int ftl_init(ftl_t* ftl, int initial_size){
	/* Assert that the total size of the largest possible per partition ftl fits within a single page */
	ftl->hash = new ftl_hash_t;
	assert(ftl->hash);
	ftl->hash->set_empty_key(INVALID_NODE);
	ftl->hash->set_deleted_key(BAD_NODE);
	return STATUS_OK;
}

vector<node_t> ftl_get_all_nodes(ftl_t* ftl){
    vector<node_t> nodes;
    ftl_hash_t::iterator i = ftl->hash->begin();
    for(; i != ftl->hash->end(); i++){
        nodes.push_back(i->first);
    }
    return nodes;
}

int ftl_add_entry(ftl_t* ftl, node_t node, address_t address){
	assert(is_valid(address));
	ftl->hash->erase(node);
	ftl->hash->insert(make_pair(node, address));
	return STATUS_OK;
}

address_t ftl_get_page(ftl_t* ftl, node_t node){
	if (ftl->hash->find(node) == ftl->hash->end()) return INVALID_ADDRESS;
	else{
		address_t a = ftl->hash->find(node)->second;
		assert(is_valid(a));
		return a;
	}
}

int ftl_delete_node(ftl_t* ftl, node_t node){
	ftl->hash->erase(node);
	return STATUS_OK;
}

int ftl_destroy(ftl_t* ftl){
	delete ftl->hash;
	ftl->hash = NULL;
	return STATUS_OK;
}

int ftl_nItems(ftl_t* ftl){
	return ftl->hash->size();
}

int ftl_clean(ftl_t* ftl){
	ftl->hash->clear();
	return STATUS_OK;
}

void ftl_print(ftl_t* ftl){
	ftl_hash_t::iterator i = ftl->hash->begin();
	for(;i!=ftl->hash->end(); i++){
		fprintf(stderr, "\n %hd->%lld", i->first, i->second);
	}
}

void loadInto(ftl_t* ftl, ftl_root_t* root, writeMetadata_t* m, int caller){ /* Write to flash */
	ftl_hash_t::iterator i = ftl->hash->begin();
	assert(root);
	bool firstEntry = true;
	ftl_root_init(root);
	for(;i!=ftl->hash->end(); i++){
		ftl_entry_t entry;
		entry.node = i->first;
		entry.address = i->second;
		address_t address = INVALID_ADDRESS;
		m->writeData((unsigned char*)&entry, sizeof(ftl_entry_t), &address);
		if (firstEntry){
			root->address = address;
			firstEntry = false;
		}
	}
	root->nItems = ftl->hash->size();
	root->sizeOnFlash = root->nItems * sizeof(ftl_entry_t);
}

void loadFrom(ftl_t* ftl, ftl_root_t* root, writeMetadata_t* m, int caller){ /* Read from flash */
	ftl->hash->clear();
	int nItems = root->nItems;
	address_t address = root->address;
	for(int i = 0; i < nItems;i++){
		ftl_entry_t entry;
		m->readData((unsigned char*)&entry, address, sizeof(ftl_entry_t));
		ftl->hash->insert(make_pair(entry.node, entry.address));
		address+= sizeof(ftl_entry_t);
	}
}

void ftl_flush(int caller, ftl_t* ftl, ftl_root_t* root, writeMetadata_t* m){
	loadInto(ftl, root, m, caller);
}

void ftl_load(int caller, ftl_t* ftl, ftl_root_t* root, writeMetadata_t* m){
	/* This guy allocs memory, so there should be a full tear down before calling it */
	loadFrom(ftl, root, m, caller);
}

void ftl_root_init(ftl_root_t* root){
	assert(root);	
	root->sizeOnFlash = 0;
	root->nItems = 0;
	root->address = INVALID_ADDRESS;
}

int ftl_self_pages(ftl_t* ftl, int additionalItems){
	return ROUND_UP_TO_PAGE_SIZE((ftl->hash->size() + additionalItems) * sizeof(ftl_entry_t));
}

bool ftl_is_empty(ftl_t* ftl){
	return (ftl->hash->size() == 0);
}

void ftl_print_root(ftl_root_t* root){
	fprintf(stderr, "\n[FtlRoot]: sizeOnFlash: %d, nItems: %d, address: %lld", root->sizeOnFlash, root->nItems, root->address);
}
