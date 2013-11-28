#include "node_cache.h"
using namespace std;

int numberOfRealIndexPages = 0;
int numberOfIndexPagesRead = 0;
int numberOfIndexNodesAccessed = 0;
int numberOfReEvictedPages = 0;
int numberOfActualNodes = 0;
int numberOfCachedNodes = 0;

void shadowWrite(node_cache_t* self, node_t node, unsigned char* data){
	address_t shadowLoc = ftl_get_page(&self->shadow_ftl, node);
	assert(!is_valid(shadowLoc) || !is_mem_address(shadowLoc));
	self->shadowM->writeData(data, self->nodeSize, &shadowLoc);
	assert(is_valid(shadowLoc));
	assert(!is_mem_address(shadowLoc));
	ftl_add_entry(&self->shadow_ftl, node, shadowLoc);
}

void shadowRead(node_cache_t* self, node_t node, unsigned char* data){
	address_t shadowLoc = ftl_get_page(&self->shadow_ftl, node);
	assert(is_valid(shadowLoc));
	assert(!is_mem_address(shadowLoc));
	self->shadowM->readData(data, shadowLoc, self->nodeSize);
}

bool flash_exhausted(node_cache_t* self, int nNewBufs = 0, int additionalFtlItems = 0){
    if (self->m->size < 0) return false;
	return (self->m->willFillSpaceUpFillUp(node_cache_total_pages_required(self, nNewBufs, additionalFtlItems)));
}
	
int node_cache_release_buf_callback(void* selfPtr, node_buf_t* buf, bool flushing){
	int bufNumber = buf->bufNumber;
	node_t node = buf->nodeNumber;
	address_t addressReadFrom = buf->addressReadFrom;
	unsigned char* data = buf->data;
	bool dirty = buf->dirty;
	node_cache_t* self = (node_cache_t*)selfPtr;	
	assert(self->nodeSize == buf->dataSize);
	assert(!is_valid(addressReadFrom) || !is_mem_address(addressReadFrom));
	if (node<0) {
		assert(false); //bad callback, as invalid node
	}

	numberOfCachedNodes --;
	if (dirty){
		/* Don't check for this when the buf manager is being flushed
		 * actually, then we will !die! eventually */

		assert(!flash_exhausted(self));
		if (is_valid(addressReadFrom)){ //This page is being reevicted
			assert(!is_mem_address(addressReadFrom));
			numberOfReEvictedPages += 1;
		}
		address_t address = addressReadFrom;
		self->m->writeData(data, self->nodeSize, &address);
		assert(is_valid(address));
		assert(!is_mem_address(address));

		address_t prevlocation = ftl_get_page(&self->ftl, node);
		assert(is_valid(prevlocation));
		assert(is_mem_address(prevlocation) && ((int)(get_address(prevlocation))) == bufNumber);

		ftl_add_entry(&self->ftl, node, address);

		shadowWrite(self, node, data);

	}else {
		if(is_valid(addressReadFrom)){
			assert(!is_mem_address(addressReadFrom));
			ftl_add_entry(&self->ftl, node, addressReadFrom);
		}else {
            assert(false);
		}
	}
	return STATUS_OK;
}

void read_buf_from_flash(node_cache_t* n, node_buf_t* buf, address_t address, node_t nodeNumber){

	assert(is_valid(address) && !is_mem_address(address));
	n->m->readData(buf->data, address, n->nodeSize);	
	assert(buf->nodeNumber == nodeNumber);
	buf_set_page(buf, address);

	address_t v = make_mem_address(buf->bufNumber);
	assert(is_valid(v));
	assert(is_mem_address(v));
	ftl_add_entry(&n->ftl, buf->nodeNumber, v);

	shadowRead(n, nodeNumber, buf->data);
}

node_buf_t* node_cache_get_buf_node(node_cache_t* n, address_t location, node_t nodeNumber){
	node_buf_t* buf;	
	assert(is_valid(location));
	address_t address = get_address(location);
	assert(is_valid(address));
	if(is_mem_address(location)){
		buf = get_buf_number(&n->buf_mgr, (int)address, true);
		assert(buf && buf->nodeNumber == nodeNumber); 
		n->numHits++;
	}else{
		buf = buf_manager_new_buf(&n->buf_mgr, nodeNumber, false);
		assert(!is_mem_address(address));
		read_buf_from_flash(n, buf, address, nodeNumber);
		numberOfCachedNodes ++;
		assert(!buf->dirty);
	}
	assert(buf && buf->nodeNumber == nodeNumber);
	return buf;
}

int node_cache_delete_node(node_cache_t* cache, node_t node){
	address_t location = ftl_get_page(&cache->ftl, node);	
	if (is_valid(location) && is_mem_address(location)){
		address_t address = get_address(location);
		node_buf_t* buf = get_buf_number(&cache->buf_mgr, (int)(address), false);
		if (buf){
			/* Return this buffer as we no longer need it */
			assert(buf->nodeNumber >= 0);
			buf_return(&cache->buf_mgr, buf);
		}
	}
	return ftl_delete_node(&cache->ftl, node);

}

void node_cache_release_aux_node(node_cache_t* n, node_t nodeNumber, void* ptr){
	assert(ptr);
	free(ptr);
}

void* node_cache_alloc_aux_node(node_cache_t* n, node_t* newNodeNumber){
	/* First check whether the ftl can accomodate a new node */
	node_t newNode = n->curNodeNumber ++;
	if (newNodeNumber) *newNodeNumber = newNode;
	void* dasNode = (unsigned char*)(malloc(n->nodeSize));
	return dasNode;
}

void* node_cache_get_aux_node(node_cache_t* n, node_t nodeNumber, bool cheatMode){
	address_t location = ftl_get_page(&n->ftl, nodeNumber);	
	if (!is_valid(location)) return NULL;
	void* node = (unsigned char*)(malloc(n->nodeSize));
	address_t address = get_address(location);
	assert(is_valid(address));
	if (is_mem_address(location)){
		node_buf_t* buf = get_buf_number(&n->buf_mgr, (int)address, false);
		assert(buf && buf->nodeNumber == nodeNumber); 
		assert(buf->dataSize == n->nodeSize);
		memcpy(node, buf->data, buf->dataSize);
	}else{
		assert(!is_mem_address(address));
		n->m->readData((unsigned char*)node, address, n->nodeSize, cheatMode);	 /* Read in cheat mode */
		shadowRead(n, nodeNumber, (unsigned char*)node);
	}
	return node;
}

bool node_cache_is_cached(node_cache_t* n, node_t nodeNumber){
	address_t location = ftl_get_page(&n->ftl, nodeNumber);	
	assert(is_valid(location));
	return is_mem_address(location);
}

void* node_cache_get_node(node_cache_t* n, node_t nodeNumber){
	address_t location = ftl_get_page(&n->ftl, nodeNumber);	
	assert(is_valid(location));
	if (!node_cache_is_cached(n, nodeNumber)){
		assert(!is_mem_address(location));
		calculateFlashCost(OP_READ, n->nodeSize, NULL, NULL);
		numberOfIndexPagesRead ++;
	}
	node_buf_t* buf = node_cache_get_buf_node(n, location, nodeNumber);
	assert(buf);
	buf_pin(&n->buf_mgr, buf);
	n->numAccessed++;
	numberOfIndexNodesAccessed ++;
	assert(buf->nodeNumber == nodeNumber);
	return (void*)(buf->data);
}

node_buf_t* node_cache_get_node_in_memory_and_in_use(node_cache_t* n, node_t nodeNumber){
	address_t location = ftl_get_page(&n->ftl, nodeNumber);	
	assert(is_valid(location));
	assert(is_mem_address(location));
	address_t address = get_address(location);
	assert(is_valid(address));
	node_buf_t* buf = get_buf_number(&n->buf_mgr, (int)address, false);
	assert(buf && buf->nodeNumber == nodeNumber);
	assert(is_buf_held(buf));
	return buf;
}

void node_cache_write_release_aux_node(node_cache_t* n, node_t nodeNumber, void* dasNode){
	assert(dasNode);
	assert(!flash_exhausted(n));
	address_t address = INVALID_ADDRESS;
	assert(!is_valid(address));
	n->m->writeData((unsigned char*)dasNode, n->nodeSize, &address);
	assert(is_valid(address));
	assert(!is_mem_address(address));
	ftl_add_entry(&n->ftl, nodeNumber, address);
	shadowWrite(n, nodeNumber, (unsigned char*)dasNode);
	free(dasNode);
}

void node_cache_release_node(node_cache_t* n, node_t nodeNumber){
	node_buf_t* buf = node_cache_get_node_in_memory_and_in_use(n, nodeNumber);
	buf_release(&n->buf_mgr, buf);
}

void node_cache_mark_dirty(node_cache_t* n, node_t nodeNumber){
	node_buf_t* buf = node_cache_get_node_in_memory_and_in_use(n, nodeNumber);
	assert(buf->nodeNumber == nodeNumber);
	buf_dirty(&n->buf_mgr, buf);
}

/* If this function succeeds, then all the nodes mentioned can be safely
 * touched/dirtied without raising any flash exhaustion issues */
void node_cache_premark_dirty(node_cache_t* n, vector<node_t>& listOfNodes){
	int i;
	int newBufs = 0;
	for(i=0;i< int(listOfNodes.size()) ;i++){
		node_t node = listOfNodes[i];
		node_buf_t* buf = node_cache_get_node_in_memory_and_in_use(n, node);
		if (!buf->dirty) newBufs++;
	}
	for(i=0;i< int(listOfNodes.size()); i++){
		node_t node = listOfNodes[i];
		node_cache_mark_dirty(n, node);
	}
}

int node_cache_load(node_cache_t* n, int maxNumberOfBufs, writeMetadata_t* m, node_cache_root_t* r, int expectedNodeSize){
	assert(n);
	node_cache_root_print(r);
	assert(r->buf_size == expectedNodeSize);
	/* This buffer manager is local as it needs to free and delete its memory on every load */
	buf_manager_init(&n->buf_mgr, r->buf_size, maxNumberOfBufs, 1, node_cache_release_buf_callback, n, r->swap);
	n->curNodeNumber = r->curNodeNumber;
	assert(n->curNodeNumber >= 0);
	n->m = m;
	n->nodeSize = r->buf_size;
	ftl_load(FLASH_MISC, &n->ftl, &r->ftl_on_flash, m);
	assert(ftl_nItems(&n->ftl) <= n->curNodeNumber);
	//ftl_print(&n->ftl);
	return STATUS_OK;
}

void node_cache_init(node_cache_t* n, int nodeSize, int maxNumberOfBufs, writeMetadata_t* m, bool swap){ 
	assert(n);
	assert(nodeSize > 0);
	n->m = m;
	n->numHits = 0;
	n->numAccessed = 0;
	n->nodeSize = nodeSize;
	n->countToShutdown = KEYS_TO_SHUTDOWN;
	n->curNodeNumber = 0;
	ftl_init(&n->ftl, -1);
	buf_manager_init(&n->buf_mgr, nodeSize, maxNumberOfBufs, 1, node_cache_release_buf_callback, n, swap);
	n->shadowM = new writeMetadata_t(-1, NODE_SHADOW, FLASH_MISC, false, true, true);
	ftl_init(&n->shadow_ftl, -1);
}

void node_cache_flush(node_cache_t* n, node_cache_root_t* r){ //flush all nodes in the cache and the ftl.
	buf_manager_flush(&n->buf_mgr);
	//ftl_print(&n->ftl);
	numberOfRealIndexPages+= ftl_nItems(&n->ftl);
	ftl_flush(FLASH_MISC, &n->ftl, &r->ftl_on_flash, n->m);
	r->curNodeNumber = n->curNodeNumber;
	r->buf_size = n->nodeSize;
	r->swap = n->buf_mgr.swap;
	node_cache_root_print(r);
	n->m->close();
}

void node_cache_clean(node_cache_t* n){
	ftl_clean(&n->ftl);
	buf_manager_clean(&n->buf_mgr);
	n->curNodeNumber = 0;
}

void node_cache_destroy(node_cache_t* n){
	ftl_destroy(&n->ftl);
	buf_manager_destroy(&n->buf_mgr);
	if (n->shadowM){
		delete(n->shadowM);
		n->shadowM = NULL;
	}
}

void* node_cache_alloc_node(node_cache_t* n, node_t* newNodeNumber, node_t splitSibling){
	/* First check whether the ftl can accomodate a new node */
	node_t newNode = n->curNodeNumber ++;
	node_buf_t* buf = buf_manager_new_buf(&n->buf_mgr, newNode, true); /* A new node is being allocated */
	assert(!buf->dirty);
	buf->nodeNumber = newNode;
	numberOfActualNodes ++;
	assert(buf->bufNumber >= 0);
	address_t v =  make_mem_address(buf->bufNumber);
	assert(is_valid(v));
	assert(is_mem_address(v));
	ftl_add_entry(&n->ftl, newNode, v);
	if (newNodeNumber) *newNodeNumber = newNode;
	buf_pin(&n->buf_mgr, buf);
	numberOfCachedNodes ++;
	assert(buf->pinned == 1); //As this buff is newly alloced, it better not be used by anyone else
	return (void*)(buf->data);
}

void node_cache_init_root(node_cache_root_t* root){
	ftl_root_init(&root->ftl_on_flash);
	root->curNodeNumber = INVALID_NODE;
	root->swap = false;
	root->buf_size = 0;
}

int node_cache_total_dirty_pages(node_cache_t* n, int newBufsToDirty){
	assert(n->nodeSize > 0);
	return int(((double)(n->nodeSize+NAND_PAGE_SIZE - 1))/NAND_PAGE_SIZE)*(buf_manager_total_dirty_pages(&n->buf_mgr) + newBufsToDirty);
}

int node_cache_self_pages(node_cache_t* n, int additionalFtlItems){
	return ftl_self_pages(&n->ftl, additionalFtlItems);	
}

void node_cache_root_print(node_cache_root_t* root){
	fprintf(stderr, "\n[NodeCacheRoot] curNodeNumber: %hd, NodeSize: %d, Swap: %d", root->curNodeNumber, root->buf_size, root->swap);
	//ftl_print_root(&root->ftl_on_flash);
}

int node_cache_any_pinned(node_cache_t* n){
	return buf_manager_any_pinned(&n->buf_mgr);
}

int node_cache_total_pages_required(node_cache_t* self, int nNewBufs, int additionalFtlItems){
	return node_cache_total_dirty_pages(self, nNewBufs) + node_cache_self_pages(self, additionalFtlItems);
}

bool node_cache_is_valid(node_cache_t* cache, node_t nodeNumber){
	address_t location = ftl_get_page(&cache->ftl, nodeNumber);	
	return is_valid(location);
}

void node_cache_nodes_list(node_cache_t* cache, vector<node_t>& nodesList){
	nodesList.clear();
	nodesList = ftl_get_all_nodes(&cache->ftl);
}
