#include <bmgr.h>

void buf_dirty(buf_manager_t* n, node_buf_t* buf){
	assert(buf->state == BUF_ACTIVE || buf->state == BUF_PINNED);
	assert(buf->nodeNumber >= 0);
	buf->dirty = true;
}



void buf_pin(buf_manager_t* n, node_buf_t* buf){
	if (buf->pinned == 0) assert(buf->state == BUF_ACTIVE);
	else assert(buf->state == BUF_PINNED);
	buf->pinned ++;
	if (buf->pinned == 1){
		buf->state = BUF_PINNED;
		list_del_init(&buf->link); /* Remove it from the active list */
		n->pinnedCount++; /* new pin */
	}
}

static void move_to_active_list(buf_manager_t* n, node_buf_t* buf){ /* put this buffer to the start of the active list */
	assert(buf->link.prev && buf->link.next);
	list_move(&buf->link, &n->activeList);
	buf->state = BUF_ACTIVE;
}

void buf_release(buf_manager_t* n, node_buf_t* buf){
	assert(buf->pinned > 0);
	assert(buf->state == BUF_PINNED);
	buf->pinned--;
	if (buf->pinned == 0){
		n->pinnedCount--; 
		move_to_active_list(n, buf);
	}
}

bool is_buf_held(node_buf_t* buf){
	return (buf->pinned>0) ? true : false;
}

node_buf_t* get_free_buf(buf_manager_t* n){
	node_buf_t* buf = NULL;
    if(list_empty(&n->freeList)) return NULL;
    else{
        struct list_head* afterHead = n->freeList.next;
        assert(afterHead);
        list_del_init(afterHead);
        buf = get_buffer_with_link(afterHead);
        assert(buf->state == BUF_FREE);
        n->nFreeBufs --;
        assert(n->nFreeBufs >= 0);
        /* No need to reclean as bufs on the freelist are putting after cleaning */
    }
	return buf;
}

node_buf_t* find_buffer_to_evict(buf_manager_t* n){
	/* Walk in reverse to find an unpinned buffer */
	
	struct list_head* cur = NULL;
	assert(!list_empty(&n->activeList));
	node_buf_t* foundBuf = NULL;
	for(cur = n->activeList.prev; cur!=&n->activeList; cur = cur->prev)
	{
		node_buf_t* buf = get_buffer_with_link(cur);
		assert(buf->magicCode == BUF_MAGIC_CODE);
		assert(buf->state == BUF_ACTIVE);
		assert(buf->pinned == 0);
		foundBuf = buf;
		break;
	}
	return foundBuf;
}

node_buf_t* buf_to_evict(buf_manager_t* n){
	node_buf_t* buf = NULL;
	if (list_empty(&n->activeList)) {
		fprintf(stderr, "\nCouldn't find a buffer to evict !");
		return NULL;	
	}else{
		buf = find_buffer_to_evict(n);
		/* Don't clean the buffer as the callee will do that */
	}
	assert(!buf || (buf->state == BUF_ACTIVE && !buf->pinned));
	return buf;
}

node_buf_t* evict_buffer(buf_manager_t* n, node_buf_t* buf){
	if (!buf) buf = buf_to_evict(n);
	/* We can also force a pinned buffer to be forcefully released .. usually to force immediate release of memory */
    if (!buf) return NULL;
	assert(buf->state == BUF_ACTIVE);
	list_del_init(&buf->link); /* Remove the buffer from the active list */
	if (buf->dirty){
		n->nWrite++;
	}
	if (buf->noCallback || n->callback(n->callback_data, buf, false) != STATUS_FAILED){
		node_buf_clean(buf);
	}else{
		/* If evictions are allowed, they should always succeed */
		assert(false);
		buf = NULL;
	}
	return buf;
}

node_buf_t* buf_manager_new_buf(buf_manager_t* n, int nodeNumber, bool newNode){
	node_buf_t* buf = get_free_buf(n);
	if (!buf && n->swap){
		buf = evict_buffer(n, NULL);
		if (!buf){
			fprintf(stderr, "\nBufferManager Ran out of memory or flash space!");
			return NULL;
		}
	}
	if (!newNode){
        assert(nodeNumber >= 0);
		n->nRead++;
	}
	assert(buf && !buf->dirty && !buf->fakeDirty);
	assert(buf->state != BUF_PINNED); /* Can't steal a pinned buffer */
	move_to_active_list(n, buf); /* This should add it to the active queue */
	buf->nodeNumber = nodeNumber;
	return buf;
}

static void clean_move_to_free_list(buf_manager_t* n, node_buf_t* buf){ 

    /* Assumes that the buf link has been initted or that the buf is already on
     * some list */

	/* Assumes that we are on some list or that the list entry has been
	 * initted */

    node_buf_clean(buf);

    /* Add to the free list (Deleting from whichever list we were)
     * */

    list_move(&buf->link, &n->freeList);
    n->nFreeBufs ++;
    buf->state = BUF_FREE;
}

bool buf_manager_any_pinned(buf_manager_t* n){
	return (n->pinnedCount>0);
}

int buf_manager_for_each_active_buffer(buf_manager_t* n, buffer_iterator_callback_t callback_func, void* func_data){
	int a = 0;
	node_buf_t *buf, *temp;
	/* Assumed callback plays no tricks with the passed buffer */
	list_for_each_entry_safe(buf, temp, &n->activeList, link){
		callback_func(buf, func_data);	
	}
	return a;
}

int buf_manager_total_dirty_pages(buf_manager_t* n){
	int a = 0;
	/* Walk the active list -- count if dirty */
	node_buf_t *buf, *temp;
	list_for_each_entry_safe(buf, temp, &n->activeList, link){
		if (buf->dirty) a++;
	}
	return a;
}

void buf_manager_flush(buf_manager_t* n){
	node_buf_t *buf;
	assert(n->pinnedCount == 0);
	while((buf = buf_to_evict(n))){
		node_buf_t* b = evict_buffer(n, buf);
        assert(b);
		clean_move_to_free_list(n, buf); /* move to the free list */
	}
	assert(list_empty(&n->activeList));
}

void buf_manager_clean(buf_manager_t* n){
	/* Walk each list -- unlink and re init each buffer */
	node_buf_t *buf, *temp;
	assert(n->pinnedCount == 0);
	list_for_each_entry_safe(buf, temp, &n->activeList, link){
		clean_move_to_free_list(n, buf);
	}
	assert(list_empty(&n->activeList));
}

void buf_manager_destroy(buf_manager_t* n){
	/* Walk each list -- unlink and destroy each buffer */

	if (n->nbufs <= 0) return;
	node_buf_t *buf, *temp;
	assert(n->pinnedCount == 0);
	list_for_each_entry_safe(buf, temp, &n->activeList, link){
		clean_move_to_free_list(n, buf);
	}
	assert(list_empty(&n->activeList));
    if (n->data){
        free(n->data);
        n->data = NULL;
    }
    init_list_head(&n->freeList);
    n->nFreeBufs = 0;
    free(n->bufs);
    n->nbufs = 0;
}

void buf_manager_init(buf_manager_t* n, int sizeOfBuffers, int maxNumberOfBufs, int minBufs, release_callback_t fn, void* callback_data, bool swap){
	int i;
	n->callback = fn;
	n->callback_data = callback_data;
	n->swap = swap;
	n->nWrite = n->nRead = 0;
	n->minNeeded = minBufs;
	n->dataSize = sizeOfBuffers;
	n->maxNeeded = maxNumberOfBufs;
	n->pinnedCount = 0;
	n->data = NULL;
	assert(n->maxNeeded >= n->minNeeded);
	init_list_head(&n->activeList);
    /* Allocate our own memory */
    init_list_head(&n->freeList);
    n->nFreeBufs = 0;
    /* allocate, init and add the buffers to the free list */
    n->nbufs = n->maxNeeded;
    n->bufs = (node_buf_t*)(malloc(sizeof(node_buf_t) * n->nbufs));
    fprintf(stderr, "\nBuffer manager init: Innitting with %d buffers of %d size", n->nbufs, n->dataSize);
    n->data = (unsigned char*)(malloc(n->dataSize * n->nbufs));
    assert(n->data);
    for(i=n->nbufs - 1; i>=0; i--){
        n->bufs[i].data = n->data + i * n->dataSize;
        init_list_head(&n->bufs[i].link);
        n->bufs[i].bufNumber = i;
        n->bufs[i].dataSize = n->dataSize;
        n->bufs[i].magicCode = BUF_MAGIC_CODE;
        clean_move_to_free_list(n, &n->bufs[i]); /* add the buffers to the free list */
    }
}

void buf_set_page(node_buf_t* buf, address_t addressReadFrom){
	buf->addressReadFrom = addressReadFrom;
}

bool buf_manager_full(buf_manager_t* n){
	return list_empty(&n->freeList);
}

/* When touch is specified the buffer manager will treat the access of this
 * buffer as a cache hit -- use it very carefully. Only when you are getting
 * buffers to give to your clients, not for debugging or merely getting a
 * handle to the buffer */

node_buf_t* get_buf_number(buf_manager_t* n, int idx, bool touch){ /* idx is the buffer idx */
	node_buf_t* b = NULL;
    assert(idx >=0 && idx < n->nbufs);
    b = &n->bufs[idx];
	assert(b && b->state != BUF_FREE);

	/* Touch this buffer in the lru queue, if it is already in the active
	 * list.. Don't do so if it is pinned */

	if (touch){
		if (b->state == BUF_ACTIVE){
			move_to_active_list(n, b); /* It is already on the active list, just move it to the front */
		}else{
			assert(b->state == BUF_PINNED);
			/* Don't put it to the active list -- it will automatically go there on the release */
		}
	}
	return b;
}

/* Note: The caller takes the complete onus for ensuring that cleaning of this
 * buffer doesn't impact anything */
/* This is usually used in the error path (mostly due to flash exhaustion) or
 * for tree switch logic */

void buf_return(buf_manager_t* n, node_buf_t* buf){

	/* Can only free an active buffer, pinned buffers can't just be thrown
	 * away like that */

	assert(buf->state == BUF_ACTIVE); 
	clean_move_to_free_list(n, buf); 
}

void buf_immediately_release(buf_manager_t* n, int bufNumber){
	node_buf_t* buf = get_buf_number(n, bufNumber, false);
	if (buf->pinned > 0){//force releasing a pinned buffer
		buf->pinned = 0;
		n->pinnedCount --;
		move_to_active_list(n, buf);
	}
	node_buf_t* b = evict_buffer(n, buf);
    assert(b);
	clean_move_to_free_list(n, buf); /* move to the free list */
}
