#ifndef BUF_MANAGER_H
#define BUF_MANAGER_H

#include <types.h>
#include <statuscodes.h>
#include <buffer.h>
#include <list.h>
#include <stdlib.h>
#include <cassert>
#include <cstdio>
#include <cstring>

typedef void (*buffer_iterator_callback_t)(node_buf_t* buf, void* arg);

typedef int (*release_callback_t)(void* selfPtr, node_buf_t* buf, bool flushingFlag);

#define BUF_MAGIC_CODE ((uint32_t)(0xdeadbeef))

struct buf_manager_t{
	int myId;
	release_callback_t callback;
	void* callback_data;
	unsigned char* data;
	int dataSize;
	bool swap;
	int minNeeded;
	int maxNeeded;
	int pinnedCount;
	int nRead;	
	int nWrite;	
	node_buf_t* bufs; /* handle to array of buffers */
	int nFreeBufs;
	int nbufs;

	/* The three lists for this buffer -- remove the free one*/

	struct list_head activeList;	/* maintained in lru manner */
	struct list_head freeList;
};

node_buf_t* get_buf_number(buf_manager_t* n, int buf, bool touch);
void buf_manager_init(buf_manager_t* n, int sizeOfBuffers, int maxNumberOfBufs, int minBufs, release_callback_t fn, void* callback_data, bool swap);
void buf_manager_destroy(buf_manager_t* n);
void buf_manager_clean(buf_manager_t* n);
void buf_manager_flush(buf_manager_t* n);
int buf_manager_total_dirty_pages(buf_manager_t* n);
bool buf_manager_any_pinned(buf_manager_t* n);
node_buf_t* buf_manager_new_buf(buf_manager_t* n, int nodeNumber, bool newNode);
void buf_pin(buf_manager_t* n, node_buf_t* buf);
void buf_dirty(buf_manager_t* n, node_buf_t* buf);
bool is_buf_held(node_buf_t* buf);
void buf_immediately_release(buf_manager_t* n, int bufNumber);
void buf_release(buf_manager_t* n, node_buf_t* buf);
bool buf_manager_full(buf_manager_t* n);
void buf_return(buf_manager_t* n, node_buf_t* buf);
bool is_buf_pinned(node_buf_t* buf);
void buf_set_page(node_buf_t* buf, address_t addressReadFrom);
int buf_manager_for_each_active_buffer(buf_manager_t* n, buffer_iterator_callback_t callback_func, void* func_data);

#define ROUND_UP_NUM_BUFFERS(b, n) (((b) + (n) - 1)/(n))

#endif
