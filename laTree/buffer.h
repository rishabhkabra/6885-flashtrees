#ifndef _BUFFER_H
#define _BUFFER_H

#include <list.h>
#include <types.h>
#include <string.h>
#include <stdlib.h>

enum bufferStatus_t {BUF_FREE, BUF_ACTIVE, BUF_PINNED};
struct node_buf_t{
	list_head link;  /* Has to be the first guy */
	int owningManagerId; 
	bool dirty;
	bool fakeDirty;
	int pinned;
	address_t addressReadFrom;
	int nodeNumber;
	uint32_t magicCode;
	bufferStatus_t state; /* which list are we on ? */
	/* fields specific for the tree */
	int bufNumber;
	int prevBufNumber;
	int parentEntry;
	int level;
	int dataSize;
    bool noCallback;
	unsigned char* data;
};

void node_buf_clean(node_buf_t* buf);

inline node_buf_t* get_buffer_with_link(list_head* linkPtr){
	return (node_buf_t*)(linkPtr);
	//return list_entry(linkPtr, node_buf_t, link);	
}

#endif
