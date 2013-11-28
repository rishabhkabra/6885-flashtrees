#include <buffer.h>

void node_buf_clean(node_buf_t* buf){
    buf->noCallback = false;
	buf->dirty = false;
	buf->fakeDirty = false;
	buf->nodeNumber = -1;
	buf->addressReadFrom = INVALID_ADDRESS; 
	buf->level = -1;
	buf->parentEntry = -1;
	buf->prevBufNumber = -1;
	buf->pinned = 0;
	memset(buf->data, 0, buf->dataSize);
}


