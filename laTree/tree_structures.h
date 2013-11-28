#ifndef TREE_STRUCTURES_H
#define TREE_STRUCTURES_H

#include <stdio.h>
#include "statuscodes.h"
#include "types.h"
#include <map>
#include <vector>
/* Buffer on flash metadata */

/* Each node and each buffer manages the volume of flash given to it, the
 * node_manager does not help in this */

typedef void* tree_buffer_t;
typedef void* tree_buffer_iterator_t;
typedef int buffer_data_t; /* Right now there is no extra pvt data -- it could be something like delta chain start etc*/

#define INSERT_DELTA 1
#define QUERY_DELTA 2

typedef uint8_t deltaType_t;

struct node_manager_t;

struct tree_delta_t{
	index_key_t key;
	location_t loc;
} __attribute__((__packed__));

bool deltaSorter(const tree_delta_t& a, const tree_delta_t& b);

struct bufferSegmentHeader_t{
	uint8_t nEntries; 
    uint32_t nextSegment;
}__attribute__((__packed__));

/* Every buffer has a one byte header having its length (nEntries)... */
#define DELTAS_IN_PAGE ((NAND_PAGE_SIZE - 1 - sizeof(bufferSegmentHeader_t))/(int)(sizeof(tree_delta_t)))

struct bufferInfo_t{
	int numberEntries;
	int numberInsertEntries;
	int currentPage;
	int maxEntries;
	int minQueryTimeStamp;
	bool isBufferFull;
	bool prematureFull;
    index_key_t maxKey;
    index_key_t minKey;
	buffer_data_t privateMetadata;
};

/* The buffer is not associated with any buffer manager -- It instead has a
 * memory within it, a malloced buffer has an internal data page which it uses
 * for buffering the deltas, and it is destroyed (along with the buffer), when
 * the buffer is released */

typedef void* tree_node_t;
typedef void* tree_node_iterator_t;

/* At present, we could do away with most of the stuff we keep about a node, by
 * pushing the curNodePageSize and the currentPage into the node and buffer
 * copies on flash. And keeping the rest as some bitmask things */

struct nodeInfo_t{ /* This is the info about the node partition .. Not about the node itself */
	int curNodePageSize; /*Number of pages used*/
	int level;
	int parent; 
	index_key_t parentPivotKey;
	int lastNodeRef;
	int lastNodeRefTotal;
	int lastBufferEmptied;
	bufferInfo_t buffer;
};

void print_tree_delta(tree_delta_t* delta);
bool deltaSorter(const tree_delta_t& a, const tree_delta_t& b);

extern int MAX_NUMBER_KEYS;
extern int MAX_DELTAS_ALLOWED;
extern int BUFFER_ERASE_BLOCKS;
extern int NODE_ERASE_BLOCKS;

struct node_key_pair_t{
	index_key_t key;
	int nodeNumber; //points to subtree having keys <= thisKey
	node_key_pair_t(index_key_t key = INVALID_KEY, int nodeNumber = INVALID_NODE) : key(key), nodeNumber(nodeNumber) {}
	bool operator==(const node_key_pair_t& given){
		return (given.key == key && given.nodeNumber == nodeNumber);
	}
}__attribute__((__packed__));

void print_node_key_pair_list(std::vector<node_key_pair_t>* pairList);

#endif
