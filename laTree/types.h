#ifndef _TYPES_H
#define _TYPES_H

#include <utility>
#include <stdint.h>

typedef unsigned char uint8_t;

typedef int32_t node_t;
typedef int32_t offset_t;

typedef int64_t address_t;
typedef address_t offsetptr_t;
typedef address_t pageptr_t;

#if (BIG_KEY_SIZE == 1)
#define INDEX_KEY_TYPE  int32_t
#elif (BIG_KEY_SIZE == 0)
#define INDEX_KEY_TYPE int16_t
#else
#error "Invalid big key size code"
#endif

typedef INDEX_KEY_TYPE  index_key_t;

typedef unsigned char version_t;

#define INVALID (-1)
#define AGE_NO_DEL 0
#define AGE_ASAP_DEL NUM_AGES

#define INVALID_PAGE (page_t)(-1)
#define INVALID_NODE (node_t)(-1)
#define INVALID_ADDRESS (address_t)(-1)
#define BAD_NODE (node_t)(-2)
#define SPECIAL_NODE (node_t)(-3)

#define ROUND_UP_TO_PAGE_SIZE(x) (((x) + NAND_PAGE_SIZE - 1)/NAND_PAGE_SIZE)
#define ROUND_UP_TO_ERASE_BLOCK_NPAGES(x) ((((x)+ NAND_ERASE_SIZE - 1)/NAND_ERASE_SIZE) * NAND_ERASE_SIZE)

typedef uint32_t location_t;
#define INVALID_LOCATION ((location_t)(-1))

extern int MAX_NUMBER_CACHED_NODES;
#define INVALID_KEY (index_key_t)-1
#define LARGEST_KEY_IN_NODE (index_key_t)-2
#define SMALLEST_KEY_IN_NODE (index_key_t)-4
#define PIVOT_KEY_DONT_CHANGE (index_key_t)-3

struct flash_bt_data_t{
	index_key_t key;	
	location_t loc;
	flash_bt_data_t(): key(INVALID_KEY), loc(INVALID_LOCATION){}
	flash_bt_data_t(const index_key_t& key, const location_t& loc): key(key), loc(loc) {}
	flash_bt_data_t(const flash_bt_data_t& data): key(data.key), loc(data.loc) {}
} __attribute__((__packed__));

struct query_answer_t{
	flash_bt_data_t answer;
	int timeTaken;
	query_answer_t(const flash_bt_data_t& answer, int timeTaken): answer(answer), timeTaken(timeTaken){}
	query_answer_t(const query_answer_t& copy): answer(copy.answer), timeTaken(copy.timeTaken) {}
	query_answer_t() : timeTaken(-1) {}
};

typedef std::pair<index_key_t, index_key_t> range_t;
typedef void (*leaf_iterator_func_t)(void* state, index_key_t key, node_t node, index_key_t pivot);
typedef void (*tree_node_callback_t)(void* state, node_t node, int level, int maxHeight);

enum cachingOrNoneCost_t{COST_WITHOUT_CACHING = 0, COST_WITH_CACHING = 1, ALWAYS_EVICT = 2, DO_NOTHING = 3};
enum ioOperation_t{READ_OP = 0, WRITE_OP = 1};

#endif
