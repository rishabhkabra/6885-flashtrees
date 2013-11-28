#ifndef SUB_TREE_INFO_H
#define SUB_TREE_INFO_H
#include <types.h>
struct subTreeInfo_t{
	node_t root;
	int curHeight;
	int maxHeight;
	int rootLevel;
	subTreeInfo_t(int maxHeight = -1, node_t root=INVALID_NODE, int curHeight = 0, int rootLevel = -1): root(root), curHeight(curHeight), maxHeight(maxHeight), rootLevel(rootLevel) {}
	subTreeInfo_t(subTreeInfo_t& sibling, node_t newRoot) : root(newRoot), curHeight(sibling.curHeight), maxHeight(sibling.maxHeight), rootLevel(sibling.rootLevel){}
	subTreeInfo_t(const subTreeInfo_t& copy): root(copy.root), curHeight(copy.curHeight), maxHeight(copy.maxHeight), rootLevel(copy.rootLevel){}
	subTreeInfo_t& operator= (const subTreeInfo_t& copy){
		if (this != &copy){
			root = copy.root;
			curHeight = copy.curHeight;
			maxHeight = copy.maxHeight;
			rootLevel = copy.rootLevel;
		}
		return *this;
	}
};

enum{NEW_ROOT_ERROR = -1, SPLIT_TREE_ERROR = -2};

#endif
