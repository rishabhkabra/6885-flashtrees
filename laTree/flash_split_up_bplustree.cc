#include <flash_split_up_bplustree.h>
#include <iostream>
#include <set>

using namespace std;
int pagesReadForThisSearch = 0;
int numNodesSplit = 0;
int numRangeQueries = 0;
int pagesWentDownForRangeQueries = 0;
int pagesScannedForRangeQueries = 0;

inline node_key_pair_t* PAIR(flash_btree_t* btr, bplustreenode_t* node, int index){
	return (node_key_pair_t*)((unsigned char*)(node) + sizeof(bplustreenode_t)) + index;
}

inline index_key_t* KEYS(flash_btree_t* btr, bplustreenode_t* node, int index){
	return &(PAIR(btr, node, index)->key);
}

inline node_t* NODES(flash_btree_t* btr, bplustreenode_t* node, int index){
	return &(PAIR(btr, node, index)->nodeNumber);
}

int flash_btree_min_pairs(flash_btree_t* btr){
	/* As 2*t - 1 == maxKeys, and the min number of keys we put is t, so the number of pairs is t + 1 */
	/* which is ... (maxKeys + 1)/2 + 1 */
	return (btr->maxKeys + 1)/2 + 1;
}

void flash_btree_set_parent(flash_btree_t* btr, node_t node, node_t parent){
	assert(parent != node);
	/* fprintf(stderr, "\nParent(%d) = %d", node, parent); */
	assert(parent!= BAD_NODE);
	btr->parentTable->erase(node);
	assert(btr->parentTable->find(node) == btr->parentTable->end());
	btr->parentTable->insert(make_pair(node, parent));
}

int COMPARE_KEY(index_key_t k1, index_key_t k2){
	assert(k1 != INVALID_KEY && k2 != INVALID_KEY);

	if (k1 == k2){
		return 0;
	}

    if (k1 == SMALLEST_KEY_IN_NODE) return -1;
    else if (k2 == SMALLEST_KEY_IN_NODE) return 1;

	if (k1 == LARGEST_KEY_IN_NODE) return 1;
	else if (k2 == LARGEST_KEY_IN_NODE) return -1;
    
    assert(k1 >= 0 && k2 >= 0);
	return (k1 < k2) ? -1 : 1;
}

node_t flash_btree_get_parent(flash_btree_t* btr, node_t node){
	if (btr->parentTable->find(node) == btr->parentTable->end()){
		/* printf(stderr, "\nParent of %d unknown .. returning bad node", node); */
		return BAD_NODE;
	}
	else{
		node_t t = btr->parentTable->find(node)->second;
		if (t == SPECIAL_NODE) return BAD_NODE;
		assert (t!= BAD_NODE);
		return t;
	}
}

inline void print_node(flash_btree_t* btr, bplustreenode_t* node, node_t nodeNumber){
	int i;
	fprintf(stderr, "\nPrinting node %d with %d keys (at level %d), with parent %d, sibling %d :\n", nodeNumber, node->n, node->level, flash_btree_get_parent(btr, nodeNumber), (*btr->siblingTable)[nodeNumber]);
	node_key_pair_t* pair = NULL;
	for(i = 0; i < node->n; i++){
		pair = PAIR(btr, node, i);
		fprintf(stderr, "[Node: %d, Key: %d], ", pair->nodeNumber, pair->key);
	}
	pair = PAIR(btr, node, i);
	fprintf(stderr, "[Final node: %d, key: %d]\n", pair->nodeNumber, pair->key);
}

inline bool isNodeLeaf(int level, subTreeInfo_t* info){
	if (info->maxHeight < 0) return (level == 0);
	else{
		assert(info->curHeight >= 1); /* curHeight and maxHeight start from 1 */
		return (level + info->curHeight - 1 == info->rootLevel);
	}
}

inline void initialize_pointers_in_node(flash_btree_t* btr, bplustreenode_t* node, node_t nodeNumber){
	int i;
	assert(node);
	node->n = 0;
	node->level = -1;
	assert(btr->maxKeys > 0);
	for(i=0; i<= btr->maxKeys; i++){
		*NODES(btr, node, i) = INVALID_NODE;
		if (i < btr->maxKeys) *KEYS(btr, node, i) = LARGEST_KEY_IN_NODE;
	}
    (*btr->siblingTable)[nodeNumber] = INVALID_NODE;
}

void verify_node(flash_btree_t* btr, node_t x, bplustreenode_t* xn, index_key_t* constrainingKey, node_t parent){
#if 0 /* The enclosed code is correct, it is just commented out for speed reasons, when bugs happen, uncomment it */
	int i;
	assert(xn);
	assert(x != INVALID_NODE);
	short int n = xn->n;	
	assert(n <= btr->maxKeys && n >= 0);
	/* assert(parent != BAD_NODE); */
	assert(parent == INVALID_NODE || (xn->level == 0 && n > 0));
	/* node number x is not used, but in theory it could be */
	if (n > 0){
		for(i=0;i<=n;i++){
			if (i<n){
				assert(*KEYS(btr, xn, i) >= 0);
                assert(COMPARE_KEY(*KEYS(btr, xn, i), SMALLEST_KEY_IN_NODE) > 0);
				if (i > 0) assert(COMPARE_KEY(*KEYS(btr, xn, i-1), *KEYS(btr, xn, i)) <= 0);
				if (constrainingKey) assert(COMPARE_KEY(*KEYS(btr, xn, i), *constrainingKey) <= 0);
			}else if (i == n && xn->level > 0) assert(*KEYS(btr, xn, i) == LARGEST_KEY_IN_NODE);
			if (xn->level > 0){
				node_t childi = *NODES(btr, xn, i);
				assert(childi != INVALID_NODE);
				assert(childi != x && childi != parent);
			}
		}
	}
#endif
}

int flash_btree_find_node_index(flash_btree_t *btr, node_cache_t* cache, bplustreenode_t* xn, node_t node)
{
	int n;
	assert(xn);
	n = 0;
	while (n <= xn->n  && (*NODES(btr, xn, n) != node)) n++;
	if (n > xn->n) n = -1;
	else assert(n >= 0);
	return n;
}

int flash_btree_findkindex(flash_btree_t *btr, node_cache_t* cache, bplustreenode_t* xn, index_key_t k, int* r)
{
	int tr;
	int* rr = (r == NULL) ? &tr : r;
	int n;
	assert(xn);
	n = 0;
	while (n < xn->n  && (*rr = COMPARE_KEY(k, *KEYS(btr, xn, n))) > 0) n++;
	assert(n >= 0 && n <= xn->n);
	return n;
}

node_t flash_btree_create_node(flash_btree_t* btr, node_cache_t* cache, subTreeInfo_t* info, int level, int newHeight){
	assert(info->maxHeight >= 1 || info->maxHeight < 0);
	assert((info->maxHeight < 0 || newHeight <= info->maxHeight) && info->curHeight >= 0 && newHeight > info->curHeight);
	node_t new_node;
	bplustreenode_t* node = NULL;
	node = (bplustreenode_t*)node_cache_alloc_node(cache, &new_node, INVALID_NODE);
	vector<node_t> nodesToDirty;
	nodesToDirty.push_back(new_node);
	node_cache_premark_dirty(cache, nodesToDirty);
	initialize_pointers_in_node(btr, node, new_node);
	assert(node);
	node->level = level;
	info->curHeight = newHeight;
	info->rootLevel = level;
	info->root = new_node;
	assert(*NODES(btr, node, btr->maxKeys) == INVALID_NODE);
	flash_btree_set_parent(btr, new_node, INVALID_NODE);
	node_cache_release_node(cache, new_node);
	return new_node;
}

int flash_btree_get_size_for_fanout(flash_btree_t* btr, int fanout){
	return sizeof(node_key_pair_t)*fanout + sizeof(bplustreenode_t);
}

int flash_btree_get_fanout(flash_btree_t* btr, int nodeSize){
	if (nodeSize < 0){
		return btr->maxKeys + 1;
	}else{
		assert(nodeSize >= int(sizeof(bplustreenode_t)) );
		return (nodeSize - sizeof(bplustreenode_t))/sizeof(node_key_pair_t);
	}
}

int flash_btree_bt_destroy(flash_btree_t* btr)
{
	if (btr->siblingTable){
		delete btr->siblingTable;
		btr->siblingTable = NULL;
	}
	if (btr->parentTable){
		delete btr->parentTable;
		btr->parentTable = NULL;
	}
	return STATUS_OK;
}


int flash_btree_bt_init(flash_btree_t* btr, int size, int fanout)
{
	short int maxKeys = flash_btree_get_fanout(btr, size) - 1;
	assert(maxKeys >= 1);
	memset(btr, 0, sizeof(flash_btree_t));
	btr->maxKeys = maxKeys;
	if (fanout >= 0){
		int nKeys = fanout - 1;
		assert(nKeys <= btr->maxKeys && nKeys >= MIN_FANOUT - 1); /* The min b+ tree fanout is 4 */
		btr->maxKeys = nKeys;
	}
	assert(btr->maxKeys + 1 >= MIN_FANOUT); /* not caring that 2*t - 1 == odd */
	//assert(btr->maxKeys + 1 >= MIN_FANOUT && btr->maxKeys % 2 == 1); /* 2*t - 1 == odd */
	btr->parentTable = new nodeToNode_t;
	btr->siblingTable = new nodeToNode_t;
	btr->parentTable->set_empty_key(INVALID_NODE);
	btr->parentTable->set_deleted_key(BAD_NODE);

	btr->siblingTable->set_empty_key(INVALID_NODE);
	btr->siblingTable->set_deleted_key(BAD_NODE);
	return STATUS_OK;
}

struct dfsRoot_t{
	node_t node;
	int curlevel;
	dfsRoot_t(node_t node, int curlevel = 0): node(node), curlevel(curlevel) {}
};

int flash_btree_release_tree_helper(flash_btree_t* btr, node_cache_t* cache, node_t root, int height, int rootLevel, int curDepth){
	if (!node_cache_is_cached(cache, root)){
		assert(curDepth == 0); /* Only the root of this subtree can be uncached .. others are ensured to be cached */
		return STATUS_OK;
	}
	int i;
	bplustreenode_t* curNode = NULL;
	curNode = (bplustreenode_t*)node_cache_get_node(cache, root);
	assert(rootLevel - curDepth == curNode->level);
	node_buf_t* buf = node_cache_get_node_in_memory_and_in_use(cache, root);
	assert(buf->pinned == 1); /* Besides this guy, nobody else should be holding this node */
	vector<node_t> tempNodes;
	if (curDepth < height - 1){
		assert(curNode->level > 0);
		for(i = 0;i<=curNode->n;i++){
			node_t child = *NODES(btr, curNode, i);
			if (child != INVALID_NODE && node_cache_is_cached(cache, child)) tempNodes.push_back(child);
		}
	}
	node_cache_release_node(cache, root);
	for(i = 0;i< int(tempNodes.size());i++){
		node_t child = tempNodes[i];
		int ret = flash_btree_release_tree_helper(btr, cache, child, height, rootLevel, curDepth + 1);
		assert(ret == STATUS_OK);
	}
	return STATUS_OK;
}

int flash_btree_release_tree(flash_btree_t* btr, node_cache_t* cache, node_t root, int height, int rootLevel){
	return flash_btree_release_tree_helper(btr, cache, root, height, rootLevel, 0);
}

/* This guy can throw */
int flash_btree_bt_insert(flash_btree_t *btr, node_cache_t* cache, index_key_t key, location_t* loc, subTreeInfo_t* info, vector<node_key_pair_t>* toInsertUp)
{

	node_t leafNodeFound;
	node_t location = flash_btree_bt_find(btr, cache, key, INVALID_KEY, info, &leafNodeFound, NULL, NULL, true, true, true);	
	if (location != INVALID_NODE){
		/* fprintf(stderr, "\nInserting a duplicate key into the node %d, rooted at %d", leafNodeFound, info->root); */
	}
	assert(leafNodeFound != INVALID_NODE);
	vector<node_key_pair_t> forMe;
	node_key_pair_t toInsert;
	toInsert.key = key;
	assert(sizeof(node_t) == sizeof(location_t));
	memcpy(&toInsert.nodeNumber, loc, sizeof(location_t));
	forMe.push_back(toInsert);
	node_t nodeToInsertTo = leafNodeFound;
	do{
		vector<node_key_pair_t> forParent;
		flash_btree_bt_insert_up(btr, cache, nodeToInsertTo, &forMe, &forParent);
		assert(forParent.size() >= 1);
		assert(forParent[0].nodeNumber == nodeToInsertTo);
		node_t newNodeToInsertTo = (nodeToInsertTo == info->root) ? INVALID_NODE : flash_btree_get_parent(btr, nodeToInsertTo);
		assert(newNodeToInsertTo != BAD_NODE);
		forMe = forParent;
		nodeToInsertTo = newNodeToInsertTo;
	}while(nodeToInsertTo != INVALID_NODE && forMe.size() > 1);
	int returnCode = STATUS_OK;
	if (forMe.size() > 1){
		assert(forMe.size() == 2);
		if (info->maxHeight < 0 || info->curHeight < info->maxHeight){
			assert(flash_btree_get_parent(btr, info->root) == INVALID_NODE);
			bplustreenode_t* s= NULL;
			node_t s_nbr;
			s = (bplustreenode_t*)node_cache_alloc_node(cache, &s_nbr, info->root);
			assert(s);
			initialize_pointers_in_node(btr, s, s_nbr);
			vector<node_t> nodesToDirty;
			nodesToDirty.push_back(s_nbr);
			node_cache_premark_dirty(cache, nodesToDirty);
			assert(s_nbr != INVALID_NODE);
			s->level = info->rootLevel + 1;
			assert(forMe.front().nodeNumber == info->root);
			info->root = s_nbr;
			flash_btree_set_parent(btr, s_nbr, INVALID_NODE);
			info->rootLevel = s->level;
			info->curHeight ++;
			node_cache_release_node(cache, s_nbr);
			vector<node_key_pair_t> forParentNew;
			forMe.back().key = LARGEST_KEY_IN_NODE;
			flash_btree_bt_insert_up(btr, cache, info->root, &forMe, &forParentNew);
			assert(forParentNew.size() == 1);
			assert(forParentNew[0].nodeNumber == info->root);
			forMe = forParentNew; /* For the callee */
			returnCode = NEW_ROOT_ERROR;
		}else{
			assert(info->maxHeight > 0 && info->curHeight == info->maxHeight);
			returnCode = SPLIT_TREE_ERROR;
		}
	}
	toInsertUp->clear();
	*toInsertUp = forMe;
	return returnCode;
}

int flash_btree_clear_parent_table(flash_btree_t* btr, node_t root){
	btr->parentTable->clear();	
	btr->parentTable->insert(make_pair(root, INVALID_NODE));
/*	fprintf(stderr, "\nForgot all parenthood info .. setting the root %d -> %d", root, INVALID_NODE); */
	return STATUS_OK;
}

node_t flash_btree_bt_find(flash_btree_t *btr, node_cache_t* cache, index_key_t key, index_key_t keyToUpdate, subTreeInfo_t* info, node_t* leafNodeFound, index_key_t* pivotWentTo, node_t* nodeToGoNext, bool doParenting, bool fromInsert, bool freeToLock){
	int i;
	int r;
	bplustreenode_t* xn;
	index_key_t k = key;
	node_t toreturn = INVALID_NODE;
	node_t temp;
	node_t x = info->root;
	assert(x!=INVALID_NODE);
	index_key_t constrainingKey;
	constrainingKey = LARGEST_KEY_IN_NODE;
	node_t prevNode = flash_btree_get_parent(btr, info->root);
	/* assert(prevNode != BAD_NODE); */
	if (pivotWentTo) *pivotWentTo = INVALID_KEY;
	int expectedLevel = info->rootLevel;
	while (1) {
		pagesReadForThisSearch++;
		xn = (bplustreenode_t*)node_cache_get_node(cache, x);
		assert(xn); /* We should find a free buffer for this, else something we are really constrained */
		assert(xn->level == expectedLevel);
		assert(expectedLevel >= 0);
		verify_node(btr, x, xn, &constrainingKey, prevNode);
		i = flash_btree_findkindex(btr, cache, xn, k, &r);
		if (isNodeLeaf(xn->level, info)){
			if (i <= xn->n - 1 && COMPARE_KEY(k, *KEYS(btr, xn, i)) ==0){
				assert(sizeof(*NODES(btr, xn, i)) == sizeof(location_t));
				toreturn = *NODES(btr, xn, i);
			}else toreturn = INVALID_NODE;
			if (pivotWentTo) *pivotWentTo = *KEYS(btr, xn, i);
			if (nodeToGoNext) *nodeToGoNext = *NODES(btr, xn, i);
			if (leafNodeFound) *leafNodeFound = x;
			node_cache_release_node(cache, x);
			break;
		}else{
			temp = *NODES(btr, xn, i);
			assert(xn->level > 0);
			if (i < xn->n) constrainingKey = *KEYS(btr, xn, i);
			node_cache_release_node(cache, x);
			prevNode = x;
			assert(node_cache_is_valid(cache, temp));
			x = temp;
		}
		expectedLevel --;
	}
	return toreturn;
}

void flash_btree_bt_range_query(flash_btree_t *btr, node_cache_t* cache, range_t* range, subTreeInfo_t* info, vector<flash_bt_data_t>& keys){
   int i;
   node_t leafNode = INVALID_NODE; 
   index_key_t pivotWentTo = INVALID_KEY;
   assert(COMPARE_KEY(range->first, LARGEST_KEY_IN_NODE) < 0);
   assert(COMPARE_KEY(range->first, range->second) <= 0);
   subTreeInfo_t ourInfo(*info);
   ourInfo.maxHeight = -1;
   numRangeQueries ++;
   int pagesReadTillNow = pagesReadForThisSearch;
   flash_btree_bt_find(btr, cache, range->first, INVALID_KEY, &ourInfo, &leafNode,  &pivotWentTo, NULL, true, false, false);
   pagesReadTillNow = pagesReadForThisSearch - pagesReadTillNow;
   assert(pagesReadTillNow >= 1); /* Number of pages read in the downward pass */
   pagesWentDownForRangeQueries += pagesReadTillNow;
   node_t cursor = leafNode;
   bplustreenode_t* xn = NULL;
   keys.clear();
   bool continueFurther = true;
   bool firstDone = false;
   while(cursor != INVALID_NODE && continueFurther){
        bool shouldNotGoAhead = false;
        node_t next = INVALID_NODE;
		xn = (bplustreenode_t*)node_cache_get_node(cache, cursor);	
		assert(xn);
        if (xn->n == 0){
            /* this is possibly a root node whose buffer hasn't been emptied yet */
            /* skip it for now */
            next = INVALID_NODE;
            goto goOn;
        }
		assert(xn->level == 0 && xn->n > 0);
        if (firstDone){
            pagesScannedForRangeQueries ++;
        }
        fprintf(stderr, "\nSearching for [%d, %d] among node %d (%d, %d)", range->first, range->second, cursor, *KEYS(btr, xn, 0), *KEYS(btr, xn, xn->n - 1));
		for(i = 0; i< xn->n; i++){
            assert(*KEYS(btr, xn, i) != LARGEST_KEY_IN_NODE);
            if (COMPARE_KEY(*KEYS(btr, xn, i), range->first) >= 0){
                if (COMPARE_KEY(*KEYS(btr, xn, i), range->second) <= 0){
                    keys.push_back(flash_bt_data_t(*KEYS(btr, xn, i), *NODES(btr, xn, i)));
                }else{
                    shouldNotGoAhead = true;
                    break;
                }
            }
		}
		next = (*btr->siblingTable)[cursor];
        if (COMPARE_KEY(*KEYS(btr, xn, xn->n - 1), range->second) <= 0) continueFurther = true;
        else{
            continueFurther = false;
            fprintf(stderr, "\nSearching for [%d, %d] discontinuing .. after %d", range->first, range->second, cursor);
        }
goOn:
        if (shouldNotGoAhead) assert(!continueFurther);
		node_cache_release_node(cache, cursor);
		cursor = next;
        firstDone = true;
   }
}

node_t flash_btree_first_leaf_node(flash_btree_t* btr, node_cache_t* cache, node_t root, bool cheatMode){
	node_t x = root;
	bplustreenode_t* xn = NULL;
	while(1){	
        assert(x != INVALID_NODE);
		if (!cheatMode) xn = (bplustreenode_t*)node_cache_get_node(cache, x);
		else xn = (bplustreenode_t*)node_cache_get_aux_node(cache, x);
		assert(xn->n >= 0);
        bool isLeaf = (xn->level == 0);
		if (isLeaf){
            if (!cheatMode) node_cache_release_node(cache, x);
            else node_cache_release_aux_node(cache, x, xn);
			break;
		}
		node_t temp = *NODES(btr, xn, 0);
		if (!cheatMode) node_cache_release_node(cache, x);
		else node_cache_release_aux_node(cache, x, xn);
		x= temp;
	}
	return x;
}



void flash_btree_for_each_leaf_entry_helper(flash_btree_t* btr, node_cache_t* cache, int curLevel, node_t node, leaf_iterator_func_t callback, void* state, bool cheatMode, bool doParenting, index_key_t nodePivot){
	bool cheating = (curLevel != 1 || cheatMode);
	bplustreenode_t* xn = NULL;
	if (cheating) xn = (bplustreenode_t*)node_cache_get_aux_node(cache, node);
	else xn = (bplustreenode_t*)node_cache_get_node(cache, node); 
	verify_node(btr, node, xn, NULL, flash_btree_get_parent(btr, node));
	vector<node_key_pair_t> children;
	if (xn->n > 0 && doParenting){
		for(int i = 0; i <= xn->n ; i++){
			flash_btree_set_parent(btr, *NODES(btr, xn, i), node);	
		}
	}
	if (curLevel == 1){
		int n = xn->n;
		if (xn->level > 0) n++;
		for(int i = 0; i<n ; i++){
			callback(state, *KEYS(btr, xn, i), *NODES(btr, xn, i), i==xn->n ? nodePivot: *KEYS(btr, xn, i));
		}
	}else{
		for(int i = 0; i<= xn->n; i++){
			children.push_back(node_key_pair_t(i == xn->n ? nodePivot : *KEYS(btr, xn, i) , *NODES(btr, xn, i)));
		}
	}
	if (cheating) node_cache_release_aux_node(cache, node, xn);
	else node_cache_release_node(cache, node);
	xn = NULL;
	for(int i = 0; i< int(children.size()); i++){
		flash_btree_for_each_leaf_entry_helper(btr, cache, curLevel - 1, children[i].nodeNumber, callback, state, cheatMode, doParenting, children[i].key);
	}
}

void flash_btree_for_each_leaf_entry(flash_btree_t* btr, node_cache_t* cache, node_t root, int curHeight, leaf_iterator_func_t callback, void* state, bool cheatMode, bool doParenting){

	/* We will do a dfs on the tree in order .. accounting in only the This
	 * is an okay thing to do .. as we could always sweat it out to
	 * construct a leaf chain (a bit complicated) and pay the same cost as
	 * we are paying now */

	/* If cheatMode is set .. we also ignore the cost of leaf node read ..
	 * making this a free operation */

	flash_btree_for_each_leaf_entry_helper(btr, cache, curHeight, root, callback, state, cheatMode, doParenting, LARGEST_KEY_IN_NODE);
}

void flash_btree_verify_subtree(flash_btree_t* btr, node_cache_t* cache, node_t root, index_key_t* constrainingKey, subTreeInfo_t* info, node_t parent, int level, int heightToGoDown){
#if 0 /* Correct code below, just enable it in case of problems, else it slows things up */
	bplustreenode_t* xn = (bplustreenode_t*)(node_cache_get_aux_node(cache, root));
	assert(xn->level == level);
	verify_node(btr, root, xn, constrainingKey, parent);
	if (heightToGoDown >= 0) assert(heightToGoDown >= 1);
	bool isLeaf = (heightToGoDown < 0) ? isNodeLeaf(xn->level, info) : (heightToGoDown == 1);
	if (!isLeaf){
		vector<node_t> nodes;
		vector<index_key_t> keys;
		int i;
		for(i = 0; i<= xn->n; i++){
			nodes.push_back(*NODES(btr, xn, i));
			if (i<xn->n) keys.push_back(*KEYS(btr, xn, i));
		}
		node_cache_release_aux_node(cache, root, xn);
		for(i = 0; i< int(nodes.size()); i++){
			node_t t = nodes[i];
			assert(t!= INVALID_NODE);
			index_key_t newConstrainingKey;
			newConstrainingKey = (i < int(keys.size()) ) ? keys[i] : LARGEST_KEY_IN_NODE;
			flash_btree_verify_subtree(btr, cache, nodes[i], &newConstrainingKey, info, root, level - 1, heightToGoDown - 1);
		}
		return;
	}
	node_cache_release_aux_node(cache, root, xn); /* Only if the above was a leaf */
#endif
}

void verify_leaf_pointer_chain(flash_btree_t* btr, node_cache_t* cache, node_t root){
#if 0
	node_t firstLeafNode = flash_btree_first_leaf_node(btr, cache, root, true);	
	assert(firstLeafNode != INVALID_NODE);
	node_t cursor = firstLeafNode;
	bplustreenode_t* xn = NULL;
	bool noMaxYet = true;
	index_key_t Max;
	int i;
	while(cursor!= INVALID_NODE){
		xn = (bplustreenode_t*)node_cache_get_aux_node(cache, cursor);	
		assert(xn);
		assert((xn->level == 0) && (xn->n >= 0));
		for(i = 0; i< xn->n; i++){
			if (!noMaxYet){
				assert(COMPARE_KEY(Max, *KEYS(btr, xn, i))<=0);	
			}else noMaxYet = false;
			Max = *KEYS(btr, xn, i);
		}
		node_t next = (*btr->siblingTable)[cursor];
		node_cache_release_aux_node(cache, cursor, xn);
		cursor = next;
	}
#endif
}

void flash_btree_verify(flash_btree_t* btr, node_cache_t* cache, subTreeInfo_t* info, int heightToGoDown){
	assert(info->root != INVALID_NODE);
	flash_btree_verify_subtree(btr, cache, info->root, NULL, info, INVALID_NODE, info->rootLevel, heightToGoDown);
    verify_leaf_pointer_chain(btr, cache, info->root);
}

node_key_pair_t getRealPair(flash_btree_t* btr, bplustreenode_t* curNode, int i, vector<node_key_pair_t>* extra, int idxToReplace, int* isReplaceIdx = NULL){
	assert(extra && (idxToReplace < 0 || idxToReplace <= curNode->n));
	assert(i < curNode->n + int(extra->size()) + ((curNode->level > 0 && curNode->n > 0) ? 1 : 0));
	int extraSize = extra->size();
	assert(extraSize > 1);
	node_key_pair_t pair;
	if (idxToReplace < 0){
		assert(curNode->n == 0);
		assert(curNode->level > 0);
		assert(i < int(extra->size() ) );
		pair = extra->at(i);
	}else{
		if (i <= idxToReplace - 1) pair = *PAIR(btr, curNode, i);
		else if (i>=idxToReplace && i<=idxToReplace + extraSize - 1){
			if (isReplaceIdx) *isReplaceIdx = (i - idxToReplace);
			pair = extra->at(i - idxToReplace);
		}else{
			/* i is at max nKeys of new node = nKeysOld + extraSize - 1 (as we are replacing the idxToReplace, not adding an extra one) */
			assert(i <= curNode->n + extraSize - 1);
			pair = *PAIR(btr, curNode, i - extraSize + 1);
		}
	}
	return pair;
}

int addMultipleKeysNonFull(flash_btree_t* btr, node_t nodeNumber, bplustreenode_t* curNode, int idx, vector<node_key_pair_t>* replaceWith){
	assert(replaceWith);
	int extraSize = replaceWith->size();
	assert(extraSize > 1);
	int nKeysOld = curNode->n;
	int i;
	if (idx < 0 || idx == nKeysOld){
		assert(replaceWith->back().key == LARGEST_KEY_IN_NODE);
	}

	if (idx >= 0 && idx + 1 <= nKeysOld){
		/* Move all pairs from [idx +1, nKeysOld] */
		/* Pairs to move : nKeysOld - (idx + 1) + 1 */
		memmove(PAIR(btr, curNode, idx + 1 + extraSize - 1), PAIR(btr, curNode, idx + 1), sizeof(node_key_pair_t)*(nKeysOld - idx));
	}
	for(i=0;i<extraSize;i++){
		node_key_pair_t pair = replaceWith->at(i);
		int copyToIndex = (idx >= 0) ? i+idx : i;
		memcpy(PAIR(btr, curNode, copyToIndex), &pair, sizeof(node_key_pair_t));
		/* Assumption: When nodes are created they themselves set their pivot keys */
		assert(pair.key != INVALID_KEY);
		if (curNode->level > 0){
			flash_btree_set_parent(btr, pair.nodeNumber, nodeNumber);
		}
	}
	/* npairs in new node = nKeysOld + 1 + extraSize - 1 ; number of keys in new is 1 less of this */
	curNode->n = nKeysOld + extraSize - 1;
	assert(curNode->n <= btr->maxKeys);
	return STATUS_OK;
}

int addMultipleKeys(flash_btree_t *btr, node_cache_t* cache, node_t node, int idx, vector<node_key_pair_t>* replaceWith, vector<node_key_pair_t>* forParent){
	assert(replaceWith);
	int extraSize = replaceWith->size();
	assert(extraSize > 1);
	bplustreenode_t* curNode = NULL;
	curNode = (bplustreenode_t*)node_cache_get_node(cache, node);
	vector<node_t> nodesToDirty;
	nodesToDirty.push_back(node);
	node_cache_premark_dirty(cache, nodesToDirty);
	int nKeysOld = curNode->n;
	int totalPairs = nKeysOld + extraSize; /* nKeysOld + 1 + extraSize - 1 */
	int level = curNode->level;
	if (level == 0){
		assert(extraSize == 2);
		totalPairs = nKeysOld + 1;
	}
	int pairsMax = (level == 0 ? btr->maxKeys : btr->maxKeys + 1);
	if (forParent) forParent->clear();
	if (totalPairs <= pairsMax){
		addMultipleKeysNonFull(btr, node, curNode, idx, replaceWith);
		forParent->push_back(node_key_pair_t(INVALID_KEY, node));
	}else{
		assert(forParent);
		node_t newNode;
		int pairStart = 0;
		int pairsToCopy = flash_btree_min_pairs(btr); 
		if (level == 0) pairsToCopy --;
		assert(pairsToCopy <= totalPairs);
		node_key_pair_t pairToInsertUp;
		bplustreenode_t* src = (bplustreenode_t*)node_cache_get_aux_node(cache, node);
        node_t oldSiblingNode = (*btr->siblingTable)[node];
		memcpy(src, curNode, cache->nodeSize);
		int nTimesAddedSplitKey = 0;
		while (pairStart <= totalPairs - 1){
			int thisShot = pairsToCopy;
			/* Check if we should consolidate now to make sure that the last thing doesn't have < minNumberKeys in it */
			int start = pairStart;
			int end = pairStart + thisShot - 1;
			if (end > totalPairs - 1) end = totalPairs - 1;
			if ((end < totalPairs - 1) && (totalPairs - end - 1 < pairsToCopy) && (totalPairs - start <= pairsMax)){
				/* fprintf(stderr, "\nGobbling till the end"); */
				end = totalPairs - 1;
			}
			assert(end - start + 1 <= pairsMax);
			/* fprintf(stderr, "\nIn this split : (%d, %d)", start, end); */
			pairToInsertUp.key = getRealPair(btr, src, end, replaceWith, idx).key;
			/* Get a new node and shove in the pairs from [start,end] to it */
			int i;
			bplustreenode_t* destNode = NULL;
			if (pairStart == 0){
				newNode = node;
				destNode = curNode;
			}else{
                bool nodeToMarkDirty = true;
                extern int MAX_SUBTREE_HEIGHT; /* Special case when buffers are attached at each level */
                if (MAX_SUBTREE_HEIGHT == 1){
                    destNode = (bplustreenode_t*)node_cache_alloc_aux_node(cache, &newNode);
                    nodeToMarkDirty = false;
                }else{
                    destNode = (bplustreenode_t*)node_cache_alloc_node(cache, &newNode, node);
                    numNodesSplit ++;
                    int numNodesTillNow = ftl_nItems(&cache->ftl);
                    extern int nKeysInserted;
                    if ((numNodesTillNow % 1000) == 0){
                        fprintf(stdout, "\nnewNode %d %d", nKeysInserted, numNodesTillNow);
                    }
                }

                if (nodeToMarkDirty){
                    vector<node_t> nodesToDirty;
                    nodesToDirty.push_back(newNode);
                    node_cache_premark_dirty(cache, nodesToDirty);
                }
			}

			initialize_pointers_in_node(btr, destNode, newNode); /* Init both the initial node and the real new nodes */
			destNode->level = level;
			int nPairsInserted = 0;
			int nPairsPrior = 0;
			vector<node_key_pair_t> newPairs;
			for(i = start; i <= end; i++){
				int isThisAReplacement = -1;
				node_key_pair_t	copyThis = getRealPair(btr, src, i, replaceWith, idx, &isThisAReplacement);	
				if (isThisAReplacement != 0){
					nPairsPrior ++;
					if (newNode != node) newPairs.push_back(copyThis);
				}else if (isThisAReplacement == 0){
					nTimesAddedSplitKey ++;
				}
				assert(copyThis.nodeNumber >= 0);
				if (level > 0){
					flash_btree_set_parent(btr, copyThis.nodeNumber, newNode);
					if (i == end) copyThis.key = LARGEST_KEY_IN_NODE;
				}
				assert(copyThis.key != INVALID_KEY); 
				assert(copyThis.nodeNumber != INVALID_NODE);
				*PAIR(btr, destNode, nPairsInserted) = copyThis;
				nPairsInserted++;
			}
			assert(nPairsInserted == end - start + 1);
			int lastIndex = nPairsInserted - 1;
			assert(*NODES(btr, destNode, lastIndex) >= 0);
			if (level == 0){
				/* Leaf level */
				assert(*KEYS(btr, destNode, lastIndex) >= 0);
				/* This should ideally point to the next leaf guy in the chain */
				*PAIR(btr, destNode, nPairsInserted) = node_key_pair_t(LARGEST_KEY_IN_NODE, INVALID_NODE);
				destNode->n = nPairsInserted;
			}else{
				assert(*KEYS(btr, destNode, lastIndex) == LARGEST_KEY_IN_NODE);
				destNode->n = nPairsInserted - 1;
			}
			assert(destNode->level == level);
			assert(destNode->n <= btr->maxKeys);
			pairToInsertUp.nodeNumber = newNode;
			forParent->push_back(pairToInsertUp);
			if (pairStart > 0){
				extern int MAX_SUBTREE_HEIGHT;
				if (MAX_SUBTREE_HEIGHT == 1){
					node_cache_write_release_aux_node(cache, newNode, destNode);
				}else{
					assert(newNode != node);
					node_cache_release_node(cache, newNode);
				}
			}
			pairStart = end + 1;
		}
		assert(forParent->size() > 1);
        for(vector<node_key_pair_t>::iterator i = forParent->begin() + 1; i!= forParent->end(); i++){
            node_t left = (i - 1)->nodeNumber;
            node_t right = (i)->nodeNumber;
            /* fprintf(stderr, "\nLinking node %d to %d", left, right); */
            (*btr->siblingTable)[left] = right;
        }
        (*btr->siblingTable)[(forParent->end() - 1)->nodeNumber] = oldSiblingNode;
        /* fprintf(stderr, "\nLinking node %d to %d", (forParent->end() - 1)->nodeNumber, oldSiblingNode); */
		node_cache_release_aux_node(cache, node, src);
	}
	node_cache_release_node(cache, node);
	return STATUS_OK;
}

/* Not maintaining the leaf chain */
int flash_btree_bt_insert_up(flash_btree_t* btr, node_cache_t* cache, node_t nodeToInsertTo, vector<node_key_pair_t>* forMe, vector<node_key_pair_t>* forParent){
	vector<node_key_pair_t> toInsert = *forMe;
	bplustreenode_t* curNode = (bplustreenode_t*)node_cache_get_node(cache, nodeToInsertTo);
	int idx = -1;
	assert(toInsert.size() >= 1);
	if (curNode->level == 0){
		assert(toInsert.size() == 1);
		idx = flash_btree_findkindex(btr, cache, curNode, toInsert[0].key, NULL);
		assert(idx <= curNode->n);
		node_key_pair_t nextGuy = (idx < curNode->n) ? *PAIR(btr, curNode, idx) : node_key_pair_t(LARGEST_KEY_IN_NODE, INVALID_NODE);
		toInsert.push_back(nextGuy);
	}else{
		assert(curNode->level > 0);
		assert(toInsert.size() > 1);
		idx = (curNode->n == 0) ? -1 : flash_btree_find_node_index(btr, cache, curNode, toInsert[0].nodeNumber);
		assert(idx < 0 || idx <= curNode->n);
		toInsert.back().key = (curNode->n == 0) ? LARGEST_KEY_IN_NODE : *KEYS(btr, curNode, idx);
		if (idx >= 0 && idx == curNode->n) assert(toInsert.back().key == LARGEST_KEY_IN_NODE);
		int i = 0;
		for(i = 0;i < int(toInsert.size()); i++){
			node_t n = toInsert[i].nodeNumber;
			if (n < 0) continue;
			bplustreenode_t* z = (bplustreenode_t*)node_cache_get_aux_node(cache, n);
			assert(z);
			assert(z->level == curNode->level - 1);
			assert(z->level >= 0);
			node_cache_release_aux_node(cache, n, z);
		}
	}
	node_cache_release_node(cache, nodeToInsertTo);
	return addMultipleKeys(btr, cache, nodeToInsertTo, idx, &toInsert, forParent);
}

int flash_btree_find_buffer_nodes(flash_btree_t* btr, node_cache_t* cache, tree_node_callback_t func, node_t root, void* state, int maxHeight){
	bplustreenode_t* curNode = (bplustreenode_t*)(node_cache_get_node(cache, root));
	assert(curNode);
	assert(curNode->n > 0);
	int level = curNode->level;
	if ((level + 1) % maxHeight == 0) func(state, root, level, maxHeight);	
	bool isLeaf = (level == 0);
	if (!isLeaf){
		vector<node_t> nodes;
		int i;
		for(i = 0; i<= curNode->n; i++){
			nodes.push_back(*NODES(btr, curNode, i));
		}
		node_cache_release_node(cache, root);
		curNode = NULL;
		for(i = 0; i< int(nodes.size()); i++){
			node_t t = nodes[i];
			assert(t!= INVALID_NODE);
			int ret = flash_btree_find_buffer_nodes(btr, cache, func, t, state, maxHeight);
			assert(ret == STATUS_OK);
		}
	}
	if (curNode) node_cache_release_node(cache, root);
	return STATUS_OK;
}
