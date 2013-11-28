#include <ostream>
#include <iostream>
#include <subtree_tree.h>
#include <my_debug.h>

using namespace std;

int MAX_DELTAS_ALLOWED = -1;
int MAX_SUBTREE_HEIGHT = -1;
int deltasRemaining = 0; /* Difference b/w entries inserted into the root buffer - entries inserted into tree */
extern int numberOfCachedNodes;
/* Which costs do we take into account for buffer scan + query ? */
cachingOrNoneCost_t PREV_COST_QUERY = COST_WITHOUT_CACHING, BUFFER_EMPTY_COST_ESTIMATE_QUERY = COST_WITHOUT_CACHING;
int numPrematureEmpties = 0;
double totalBufferScanCost = 0;
int numBufferScans = 0;
int sumNEntriesWhenFlushing = 0;
int keysActuallyInserted = 0;
int _numBufferEmpties = 0;
int _nonFlushNumBufferEmpties = 0;
int _numSplitUps = 0;

subtree_tree_t* theGlobalTree = NULL;

int subtreeDetails_t::numberOfSubtrees = 0;

subtreeNumberMap_t* subtreeDetails_t::subtreeNumberMap = NULL;

ostream& operator<<(ostream& os, const pastQueryInfo_t& s){	
	os << "[ " << "queryId: " << s.queryId << ", numAccumulated: " << s.numAccumulated << ", savingsSoFar: " << s.savingsSoFar << ", bufferEmptyCost: " << s.bufferEmptyCost << ", queryCost: " << s.queryCost << "]";
	return os;
}

bufferInfo_t* subtree_tree_t::getBufferInfo(int bufferNumber){
	assert(bufferNumber >= 0);
	if (bufferMap.find(bufferNumber) == bufferMap.end()){
		bufferInfo_t* info = new bufferInfo_t;
		tree_buffer_info_init(info);
		assert(MAX_DELTAS_ALLOWED > 0);
		info->maxEntries = MAX_DELTAS_ALLOWED;
		bufferMap.insert(make_pair(bufferNumber, info));
		assert(bufferMap.find(bufferNumber)->second == info);
	}
	bufferInfo_t* t = bufferMap.find(bufferNumber)->second;
	assert(t);
	return t;
}

void subtree_tree_t::destroyBufferMap(){	
	for(bufferMap_t::iterator i = bufferMap.begin(); i != bufferMap.end(); i++){
		assert(i->second);
		delete i->second;
	}
	bufferMap.clear();
}

void subtree_tree_t::queryHit(node_t x){
	subtreeDetails_t& details = getDetails(x);
	/* Do fresh query state changes .. to track a fresh query */
	if (details.freshQueryState == QUERY_QUERY || details.freshQueryState == INSERT_QUERY){
		details.freshQueryState = QUERY_QUERY;	
	}else{
		details.freshQueryState = INSERT_QUERY;
	}
}


void subtree_tree_t::deltaAdded(node_t x){
	subtreeDetails_t& details = getDetails(x);
	/* Do fresh query state changes .. to track a fresh query */
	if (details.freshQueryState == QUERY_QUERY || details.freshQueryState == INSERT_QUERY){
		details.freshQueryState = QUERY_INSERT;	
	}else{
		details.freshQueryState = INSERT_INSERT;
	}
}

subtreeDetails_t& subtree_tree_t::getDetails(node_t node){
	assert(treeTable.find(node) != treeTable.end());
	return treeTable.find(node)->second;
}

void subtree_tree_t::setDetails(node_t node, subtreeDetails_t info, bool newFlag){
	assert(newFlag == (treeTable.find(node) == treeTable.end()));
	treeTable.erase(node);
	treeTable.insert(make_pair(node, info));
}

bool subtree_tree_t::isBufferNode(node_t node){
	if (treeTable.find(node) != treeTable.end()){
		subtreeDetails_t& details = getDetails(node);
		assert((details.treeDetails.rootLevel + 1) % MAX_SUBTREE_HEIGHT == 0 || node == rootNode); /* Check if it has a correct height */
		return true; /* As we found it in our table */
	}else return false;
}

int subtree_tree_t::subtree_get_buffer(node_t subtreeRoot, tree_buffer_t* buffer){
	assert(buffer); 
	tree_buffer_t newbuffer;
	cached_buffer_t cachedBuffer; 
	DEBUG_FILE_PRINTF(stderr, "\nGetting buffer for subtree root %d", subtreeRoot);
	subtreeDetails_t& details = getDetails(subtreeRoot);
	assert(details.subtreeNumber >= 0);
	if (details.bufferNumber < 0){
		details.bufferNumber = numBuffers++;
		assert(bufferToSubtreeNumberMap.find(details.bufferNumber) == bufferToSubtreeNumberMap.end());
		bufferToSubtreeNumberMap.insert(make_pair(details.bufferNumber, details.subtreeNumber));
		tree_buffer_info_init(getBufferInfo(details.bufferNumber));
	}
	if (cachedBuffers.find(details.bufferNumber) != cachedBuffers.end()){
		cachedBuffer = cachedBuffers[details.bufferNumber];
		assert(cachedBuffer.refcount > 0);
		cachedBuffer.refcount++;
		newbuffer = cachedBuffer.buffer;
	}else{
		newbuffer = tree_buffer_alloc();
		assert(newbuffer);
		tree_buffer_load(&newbuffer, &details, details.bufferNumber, getBufferInfo(details.bufferNumber));
		cachedBuffer.refcount = 1;
		cachedBuffer.buffer = newbuffer;
	}
	cachedBuffers.erase(details.bufferNumber);
	cachedBuffers.insert(make_pair(details.bufferNumber, cachedBuffer));
	*buffer = newbuffer;
	return STATUS_OK;
}

int subtree_tree_t::subtree_release_buffer(node_t subtreeRoot, tree_buffer_t* buffer){
	DEBUG_FILE_PRINTF(stderr, "\nReleasing buffer for subtree root %d", subtreeRoot);
	subtreeDetails_t& details = getDetails(subtreeRoot);
	assert(details.bufferNumber >= 0);
	assert(cachedBuffers.find(details.bufferNumber) != cachedBuffers.end());
	cached_buffer_t& cachedBuffer = cachedBuffers[details.bufferNumber];
	if (buffer) assert(cachedBuffer.buffer == *buffer);
	else buffer = &cachedBuffer.buffer;
	cachedBuffer.refcount --;
	assert(cachedBuffers[details.bufferNumber].refcount == cachedBuffer.refcount);
	if (cachedBuffer.refcount == 0){
		cachedBuffers.erase(details.bufferNumber);
		tree_buffer_node_destroy(buffer);
	}
	*buffer = NULL;
	return STATUS_OK;
}

int getHeighestPossibleFanout(int& reqdHeight, int maxPossibleFanout, int maxNumNodes){
	int newActualNumNodesPerTree;
	int maxFanout = -1;
	int minFanout = MIN_FANOUT;
	do{
		DEBUG_FILE_PRINTF(stderr, "\nTrying with a height of %d, maxNumNodes = %d, maxPossibleFanout = %d", reqdHeight, maxNumNodes, maxPossibleFanout);
		maxFanout = maxPossibleFanout;
		while(maxFanout >= minFanout){
			newActualNumNodesPerTree = (((int)(pow(maxFanout, reqdHeight)) - 1)/(maxFanout - 1));
			if (newActualNumNodesPerTree <= maxNumNodes) break;
			maxFanout --;
		}
		if (maxFanout == minFanout && newActualNumNodesPerTree > maxNumNodes) reqdHeight --; /* try again with lower height */
		else newActualNumNodesPerTree = (((int)(pow(maxFanout, reqdHeight)) - 1)/(maxFanout - 1));
	}while(newActualNumNodesPerTree > maxNumNodes && reqdHeight >= 1);
	return maxFanout;
}

void subtree_tree_t::init(int smallNodeSize, int fanout, int height, int numNodesCached, int bufferSize){
	int actualNodeSize = treeFunctions.get_size_for_fanout(actualTree, fanout);
	assert(fanout >= MIN_FANOUT);
	assert(actualNodeSize <= smallNodeSize);
	MAX_SUBTREE_HEIGHT = height;
	actualFanout = fanout;
	MAX_DELTAS_ALLOWED = tree_buffer_get_max_deltas(bufferSize);
	int realMemoryLeft = (smallNodeSize - actualNodeSize)*numNodesCached;
	assert(realMemoryLeft >= 0);
	int extraHangingDelta = realMemoryLeft/sizeof(tree_delta_t);
    REALLY_MAX_HANGING_DELTAS_ALLOWED = extraHangingDelta + MAX_DELTAS_ALLOWED;
    /* fakeNodesCached is the number of fictious nodes cached, which account for the memory used by the buffer segments */
    int fakeNodesCached = 1 + ROUND_UP_NUM_BUFFERS((REALLY_MAX_HANGING_DELTAS_ALLOWED * sizeof(tree_delta_t)), actualNodeSize);
	int ret = treeFunctions.init_index(actualTree, fanout, actualNodeSize, numNodesCached + fakeNodesCached);
	assert(ret == STATUS_OK);
    nodeBufMgr = b_tree_get_buffer_manager(actualTree);
    assert(nodeBufMgr);
	btree_index_t* thing =  (btree_index_t*)actualTree;
    assert(&thing->cache.m);
	ret = tree_buffer_init(thing->cache.m);
    assert(ret == STATUS_OK);
	new_root_subtree();
	assert(rootNode != INVALID_NODE);
	open_root_buffer();
}

void subtree_tree_t::release_root_subtree(){
	treeFunctions.close_tree(actualTree, &(getRootDetails().treeDetails));	
}

node_t subtree_tree_t::new_root_subtree(){
	int newRootLevel = 0;
	int newSubtreeLevel = 0;
	if (rootNode != INVALID_NODE){
		subtreeDetails_t& details = getDetails(rootNode);
		newSubtreeLevel = details.subtreeLevel + 1;
		newRootLevel = details.treeDetails.rootLevel + 1;
	}else DEBUG_FILE_PRINTF(stderr, "\nCreating a brand new root subtree");
	DEBUG_FILE_PRINTF(stderr, "Created a new subtree with subtree level %d and real level %d", newSubtreeLevel, newRootLevel);
	assert(newRootLevel % MAX_SUBTREE_HEIGHT == 0); /* Only create at integral multiples of the subtree height */
	subtreeDetails_t info(newSubtreeLevel, MAX_SUBTREE_HEIGHT);
	node_t node = treeFunctions.new_node(actualTree, &(info.treeDetails), newRootLevel, 1);
	assert(node!= INVALID_NODE);
	DEBUG_FILE_PRINTF(stderr, "\nCreated a new root %d", node);
	setDetails(node, info, true);
	if (rootNode != INVALID_NODE){
		close_root_buffer();
	}
	rootNode = node;
	open_root_buffer();
	return rootNode;
}

void subtree_tree_t::handleSubtreeSplit(node_t node, vector<node_key_pair_t>* splitInto){
	assert(splitInto->size() > 1);
	assert(splitInto->front().nodeNumber == node);
	/* Create new subtree info's for the sibling subtrees */	
	int i;
	subtreeDetails_t& details = getDetails(node);
	DEBUG_FILE_PRINTF(stderr, "\nSubtree at %d splits ... printing self:", node);
	for(i = 1; i < int(splitInto->size()); i++){
		node_t new_node = splitInto->at(i).nodeNumber;
		subtreeDetails_t newNodeDetails(details, new_node);
		DEBUG_FILE_PRINTF(stderr, "\nSibling %d (%d of %d) of node %d", new_node, i, splitInto->size() - 1, node);
		setDetails(new_node, newNodeDetails, true);
		treeFunctions.release_tree(actualTree, new_node, newNodeDetails.treeDetails.curHeight, newNodeDetails.treeDetails.rootLevel); /* In cheat mode */
	}
	if (!isBufferEmpty(node)){
		DEBUG_FILE_PRINTF(stderr,"\nWe had stuff in this buffer so splitting it");
		split_buffer(node, splitInto);		
	}
}

double subtree_tree_t::getNodeReadCost(){
	int fanout = treeFunctions.get_fanout(actualTree, -1);
	int nodeSize = treeFunctions.get_size_for_fanout(actualTree, fanout);
	return calculateFlashCost(OP_READ, nodeSize, NULL, NULL);
}

double subtree_tree_t::getEntireSubtreeCost(int oper, subtreeDetails_t& details){
	int fanout = treeFunctions.get_fanout(actualTree, -1);
	int nodeSize = treeFunctions.get_size_for_fanout(actualTree, fanout);
	int actualNumNodesPerTree = (((int)(pow(fanout, MAX_SUBTREE_HEIGHT)) - 1)/(fanout - 1));
	int numberOfNodesToCount = 0;
	int numberOfNodesToStartWith = (int)ceil(numberOfLeafNodes(details));
	int height = details.treeDetails.curHeight;
	int curLevel = height;
	while(curLevel >= 1){
		int nodesAtThisLevel = (int)(pow(fanout, curLevel - 1));
		if (numberOfNodesToStartWith > nodesAtThisLevel) numberOfNodesToStartWith = nodesAtThisLevel;
		numberOfNodesToCount += numberOfNodesToStartWith;
		numberOfNodesToStartWith /= fanout;
		if (numberOfNodesToStartWith <= 0) numberOfNodesToStartWith = 1;
		curLevel --;
	}
	actualNumNodesPerTree = numberOfNodesToCount;
	double cost = 0;
	if (oper == OP_READ){
		/* nodes are read non contiguosly */
		cost = actualNumNodesPerTree * calculateFlashCost(OP_READ, nodeSize, NULL, NULL);
	}else {
		assert(oper == OP_WRITE);
		int byteSize = actualNumNodesPerTree * nodeSize;
		if (doWeWriteAtSamePlace){
			/* Assuming nodes are scattered -- one over each flash page */
			cost = byteSize * FLASH_RANDOM_WRITE_VAR_COST + actualNumNodesPerTree * FLASH_RANDOM_WRITE_FIXED_COST;
		}else{
			cost = (((double)byteSize)/NAND_PAGE_SIZE) * calculateFlashCost(OP_WRITE, NAND_PAGE_SIZE, NULL, NULL);
		}
	}
	if (actualNumNodesPerTree > 0) assert(cost > 0);
	return cost;
}

void subtree_tree_t::getSubtreeCosts(node_t x, double& nodeReadCost, double& nodeWriteCost){ 
	subtreeDetails_t& details = getDetails(x);
	double fudgeFactor = 0.5;
	nodeReadCost = getEntireSubtreeCost(OP_READ, details) * fudgeFactor; /* Make it easier to empty */
	nodeWriteCost = getEntireSubtreeCost(OP_WRITE, details) * fudgeFactor;
}

int subtree_tree_t::numberOfLeafNodes(subtreeDetails_t& details){
	int curHeight = details.treeDetails.curHeight;
	assert(curHeight >= 1);
	return (int)(pow(actualFanout, curHeight - 1));
}

bool subtree_tree_t::adjustOldQueryCosts(node_t x){
	subtreeDetails_t& details = getDetails(x);
	if (details.oldQueries.empty()) return false; /* Need to have seen atleast one old query to take a late decision */
	int emptyingQueryIdx = -1;
	for(int i = 0; i < int(details.oldQueries.size()); i++){
		pastQueryInfo_t& query = details.oldQueries[i];
		query.savingsSoFar += query.queryCost;	
		query.numAccumulated ++;
		if (query.savingsSoFar > query.bufferEmptyCost && emptyingQueryIdx < 0){
			emptyingQueryIdx = i;
		}
	}
	if (emptyingQueryIdx >= 0){
		DEBUG_FILE_PRINTF(stderr, "\nIn adjustOldQuery costs for node %d, subtreeNumber: %d, Will empty for queryId: %d, currentQueryId: %d", x, details.subtreeNumber, details.oldQueries[emptyingQueryIdx].queryId, details.numberOfQueryScans);
#ifndef NO_PRINTF
		cerr << details.oldQueries[emptyingQueryIdx] << endl;
#endif
		return true;	
	}else return false;
}

/* shouldEmpty should only be called from the context of answering a query at a
 * subtree */
bool subtree_tree_t::shouldEmpty(node_t x){
    assert(!isBufferEmpty(x));
	subtreeDetails_t& details = getDetails(x);
	int numberDeltasThisTime = (getBufferInfo(details.bufferNumber))->numberEntries;
	assert(numberDeltasThisTime > 0);
    if (numberDeltasThisTime >= MAX_DELTAS_ALLOWED) return true;

	tree_buffer_t src_buffer;
	subtree_get_buffer(x, &src_buffer);
	double bufferCostWithoutCaching, actualBufferCost;
	tree_buffer_read_cost(&src_buffer, &bufferCostWithoutCaching, &actualBufferCost, false, &getDetails(x));
	double nodeReadCost, nodeWriteCost;
	getSubtreeCosts(x, nodeReadCost, nodeWriteCost);
	double bufferWriteCost = tree_buffer_write_cost(&src_buffer);
	double bufferReadCostWhileEmptying = (BUFFER_EMPTY_COST_ESTIMATE_QUERY == COST_WITHOUT_CACHING) ? bufferCostWithoutCaching : actualBufferCost;
	double costThisBufferEmpty = bufferReadCostWhileEmptying + nodeReadCost + (details.subtreeLevel == 0 ? nodeWriteCost : bufferWriteCost);
	subtree_release_buffer(x, &src_buffer);
	double currentQueryCost = ((PREV_COST_QUERY == COST_WITHOUT_CACHING) ? bufferCostWithoutCaching : actualBufferCost );


	bool freshQuery = (details.freshQueryState == INSERT_QUERY);
	bool willEmpty = adjustOldQueryCosts(x);
	if (!willEmpty && freshQuery){
        details.oldQueries.push_back(pastQueryInfo_t(details.numberOfQueryScans, currentQueryCost, costThisBufferEmpty));
	}
	return willEmpty;
}

void subtree_tree_t::checkForInterestingNodeSplit(vector<node_key_pair_t>* splitVector, node_t x){
	assert(splitVector && splitVector->size() > 1);
	assert(splitVector->front().nodeNumber == x);
	if (x == interestingNode){
		assert(interestingNodeSplitVector.empty());
		DEBUG_FILE_PRINTF(stderr, "\nThe interesting node %d split", x);
		interestingNodeSplitVector = *splitVector;
	}
}

void subtree_tree_t::handle_leaf_subtree_split_up(vector<node_key_pair_t>* forParent, node_t leafSubtreeRoot){
	node_t toInsertTo = treeFunctions.get_parent(actualTree, leafSubtreeRoot);	
	assert(toInsertTo != BAD_NODE);
	vector<node_key_pair_t> toInsert = *forParent;
	vector<node_key_pair_t> forUp;
	while(toInsertTo != INVALID_NODE){
		/* The split up is done in without lock mode ... no need for the nodes to hang in there */
		treeFunctions.handle_split_up(actualTree, toInsertTo, &toInsert, &forUp);
		toInsert = forUp;
		forUp.clear();
		if (toInsert.size() <= 1){
			/* It was absorbed by this node without splitting */
			break;
		}else{ /* We split .. check if we are the root of a subtree */
			checkForInterestingNodeSplit(&toInsert, toInsertTo); 
			if (isBufferNode(toInsertTo) && (toInsertTo != rootNode || getRootInfo()->curHeight == getRootInfo()->maxHeight)){
				handleSubtreeSplit(toInsertTo, &toInsert);
			}
		}
		toInsertTo = treeFunctions.get_parent(actualTree, toInsertTo);	
		assert(toInsertTo != BAD_NODE);
		_numSplitUps += 1;
	}

	if (toInsert.size() > 1){
		assert(toInsertTo == INVALID_NODE);
		assert(toInsert.front().nodeNumber == rootNode);
		int additionalLevels = 0;
		while(toInsert.size() > 1){
			DEBUG_FILE_PRINTF(stderr, "\nRoot node (%d) split -- %d", rootNode, additionalLevels);
			forUp.clear();
			if (getRootInfo()->curHeight < getRootInfo()->maxHeight){
				/* Grow this root subtree by one */
				assert(getRootInfo()->curHeight >= 1);
				node_t new_root = treeFunctions.new_node(actualTree, getRootInfo(), getRootInfo()->rootLevel + 1, getRootInfo()->curHeight + 1);
				assert(new_root != INVALID_NODE);
				rootNodeChanged(new_root);
			}else{
				/* Must create a new subtree */
				new_root_subtree();
			}
			treeFunctions.handle_split_up(actualTree, rootNode, &toInsert, &forUp);
			if (forUp.size() > 1){
				assert(forUp.front().nodeNumber == rootNode);
				if (getRootInfo()->curHeight == getRootInfo()->maxHeight){
					handleSubtreeSplit(rootNode, &forUp);	
				}
			}
			toInsert = forUp;
			additionalLevels++;
			_numSplitUps += 1;
		}
		DEBUG_FILE_PRINTF(stderr, "\nAdded %d additional levels", additionalLevels);
	}
}

void subtree_tree_t::prepareNodeState(node_t node, int level, int maxHeight){
	assert(level >= 0);
	int height = level + 1;
	int subtreeLevel = ((height +  maxHeight - 1)/maxHeight) - 1;
	int subtreeHeight = height % maxHeight;
	if (subtreeHeight == 0) subtreeHeight = maxHeight;
	subtreeDetails_t info(subtreeLevel, maxHeight);
	info.treeDetails.curHeight = subtreeHeight;
	info.treeDetails.rootLevel = level;
	info.treeDetails.root = node;
	assert(info.treeDetails.maxHeight == maxHeight);
	DEBUG_FILE_PRINTF(stderr, "\nSet the node %d (level %d) to be a new subtree", node, level);
	setDetails(node, info, true);
}

void subtree_prepare_buffer_node(void* self, node_t node, int level, int maxHeight){
	assert((level + 1) % maxHeight == 0);
	subtree_tree_t* t = (subtree_tree_t*)self;
	t->prepareNodeState(node, level, maxHeight);
}

int subtree_tree_t::clearInteresting(){
	interestingKeyFound = false;
	interestingNodeSplitVector.clear();
	interestingNode = INVALID_NODE;
	interestingKeyLocation = INVALID_LOCATION;
	interestingKey = INVALID_KEY;
	return STATUS_OK;
}

void subtree_tree_t::change_buffer(int subtreeNumber, int newMax){
	subtreeDetails_t* details = getDetailsByNumber(subtreeNumber);
	assert(details->subtreeNumber == subtreeNumber);
	assert(details->bufferNumber >= 0);
	bufferInfo_t* bufInfo = getBufferInfo(details->bufferNumber);
	if (newMax > MAX_DELTAS_ALLOWED) newMax = MAX_DELTAS_ALLOWED;
	if (newMax <= 0) newMax = 1;
	assert(bufInfo->numberEntries == 0);
	assert(newMax >= bufInfo->numberEntries && newMax <= MAX_DELTAS_ALLOWED);
	DEBUG_FILE_PRINTF(stderr, "\nChanging the size of the buffer of subtree number %d from %d to %d", subtreeNumber, bufInfo->maxEntries, newMax);
	bufInfo->maxEntries = newMax;
}

int subtree_tree_t::setInteresting(index_key_t key, node_t x){
	assert(interestingKey == INVALID_KEY);
	assert(interestingNodeSplitVector.empty());
	interestingKeyLocation = INVALID_LOCATION;
	interestingKey = key;
	interestingKeyFound = false;
	interestingNode = x;
	return STATUS_OK;
}

int subtree_tree_t::find_key_in_buffer(node_t& x, index_key_t key, location_t* loc){
	/* This function should be called only in the context of answering a query at a subtree */
	subtreeDetails_t& details = getDetails(x);
	int ret = STATUS_FAILED;
	int subtreeNumber = details.subtreeNumber;
	assert(getDetailsByNumber(subtreeNumber)->treeDetails.root == x);

    if (details.bufferNumber >= 0){
        bufferInfo_t* info = getBufferInfo(details.bufferNumber);
        if (MAX_DELTAS_ALLOWED == 1) assert(info->numberEntries == 0);
        if (info->numberEntries == 0) return STATUS_FAILED;
    }else return STATUS_FAILED;

    /* At this point, we are going due diligence */
    assert(!isBufferEmpty(x));
    queryHit(x);

	if (shouldEmpty(x)){
		int nEntriesWhenFlushing = getBufferInfo(details.bufferNumber)->numberEntries;
		assert(nEntriesWhenFlushing > 0);
		DEBUG_FILE_PRINTF(stderr, "\nPrematurely emptying the buffer for node %d, with just %d entries", x, getBufferInfo(details.bufferNumber)->numberEntries);
		numPrematureEmpties ++;
		sumNEntriesWhenFlushing += nEntriesWhenFlushing;
		setInteresting(key, x);
		DEBUG_FILE_PRINTF(stderr, "\nDoing premature buffer emptying on %d", x);
		empty_buffer(x, true);
		DEBUG_FILE_PRINTF(stderr, "\nEnded premature buffer emptying on %d", x);
		node_t newSubtreeRoot = getDetailsByNumber(subtreeNumber)->treeDetails.root;
		if (newSubtreeRoot != x){
			DEBUG_FILE_PRINTF(stderr, "\nThe subtree root node %d changed to %d while doing the premature buffer emptying", x, newSubtreeRoot);
			x = newSubtreeRoot;
			interestingNodeSplitVector.clear();
		}
		if (interestingKeyFound){
			assert(interestingKey == INVALID_KEY); /* Clear the key once we have found it .. so that others may not report duplicates */
			assert(interestingKeyLocation != INVALID_LOCATION);
			DEBUG_FILE_PRINTF(stderr, "\nFound the key %d via buffer emptying !", key);
			if (loc) *loc = interestingKeyLocation;
			ret = STATUS_OK;
		}
		if (!interestingNodeSplitVector.empty()){
			DEBUG_FILE_PRINTF(stderr, "\nGuess what the interesting node %d split into %d", x, interestingNodeSplitVector.size());
			assert(interestingNodeSplitVector.front().nodeNumber == x);
			int currentIndex = 0;
			if (COMPARE_KEY(key, interestingNodeSplitVector[currentIndex].key) == 1){
				do{
					currentIndex ++;
				}while((COMPARE_KEY(key, interestingNodeSplitVector[currentIndex].key) == 1) && (currentIndex < int(interestingNodeSplitVector.size()) - 1));
				x = interestingNodeSplitVector[currentIndex].nodeNumber;
			}
			interestingNodeSplitVector.clear();
		}
		clearInteresting();
	}else{
		double costNoCaching, realCost;
		tree_buffer_t src_buffer;
		subtree_get_buffer(x, &src_buffer);
		ret = tree_buffer_find_key(&src_buffer, key, loc, &costNoCaching, &realCost, &getDetails(x));
		DEBUG_FILE_PRINTF(stderr, "\nThis buffer scan took %lf, %lf", costNoCaching, realCost);
		numBufferScans++;
		totalBufferScanCost += realCost;
		details.immediateQueryBufferReadCost += realCost;
		details.immediateQuerybufferCostWithoutCaching += costNoCaching;
		details.numberOfQueryScans ++;
		subtree_release_buffer(x, &src_buffer);
	}
	return ret;
}

int subtree_tree_t::find_key_immediate(index_key_t key, location_t* loc){
	currentlyEmptyingSubtreeNumber = -1;
	assert(loc);
	node_t x = rootNode;
	int ret = 0;
	DEBUG_FILE_PRINTF(stderr, "\nSearching for the key %d in immediate search", key);
	int curLevel = getRootDetails().subtreeLevel;
	do{
		assert(isBufferNode(x));	
		assert(curLevel >= 0);
		DEBUG_FILE_PRINTF(stderr, "\nStarting the search on node %d -- level %d", x, getDetails(x).subtreeLevel);
		assert(curLevel == getDetails(x).subtreeLevel);
		if (find_key_in_buffer(x, key, loc) == STATUS_OK){
			DEBUG_FILE_PRINTF(stderr, "\nFound the key in the buffer of node %x also", x);
            ret += 1;
            /* Don't exit the search early -- we need to go all the way down too */
		}
		if (getDetails(x).subtreeLevel == 0){
			node_t result = treeFunctions.route_key(actualTree, key, INVALID_KEY, &getDetails(x).treeDetails, NULL, NULL, NULL, true);
			if (result!= INVALID_NODE){
				assert(sizeof(node_t) == sizeof(location_t));
				memcpy(loc, &result, sizeof(node_t));
				DEBUG_FILE_PRINTF(stderr, "\nFound the key %d in the subtree rooted at %d", key, x);
				ret += 1;
			}else{
				DEBUG_FILE_PRINTF(stderr, "\nDid not find the key %d", key);
			}
			break;
		}else{
			assert(getDetails(x).subtreeLevel > 0);
			node_t tempNode;
			treeFunctions.route_key(actualTree, key, INVALID_KEY, &getDetails(x).treeDetails, NULL, NULL, &tempNode, true);
			assert(isBufferNode(tempNode));
			x = tempNode;
		}
		curLevel--;
	}while(curLevel >= 0);
	return (ret > 0 ? STATUS_OK : STATUS_FAILED);
}

void subtree_tree_t::checkForInteresting(tree_delta_t* delta){
	assert(delta);
	if (interestingKey != INVALID_KEY && interestingKey == delta->key){
		interestingKeyFound = true;
		interestingKey = INVALID_KEY; /* Reset .. so that others dont' report duplicates */
		interestingKeyLocation = delta->loc;
	}
}

int add_transaction_t::funnel_key(index_key_t key){
	if (lastKeyInserted == INVALID_KEY) lastKeyInserted = key;
	else{
		assert(COMPARE_KEY(key, lastKeyInserted) >= 0);
		lastKeyInserted = key;
	}
	if (COMPARE_KEY(key, nodeVector[currentIndex].key) == 1){
		tree->treeFunctions.close_tree(tree->actualTree, getCurrentTreeInfo());
		assert(currentIndex < int(nodeVector.size()) - 1);
		do{
			currentIndex ++;
		}while((COMPARE_KEY(key, nodeVector[currentIndex].key) == 1) && (currentIndex < int(nodeVector.size()) - 1));
		assert(COMPARE_KEY(key, nodeVector[currentIndex].key) <= 0);
		assert(currentIndex < int(nodeVector.size() ) );
		tree->treeFunctions.open_tree(tree->actualTree, getCurrentTreeInfo());
	}
	return STATUS_OK;
}

int add_transaction_t::add_pair(node_key_pair_t pair){
	int ret = -1;
	funnel_key(pair.key);
	vector<node_key_pair_t> splitInfo;
	ret = tree->treeFunctions.put_same_height(tree->actualTree, pair.key, (location_t*)(&pair.nodeNumber), NULL, getCurrentTreeInfo(), &splitInfo);
	subtreeDetails_t& details = getCurrentDetails();
	if (ret == STATUS_OK){
		 assert(splitInfo.size() <= 1);
		 if (details.subtreeLevel > 0){
			assert(tree->isBufferNode(pair.nodeNumber));
		 }
	}else{
		if (ret == NEW_ROOT_ERROR){
			assert(!weSplit);
			assert(splitInfo.size() == 1);
			assert(tree->rootNode == nodeStartedWith);
			assert(nodeVector[currentIndex].key == LARGEST_KEY_IN_NODE);
			node_t new_root = splitInfo[0].nodeNumber;
			tree->rootNodeChanged(new_root);

			/* If we are getting a new root error .. it means that
			 * we have to be the root -- else we can't grow roots
			 * of non root subtrees (as they already have the max
			 * height) */

			nodeStartedWith = tree->rootNode;
			nodeVector[currentIndex].nodeNumber = new_root;
			assert(nodeVector[currentIndex].key == LARGEST_KEY_IN_NODE);
		}else if (ret == SPLIT_TREE_ERROR){
			assert(splitInfo.size() == 2);
			assert(splitInfo[0].nodeNumber == nodeVector[currentIndex].nodeNumber);
			index_key_t oldPivot = nodeVector[currentIndex].key;
			index_key_t newPivot = splitInfo[0].key;
			assert(newPivot >= 0);
			assert(COMPARE_KEY(newPivot, oldPivot) <= 0);
			node_t new_node = splitInfo[1].nodeNumber;
			DEBUG_FILE_PRINTF(stderr, "Subtree rooted at %d (index %d) split into %d @ %d", nodeVector[currentIndex].nodeNumber, currentIndex, new_node, newPivot);

			nodeVector.insert(nodeVector.begin() + currentIndex + 1, node_key_pair_t(oldPivot, new_node));
			assert(nodeVector[currentIndex+1].nodeNumber == new_node);
			nodeVector[currentIndex].key = newPivot;
			node_t currentNode = nodeVector[currentIndex].nodeNumber;
			assert(nodeStartedWith == currentNode || tree->isBufferEmpty(currentNode));
			subtreeDetails_t newNodeDetails(details, new_node);
			tree->setDetails(new_node, newNodeDetails, true);
			tree->treeFunctions.verify_tree(tree->actualTree, &newNodeDetails.treeDetails, newNodeDetails.treeDetails.curHeight);
			tree->treeFunctions.release_tree(tree->actualTree, new_node, newNodeDetails.treeDetails.curHeight, newNodeDetails.treeDetails.rootLevel);
			weSplit = true;
		}else assert(false);
	}
	assert(nodeVector.back().key == lastPivot);
	assert(nodeVector.front().nodeNumber == nodeStartedWith);
	return STATUS_OK;
}

void subtree_tree_t::empty_leaf_buffer(node_t nodeNumber, bool inPrematureEmptyingContext){
	assert(nodeNumber >= 0);
	DEBUG_FILE_PRINTF(stderr, "\nEmptying the buffer for the leaf node %d", nodeNumber);
    assert(!isBufferEmpty(nodeNumber));
	int subtreeAccounted = getDetails(nodeNumber).subtreeNumber; /* Which subtree is being updated */
	tree_buffer_t src_buffer;
	subtree_get_buffer(nodeNumber, &src_buffer);
	tree_buffer_iterator_t buf_iterator = tree_buffer_iterator_create(&src_buffer, subtreeAccounted, &getDetails(nodeNumber));
	int deltaNumber = 0;
	add_transaction_t transaction(this, LARGEST_KEY_IN_NODE, nodeNumber);
	/* this nodeNumber will be updated as a new root etc is created */
	transaction.open_transaction();
	while(tree_buffer_iterator_has_next(&buf_iterator)){
		tree_delta_t delta;
		tree_buffer_iterator_get_next(&buf_iterator, &delta, false);
		checkForInteresting(&delta);
		deltaNumber++;
		node_key_pair_t locationNode;
		DEBUG_FILE_PRINTF(stderr, "\nInserting the delta (number %d) key %d, into the leaf %d", deltaNumber, delta.key, nodeNumber);
		assert(sizeof(locationNode.nodeNumber) == sizeof(delta.loc));
		memcpy(&locationNode.nodeNumber, &delta.loc, sizeof(node_t));	
		locationNode.key = delta.key;
		int status = transaction.add_pair(locationNode);
		assert(status == STATUS_OK);
		keysActuallyInserted++;
	}
	tree_buffer_iterator_destroy(&buf_iterator);
	getDetails(nodeNumber).deltasFlushed += deltaNumber;
	deltasRemaining -= deltaNumber;
	assert(deltasRemaining >= 0);
	tree_buffer_emptied(&src_buffer, subtreeAccounted, &getDetails(nodeNumber));
	subtree_release_buffer(nodeNumber, &src_buffer);
	vector<node_key_pair_t> forUp;	
	transaction.close_transaction(&forUp);
	if (forUp.size() > 1){
		DEBUG_FILE_PRINTF(stderr, "\nLeaf subtree %d split .. recursing up", nodeNumber);
		checkForInterestingNodeSplit(&forUp, nodeNumber);
		handle_leaf_subtree_split_up(&forUp, nodeNumber);
	}
}

#define CLOSE_BUFFER  \
		do{\
			if (destinationNodeNumber >= 0){\
				tree_buffer_end_adding(&currentDestinationBufPtr);\
				node_key_pair_t _nodeToPush(currentPivot, destinationNodeNumber);\
				if (tree_buffer_is_full(&currentDestinationBufPtr)) nodesHavingFullBuffers.push_back(_nodeToPush);\
				assert(currentDestinationBufPtr);\
				subtree_release_buffer(destinationNodeNumber, &currentDestinationBufPtr);\
				assert(!currentDestinationBufPtr);\
				nodesVisitedSoFar.push_back(destinationNodeNumber);\
			}\
		}while(0)

void subtree_tree_t::empty_non_leaf_buffer(node_t nodeNumber, bool inPrematureEmptyingContext){
	assert(nodeNumber >= 0);
	DEBUG_FILE_PRINTF(stderr, "\nEmptying the buffer for the non leaf node %d", nodeNumber);
	assert(!isBufferEmpty(nodeNumber));
	tree_buffer_t src_buffer;
	subtreeDetails_t& details = getDetails(nodeNumber);
	int subtreeAccounted = details.subtreeNumber; /* Which subtree is being updated */
	assert(details.subtreeLevel > 0);
	subtree_get_buffer(nodeNumber, &src_buffer);
	treeFunctions.open_tree(actualTree, &details.treeDetails);
	DEBUG_FILE_PRINTF(stderr, "\nPrinting the emptying node %d", nodeNumber);
	node_t destinationNodeNumber = INVALID_NODE;
	tree_buffer_t currentDestinationBufPtr;
	index_key_t currentPivot = (index_key_t)INVALID_KEY;
	tree_buffer_iterator_t buf_iterator = tree_buffer_iterator_create(&src_buffer, subtreeAccounted, &details);
	vector<node_key_pair_t> nodesHavingFullBuffers;
	vector<int> nodesVisitedSoFar;
	int deltasEmptied = 0;
	int i = 0;
    int numberBufferEntries = getBufferInfo(getDetails(nodeNumber).bufferNumber)->numberEntries;
    if (MAX_DELTAS_ALLOWED == 1) assert(numberBufferEntries <= 1);
	while(tree_buffer_iterator_has_next(&buf_iterator)){
		tree_delta_t delta;
		node_t tempNode;
		index_key_t pivotWentTo;
		tree_buffer_iterator_get_next(&buf_iterator, &delta, false);
		checkForInteresting(&delta);
		deltasEmptied++;
		treeFunctions.route_key(actualTree, delta.key, INVALID_KEY, &details.treeDetails, NULL, &pivotWentTo, &tempNode, true);
		assert(pivotWentTo != INVALID_KEY);
		DEBUG_FILE_PRINTF(stderr, "\nRouted the delta number %d with key %d, to node %d (%d'th pivotKey)", deltasEmptied, delta.key, tempNode, pivotWentTo);
		if (tempNode != destinationNodeNumber){
			CLOSE_BUFFER;
			destinationNodeNumber = tempNode;
			assert(find(nodesVisitedSoFar.begin(), nodesVisitedSoFar.end(), destinationNodeNumber) == nodesVisitedSoFar.end());
			subtree_get_buffer(destinationNodeNumber, &currentDestinationBufPtr);
			subtreeDetails_t& destdetails = getDetails(destinationNodeNumber);
			assert(!(getBufferInfo(destdetails.bufferNumber)->isBufferFull));
			tree_buffer_start_adding(&currentDestinationBufPtr);
			currentPivot = pivotWentTo;
		}else {
			assert(pivotWentTo == currentPivot);
		}
		deltaAdded(tempNode);
		tree_buffer_add_delta(&currentDestinationBufPtr, &delta, subtreeAccounted);
	}
    assert(deltasEmptied <= numberBufferEntries);
	tree_buffer_iterator_destroy(&buf_iterator);
	assert(getBufferInfo(details.bufferNumber)->numberEntries > 0);
    tree_buffer_emptied(&src_buffer, subtreeAccounted, &details);
	details.deltasFlushed += deltasEmptied;
	assert(src_buffer);
	subtree_release_buffer(nodeNumber, &src_buffer);
	assert(!src_buffer);
	assert( int(nodesHavingFullBuffers.size()) <= deltasEmptied);
	CLOSE_BUFFER;
	treeFunctions.close_tree(actualTree, &details.treeDetails);
	for(i = 0; i < int(nodesHavingFullBuffers.size()); i++){
		node_key_pair_t s = nodesHavingFullBuffers[i];
		empty_buffer(s.nodeNumber);
	}
}

void subtree_tree_t::empty_buffer(node_t nodeNumber, bool inPrematureEmptyingContext){

	subtreeDetails_t& details = getDetails(nodeNumber);
	int subtreeNumber = details.subtreeNumber;
    assert(!isBufferEmpty(nodeNumber));
	DEBUG_FILE_PRINTF(stderr, "\nEmptying buffer of node %d, subtree %d, having %d keys, %d pages, bufferNumber: %d, buffer start page: %d", nodeNumber, details.subtreeNumber, isBufferEmpty(nodeNumber) ? 0: getBufferInfo(details.bufferNumber)->numberEntries , isBufferEmpty(nodeNumber) ? 0 : getBufferInfo(details.bufferNumber)->currentPage, details.bufferNumber, details.bufferNumber);
	if (!inPrematureEmptyingContext){
		DEBUG_FILE_PRINTF(stderr, "\nGoing to empty the buffer for node %d (subtree number %d), which has already been emptied %d times", nodeNumber, details.subtreeNumber, details.numberOfBufferEmpties);
	}
    _numBufferEmpties++;
    details.numberOfBufferEmpties ++;
    if (details.subtreeLevel == 0) details.numberOfLeafBufferEmpties ++;
    else details.numberOfNonLeafBufferEmpties++;
    if (!flushMode) _nonFlushNumBufferEmpties ++;
	details.immediateQueryBufferReadCost = 0;
	details.immediateQuerybufferCostWithoutCaching= 0;
	details.numberOfQueryScans = 0;
	details.freshQueryState = QUERY_QUERY;
	details.oldQueries.clear();
	currentlyEmptyingSubtreeNumber = subtreeNumber;
	if (details.subtreeLevel == 0) empty_leaf_buffer(nodeNumber, inPrematureEmptyingContext);
	else empty_non_leaf_buffer(nodeNumber, inPrematureEmptyingContext);
}


int subtree_verify(void* self){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	return tree->verifyTree();
}

int subtree_flush_all_buffers(void* self, bool partialOk){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	subtree_verify(self);
	tree->flush_all_buffers(partialOk);
	subtree_verify(self);
	return STATUS_OK;
}

void subtree_tree_t::tree_insert(index_key_t key, location_t* loc){
	currentlyEmptyingSubtreeNumber = -1;
	tree_delta_t delta;
	assert(key >= 0);
	memcpy(&delta.loc, loc, sizeof(location_t));
	delta.key = key;
	assert(rootBuffer);
	DEBUG_FILE_PRINTF(stderr, "\nBuffer tree added the key %d", key);
	deltasRemaining++;
	deltaAdded(rootNode);
	tree_buffer_add_delta(&rootBuffer, &delta, getRootDetails().subtreeNumber);
	if (tree_buffer_is_full(&rootBuffer)) empty_root_buffer();
}

void subtree_tree_t::empty_root_buffer(){
	empty_buffer(rootNode);
}

void subtree_tree_t::getAllNonEmptyBuffers(node_t nodeNumber, vector<node_t>& subtreesToFlush){
    int i;
    vector<node_key_pair_t> myChildren;
	subtreeDetails_t& details = getDetails(nodeNumber);
    if (!isBufferEmpty(nodeNumber)) subtreesToFlush.push_back(nodeNumber);
    if (details.subtreeLevel > 0){
        get_all_children(nodeNumber, &myChildren, -1, true, false);
        for(i = 0; i< int(myChildren.size()); i++){
            assert(myChildren[i].nodeNumber != INVALID_NODE);
            getAllNonEmptyBuffers(myChildren[i].nodeNumber, subtreesToFlush); 
        }
    }
}

void subtree_tree_t::flush_all_buffers(bool partialOk){
    int i;
	assert(rootBuffer);
	if (!rootBufferEndAdded){
		tree_buffer_end_adding(&rootBuffer);	
		rootBufferEndAdded = true;
	}
	DEBUG_FILE_PRINTF(stderr, "\nDoing the flush now !!!");
	flushMode = true; /* Used for accounting */
    vector<node_t> subtreesToFlush;
    extern int nKeysInserted;
    do{ 
    /* We loop -- as some buffers could have been filled (not necessarily
     * fully) after the first full round of emptying */
        subtreesToFlush.clear();
        getAllNonEmptyBuffers(rootNode, subtreesToFlush);
        subtreeEmptySorter levelSorter(*this, true);
        stable_sort(subtreesToFlush.begin(), subtreesToFlush.end(), levelSorter);
        for(i = 0; i < int(subtreesToFlush.size()); i++){
            node_t x = subtreesToFlush[i];
            getDetails(x); /* To check if this guy still exists */
            if (!isBufferEmpty(x)) empty_buffer(x);
        }
    }while(!subtreesToFlush.empty() && (!partialOk || keysActuallyInserted < (int)((FINAL_FLUSH_THRESH) * nKeysInserted)));
	flushMode = false;
	assert(rootBuffer);
    if (keysActuallyInserted == nKeysInserted){
	    assert(tree_buffer_number_entries(&rootBuffer) == 0);
        assert(numberOfHangingDeltas == 0); /* If we have emptied all our buffers, there is no reason to have stray hanging around deltas */
    }
}

void get_all_children_helper(void* state, index_key_t key, node_t node, index_key_t pivot){
	assert(state);
	vector<node_key_pair_t>* v = (vector<node_key_pair_t>*)state;
	v->push_back(node_key_pair_t(pivot, node));
}

void subtree_tree_t::get_all_children(node_t nodeNumber, vector<node_key_pair_t>* nodeList, int height, bool cheatMode, bool buildParents){
	if (height < 0){
		subtreeDetails_t& details = getDetails(nodeNumber);
		assert(details.treeDetails.root == nodeNumber);
		height = details.treeDetails.curHeight;
	}
	treeFunctions.for_each_child(actualTree, nodeNumber, height, get_all_children_helper, nodeList, cheatMode, buildParents);
}

void subtree_tree_t::flush(){
	flush_all_buffers(false);
	treeFunctions.flush_tree(actualTree, rootNode);
}

int subtree_tree_t::scanBufferForRangeKeys(node_t x, range_t rangeToFind, vector<flash_bt_data_t>& keys){
    double costNoCaching, realCost;
	subtreeDetails_t& details = getDetails(x);
    tree_buffer_t src_buffer;
    subtree_get_buffer(x, &src_buffer);
    int keysFound = tree_buffer_find_keys_in_range(&src_buffer, keys, rangeToFind, &costNoCaching, &realCost, &details);
    DEBUG_FILE_PRINTF(stderr, "\nThis buffer scan took %lf, %lf", costNoCaching, realCost);
    numBufferScans++;
    totalBufferScanCost += realCost;
    details.immediateQueryBufferReadCost += realCost;
    details.immediateQuerybufferCostWithoutCaching += costNoCaching;
    details.numberOfQueryScans ++;
    subtree_release_buffer(x, &src_buffer);
    return keysFound;
}

void subtree_tree_t::get_range_satisfying_emptying_buffers(node_t nodeNumber, vector<node_t>& interestingSubtrees, range_t rangeToFind, range_t myRange, vector<flash_bt_data_t>& keysFound){
    int i;
	subtreeDetails_t& details = getDetails(nodeNumber);
    vector<node_key_pair_t> myChildren;
    /* Is my right one bigger than query's left and is my left one smaller than query's right */
    if (!( (COMPARE_KEY(myRange.second, rangeToFind.first) >= 0) && (COMPARE_KEY(myRange.first, rangeToFind.second) <= 0) )) return;
    if (!isBufferEmpty(nodeNumber)){ /* We must scan this buffer and check for emptying */
        int nKeys = -1;
        queryHit(nodeNumber);
        nKeys = scanBufferForRangeKeys(nodeNumber, rangeToFind, keysFound); 
        if (nKeys > 0){
            DEBUG_FILE_PRINTF(stderr, "\nWe found %d keys interesting keys for the range %d, %d inside the buffer of subtree rooted at %d", nKeys, rangeToFind.first, rangeToFind.second, nodeNumber);
        }
        if (shouldEmpty(nodeNumber)){
            int nEntriesWhenFlushing = getBufferInfo(details.bufferNumber)->numberEntries;
            assert(nEntriesWhenFlushing > 0);
            DEBUG_FILE_PRINTF(stderr, "\n[Range query] Prematurely emptying the buffer for node %d, with just %d entries at level %d", nodeNumber, getBufferInfo(details.bufferNumber)->numberEntries, details.subtreeLevel);
            numPrematureEmpties ++;
            sumNEntriesWhenFlushing += nEntriesWhenFlushing;
            interestingSubtrees.push_back(nodeNumber);
        }
    }

    if (details.subtreeLevel > 0){
        get_all_children(nodeNumber, &myChildren, -1, true, false);
        for(i = 0; i< int(myChildren.size()); i++){
            assert(myChildren[i].nodeNumber != INVALID_NODE);
            range_t childsRange = myRange;
            index_key_t childLeft = (i == 0) ? SMALLEST_KEY_IN_NODE : myChildren[i - 1].key; 
            assert(childLeft == SMALLEST_KEY_IN_NODE || childLeft >= 0);
            assert(COMPARE_KEY(childLeft, LARGEST_KEY_IN_NODE) < 0);
            index_key_t childRight = (i == int(myChildren.size()) - 1) ? LARGEST_KEY_IN_NODE : myChildren[i].key; 
            assert(childRight == LARGEST_KEY_IN_NODE || childRight >= 0);
            assert(COMPARE_KEY(childRight, SMALLEST_KEY_IN_NODE) > 0);
            if (COMPARE_KEY(childLeft, childsRange.first) > 0) childsRange.first = childLeft;
            if (COMPARE_KEY(childRight, childsRange.second) < 0) childsRange.second = childRight;
            get_range_satisfying_emptying_buffers(myChildren[i].nodeNumber, interestingSubtrees, rangeToFind, childsRange, keysFound); 
        }
    }
}

void subtree_tree_t::find_range(range_t* range, vector<flash_bt_data_t>& keys){
    int i;
    assert(range);
    keys.clear();
    DEBUG_FILE_PRINTF(stderr, "\nGot a range query from %d to %d, first searching on the tree, having %d actually inserted keys", range->first, range->second, keysActuallyInserted);
    assert(actualTree);
	treeFunctions.get_range(actualTree, range, keys, &(getRootDetails().treeDetails));
    DEBUG_FILE_PRINTF(stderr, "\nDid the node scanning -- found %d keys", keys.size());
    vector<node_t> subtreesToEmpty;
    range_t rangeToFind = *range;
    range_t wholeRange(SMALLEST_KEY_IN_NODE, LARGEST_KEY_IN_NODE);
    assert(rootNode != INVALID_NODE);
    get_range_satisfying_emptying_buffers(rootNode, subtreesToEmpty, rangeToFind, wholeRange, keys);
    subtreeEmptySorter levelSorter(*this, true);
    stable_sort(subtreesToEmpty.begin(), subtreesToEmpty.end(), levelSorter);
    DEBUG_FILE_PRINTF(stderr, "\nFound %d keys satisfying the range, %d buffers to empty", keys.size(), subtreesToEmpty.size());
    DEBUG_FILE_PRINTF(stderr, "\nNow emptying %d trees in the context of a range query from %d to %d", subtreesToEmpty.size(), range->first, range->second);
	for(i = 0; i < int(subtreesToEmpty.size()); i++){
        node_t x = subtreesToEmpty[i];
        getDetails(x); /* To check if this guy still exists */
		if (!isBufferEmpty(x)){
            empty_buffer(x);
        }
    }
}

int subtree_tree_t::split_buffer(int nodeNumber, vector<node_key_pair_t>* nodeList){
	tree_buffer_t src_buffer;
    double costSpendHere = 0;
    double costSpendHereWithoutCaching = 0;
    int numberDeltasThisTime = 0;

	DEBUG_FILE_PRINTF(stderr, "\nSplitting the buffer for node %d into the node pair list:", nodeNumber);
	print_node_key_pair_list(nodeList);
	assert(nodeList->size() > 1);
	assert(!isBufferEmpty(nodeNumber));
	subtree_get_buffer(nodeNumber, &src_buffer);

    tree_buffer_read_cost(&src_buffer, &costSpendHereWithoutCaching, &costSpendHere, false, NULL);
    subtreeDetails_t& details = getDetails(nodeNumber);
	numberDeltasThisTime = details.bufferNumber < 0 ? 0 : (getBufferInfo(details.bufferNumber))->numberEntries;

	tree_buffer_iterator_t buf_iterator = tree_buffer_iterator_create(&src_buffer, INVALID_NODE, NULL);
	tree_buffer_emptied(&src_buffer, INVALID_NODE, NULL);
	int curDestIndex = 0;
	tree_buffer_t dest_buffer = src_buffer;
	assert((nodeList->at(curDestIndex)).nodeNumber == nodeNumber);
	bool lastBufferToClose = false;
	tree_buffer_start_adding(&dest_buffer);
	int buffersClosed = 0;
	while(tree_buffer_iterator_has_next(&buf_iterator)){
		tree_delta_t delta;
		tree_buffer_iterator_get_next(&buf_iterator, &delta, false);
		if (COMPARE_KEY(delta.key, (nodeList->at(curDestIndex)).key) > 0){
            /* Close the current buffer */
			tree_buffer_end_adding(&dest_buffer);
			if (dest_buffer == rootBuffer) rootBufferEndAdded = true;
			buffersClosed ++;
 			subtree_release_buffer((nodeList->at(curDestIndex)).nodeNumber, &dest_buffer);
			assert(!dest_buffer);

            /* Move to the correct one */

            do{
			    curDestIndex ++;
            }while(curDestIndex < int(nodeList->size()) - 1 && COMPARE_KEY(delta.key, nodeList->at(curDestIndex).key) > 0);

            /* Open the new one */
            assert(curDestIndex < int(nodeList->size())) ;
			subtree_get_buffer((nodeList->at(curDestIndex).nodeNumber), &dest_buffer);
			assert(dest_buffer);
			assert(isBufferEmpty((nodeList->at(curDestIndex)).nodeNumber));
			tree_buffer_start_adding(&dest_buffer);
        }

		assert( int(nodeList->size()) - 1 == curDestIndex || COMPARE_KEY(delta.key,(nodeList->at(curDestIndex)).key) <= 0);
		tree_buffer_add_delta(&dest_buffer, &delta, INVALID_NODE); /* A buffer split is caused of some other node's split up */
		lastBufferToClose = true;
	}
	tree_buffer_iterator_destroy(&buf_iterator);
	if (lastBufferToClose){
		tree_buffer_end_adding(&dest_buffer);
		if (dest_buffer == rootBuffer) rootBufferEndAdded = true;
		subtree_release_buffer((nodeList->at(curDestIndex)).nodeNumber, &dest_buffer);
		buffersClosed ++;
		assert(!dest_buffer);
	}
	assert(buffersClosed <= int(nodeList->size()) );
	return STATUS_OK;
}

int subtree_get_value_immediate(void* self, index_key_t key, location_t* loc){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	location_t locFound;
	int ret = tree->find_key_immediate(key, &locFound);
	if (loc){
		*loc = locFound;
	}else{
		if (ret == STATUS_FAILED){
			locFound = INVALID_LOCATION;
			ret = STATUS_OK; /* Deferred'ish query .. the caller shouldn't know we failed */
		 	DEBUG_FILE_PRINTF(stderr, "Not found %d", key);	
		}else DEBUG_FILE_PRINTF(stderr, "Found %d", key);
	}
	return ret;
}

int subtree_init(void* self, int nodeSize, int bufferSize, int numNodes, int nkeys, char* structuralDetails){
	assert(self);
	assert(nodeSize >= 0);
	assert(bufferSize >= 0);
	subtree_tree_t* tree = new (self) subtree_tree_t(b_tree_describe_tree);
	assert((void*)tree == self);
	assert(structuralDetails);
	int fanout, height;
	sscanf(structuralDetails, "%d-%d", &fanout, &height);
	DEBUG_FILE_PRINTF(stderr, "\nGiven %d, %d", fanout, height);
	assert(fanout >= MIN_FANOUT);
	assert(height >= 1);
	tree->init(nodeSize, fanout, height, numNodes, bufferSize);
	assert(!theGlobalTree);
	theGlobalTree = tree;
	return STATUS_OK;
}

int subtree_destroy(void* self){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	/* Was created via placement new .. so explicit call to destructor reqd */
	tree->~subtree_tree_t();
	return STATUS_OK;
}

int subtree_flush(void* self){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	tree->flush();
	return STATUS_OK;
}

int subtree_get_height(void* self){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	return tree->getRootInfo()->rootLevel + 1;
}

int subtree_get_nkeys(void* self){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	return tree->treeFunctions.get_nkeys(tree->actualTree);
}

int subtree_put_value(void* self, index_key_t key, location_t* loc, int* abortReason){
	subtree_tree_t* tree = (subtree_tree_t*)self;
	tree->tree_insert(key, loc);
	//subtree_verify(self);
	return STATUS_OK;
}

int subtree_get_num_buffers(void* selfPtr){
	return tree_buffer_num_buffers();
}

node_t subtree_tree_get_root(){
	assert(theGlobalTree);
	return theGlobalTree->rootNode;
}

void subtree_get_range_immediate(void* selfPtr, range_t* range, std::vector<flash_bt_data_t>& keys){
	subtree_tree_t* tree = (subtree_tree_t*)selfPtr;
	tree->find_range(range, keys);	
}

int subtree_get_nbus_used(void* selfPtr){
	subtree_tree_t* tree = (subtree_tree_t*)selfPtr;
	return tree->treeFunctions.num_bufs_actual(tree->actualTree);
}

void subtree_loading_done(void* selfPtr){}

void latree_describe_self(index_func_t* f){
	f->indexSize = sizeof(subtree_tree_t);
	f->put_value = subtree_put_value;
	f->get_value = subtree_get_value_immediate;
	f->init_index = subtree_init;
	f->destroy_index = subtree_destroy;
	f->flush_index = subtree_flush;
	f->flush_query_buffers = subtree_flush_all_buffers;
	f->verify_tree = subtree_verify;
	f->get_nkeys = subtree_get_nkeys;
	f->get_height = subtree_get_height;
	f->get_num_buffers = subtree_get_num_buffers;
	f->loading_done = subtree_loading_done;
    f->get_range = subtree_get_range_immediate;
	f->get_nbufs_used = subtree_get_nbus_used;
}

void getTheRootBuffer(tree_buffer_t* buffer){
    assert(theGlobalTree);
    theGlobalTree->subtree_get_buffer(theGlobalTree->rootNode, buffer);
}
