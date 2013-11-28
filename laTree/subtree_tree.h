#ifndef SUBTREE_TREE_H
#define SUBTREE_TREE_H
#include <subtreeInfo.h>
#include <tree_structures.h>
#include <vector>
#include <queue>
#include <map>
#include <set>
#include <tree_func.h>
#include <flash.h>
#include <math.h>
#include <buffer_interface.h>
#include <algorithm>
#include <rooted_bplus_tree_index.h>
#include <index_func.h>
#include <my_debug.h>
#include <google/dense_hash_map>

using namespace std;

extern int MAX_DELTAS_ALLOWED;
extern int BUFFER_ERASE_BLOCKS;
extern int MAX_SUBTREE_HEIGHT;
extern int TREE_MAX_HEIGHT;
extern int REALLY_MAX_HANGING_DELTAS_ALLOWED;
extern int currentlyStolenAmount, numberOfHangingDeltas;
extern buf_manager_t* nodeBufMgr;
enum freshQueryState_t {QUERY_QUERY = 0, QUERY_INSERT = 1, INSERT_QUERY = 2, INSERT_INSERT = 3};

struct cached_buffer_t{
	tree_buffer_t buffer;
	int refcount;
};

struct pastQueryInfo_t{
	int queryId;
	double savingsSoFar;
	double queryCost;
	int numAccumulated;
	double bufferEmptyCost;

	pastQueryInfo_t(int queryId, double queryCost, double bufferEmptyCost) : queryId(queryId),savingsSoFar(0), queryCost(queryCost), numAccumulated(0), bufferEmptyCost(bufferEmptyCost) {}
};

struct subtreeDetails_t;

/* Note on making this a optimized google dense hash map: This one is a bitch
 * to densify -- because of the default constructor requirment, We maintain
 * state at the constructors, so this one's other constructors don't work well
 * with the default constructor */
typedef map<node_t, subtreeDetails_t> subtreeMap_t;

using google::dense_hash_map;
typedef dense_hash_map<int, bufferInfo_t*> bufferMap_t;
typedef dense_hash_map<int, int> bufferToSubtreeNumberMap_t;
typedef dense_hash_map<int, cached_buffer_t> buffer_cache_t;
typedef dense_hash_map<int, subtreeDetails_t*> subtreeNumberMap_t;

struct subtreeDetails_t{
	int bufferNumber;
	subTreeInfo_t treeDetails;
	int subtreeLevel;
	int subtreeNumber;
	int numberOfBufferEmpties;
	int numberOfLeafBufferEmpties;
	int numberOfNonLeafBufferEmpties;
	int deltasFlushed;
	double immediateQueryBufferReadCost;
	double immediateQuerybufferCostWithoutCaching;
	int numberOfQueryScans;
	int numberOfEmptyQueryScans;
	freshQueryState_t freshQueryState;
	vector<pastQueryInfo_t> oldQueries;


/* ******** Class local **********/
	static int numberOfSubtrees;
	static subtreeNumberMap_t* subtreeNumberMap;

	void clearBufferEmptyEstimateCosts(){
		deltasFlushed = 0;
		numberOfQueryScans = 0;
		numberOfEmptyQueryScans= 0;
		immediateQueryBufferReadCost = 0;
		immediateQuerybufferCostWithoutCaching= 0;
		numberOfBufferEmpties = 0;
		numberOfLeafBufferEmpties = 0;
		numberOfNonLeafBufferEmpties = 0;
	}

	static void storeSubtreeNumber(int sNumber, subtreeDetails_t* details){
		if (!subtreeNumberMap){
			/* fprintf(stderr, "\nCreated a new subtree number map"); */
			subtreeNumberMap = new subtreeNumberMap_t; /* Will be only reclaimed on program termination */
			subtreeNumberMap->set_empty_key(INVALID_NODE);
			subtreeNumberMap->set_deleted_key(BAD_NODE);
		}
		assert(subtreeNumberMap->find(sNumber) == subtreeNumberMap->end());
		/* fprintf(stderr, "\nCreated subtree number %d, rooted at %d", sNumber, details->treeDetails.root);*/
		subtreeNumberMap->insert(make_pair(sNumber, details));
	}

	static subtreeDetails_t* getSubtreeNumber(int sNumber){
		assert(subtreeNumberMap);
		assert(subtreeNumberMap->find(sNumber) != subtreeNumberMap->end());
		return subtreeNumberMap->find(sNumber)->second;
	}

	subtreeDetails_t(int subtreeLevel, int maxHeight): bufferNumber(-1), treeDetails(maxHeight), subtreeLevel(subtreeLevel), numberOfBufferEmpties(0), numberOfLeafBufferEmpties(0), numberOfNonLeafBufferEmpties(0), deltasFlushed(0), immediateQueryBufferReadCost(0), immediateQuerybufferCostWithoutCaching(0), numberOfQueryScans(0), numberOfEmptyQueryScans(0), freshQueryState(QUERY_QUERY){
		subtreeNumber = numberOfSubtrees++;
		assert(subtreeNumber < numberOfSubtrees);
		storeSubtreeNumber(subtreeNumber, this);
	}

	subtreeDetails_t(subtreeDetails_t& siblingNode, node_t newNode): bufferNumber(-1), treeDetails(siblingNode.treeDetails, newNode), subtreeLevel(siblingNode.subtreeLevel), numberOfBufferEmpties(0), numberOfLeafBufferEmpties(0) , numberOfNonLeafBufferEmpties(0), deltasFlushed(0), immediateQueryBufferReadCost(0), immediateQuerybufferCostWithoutCaching(0), numberOfQueryScans(0), numberOfEmptyQueryScans(0), freshQueryState(QUERY_QUERY) {
		/* Make sure that subtrees are formed at only integral multiples */
		assert((siblingNode.treeDetails.rootLevel + 1) % MAX_SUBTREE_HEIGHT == 0);
		subtreeNumber = numberOfSubtrees++;
		assert(subtreeNumber < numberOfSubtrees);
		storeSubtreeNumber(subtreeNumber, this);
	}
	
	subtreeDetails_t(const subtreeDetails_t& copy) : bufferNumber(copy.bufferNumber), treeDetails(copy.treeDetails), subtreeLevel(copy.subtreeLevel), subtreeNumber(copy.subtreeNumber), numberOfBufferEmpties(copy.numberOfBufferEmpties), numberOfLeafBufferEmpties(copy.numberOfLeafBufferEmpties), numberOfNonLeafBufferEmpties(copy.numberOfNonLeafBufferEmpties), deltasFlushed(copy.deltasFlushed), immediateQueryBufferReadCost(copy.immediateQueryBufferReadCost), immediateQuerybufferCostWithoutCaching(copy.immediateQuerybufferCostWithoutCaching), numberOfQueryScans(copy.numberOfQueryScans), numberOfEmptyQueryScans(copy.numberOfEmptyQueryScans), freshQueryState(copy.freshQueryState), oldQueries(copy.oldQueries) {
		assert(subtreeNumber < numberOfSubtrees);
		subtreeNumberMap->erase(subtreeNumber);
		subtreeNumberMap->insert(make_pair(subtreeNumber, this));
	}
	
	subtreeDetails_t& operator=(const subtreeDetails_t& copy){
		if (this != &copy){
			assert(subtreeNumber == copy.subtreeNumber);
			bufferNumber = copy.bufferNumber;
			treeDetails = copy.treeDetails;
			subtreeLevel = copy.subtreeLevel;
			subtreeNumber = copy.subtreeNumber;
			numberOfBufferEmpties = copy.numberOfBufferEmpties;
			numberOfLeafBufferEmpties = copy.numberOfLeafBufferEmpties;
			numberOfNonLeafBufferEmpties = copy.numberOfNonLeafBufferEmpties;
			subtreeNumberMap->erase(subtreeNumber);
			subtreeNumberMap->insert(make_pair(subtreeNumber, this));
			deltasFlushed = copy.deltasFlushed;
			immediateQueryBufferReadCost = copy.immediateQueryBufferReadCost;
			immediateQuerybufferCostWithoutCaching = copy.immediateQuerybufferCostWithoutCaching;
			numberOfQueryScans = copy.numberOfQueryScans;
			numberOfEmptyQueryScans= copy.numberOfEmptyQueryScans;
			freshQueryState = copy.freshQueryState;
			oldQueries = copy.oldQueries;
		}
		return *this;
	}
	
	~subtreeDetails_t(){ /* Nullify everything you free */
	}
	

};

typedef void (*tree_describe_self_t)(tree_func_t*);

struct subtree_tree_t{
	node_t rootNode;
	tree_func_t treeFunctions;
	subtreeMap_t treeTable;
	int currentlyEmptyingSubtreeNumber;
	void* actualTree;
	buffer_cache_t cachedBuffers;
	int numBuffers;
	queue<query_answer_t> queriesReturned;
	int actualFanout;
	tree_buffer_t rootBuffer;
	bufferMap_t bufferMap;
	bool flushMode;
	bool rootBufferEndAdded;
	bool interestingKeyFound;
	index_key_t interestingKey;
	node_t interestingNode;
	location_t interestingKeyLocation;
	vector<node_key_pair_t> interestingNodeSplitVector;
	bufferToSubtreeNumberMap_t bufferToSubtreeNumberMap;

	subtree_tree_t(tree_describe_self_t initFunction): rootNode(INVALID_NODE), currentlyEmptyingSubtreeNumber(-1), numBuffers(0), rootBuffer(NULL), flushMode(false), rootBufferEndAdded(false){
		bufferMap.set_empty_key(INVALID_NODE);
		bufferMap.set_deleted_key(BAD_NODE);
		
		bufferToSubtreeNumberMap.set_empty_key(INVALID_NODE);
		bufferToSubtreeNumberMap.set_deleted_key(BAD_NODE);

		cachedBuffers.set_empty_key(INVALID_NODE);
		cachedBuffers.set_deleted_key(BAD_NODE);

		initFunction(&treeFunctions);
		actualTree = malloc(treeFunctions.indexSize);
		clearInteresting();
	}
	void init(int bigNodeSize, int fanout, int height, int numBigNodesCached, int bufferSize);
	~subtree_tree_t(){
		if (rootBuffer){
			subtree_release_buffer(rootNode, &rootBuffer);
		}
		if (actualTree){
			free(actualTree);
			actualTree = NULL;
		}
		tree_buffer_destroy();
		treeTable.clear();
		destroyBufferMap();
	}
	int subtree_release_buffer(node_t subtreeRoot, tree_buffer_t* buffer);
	int subtree_get_buffer(node_t subtreeRoot, tree_buffer_t* buffer);
	node_t new_root_subtree();

	int getDefferedQuery(query_answer_t* answer){
		if (!queriesReturned.size()) return STATUS_FAILED;
		assert(answer);
		*answer = queriesReturned.front();
		queriesReturned.pop();
		return STATUS_OK;
	}

	void open_root_buffer(){
		subtree_get_buffer(rootNode, &rootBuffer);
		tree_buffer_start_adding(&rootBuffer);
		rootBufferEndAdded = false;
	}

	void close_root_buffer(){
		assert(rootBuffer);
		if (!rootBufferEndAdded){
			tree_buffer_end_adding(&rootBuffer);	
			rootBufferEndAdded = true;
		}
		subtree_release_buffer(rootNode, &rootBuffer);
		assert(!rootBuffer);
	}
	inline subtreeDetails_t& getRootDetails(){
		return getDetails(rootNode);
	}

	inline subTreeInfo_t* getRootInfo(){
		return &(getRootDetails().treeDetails);
	}

	inline void rootNodeChanged(node_t new_root){
		setDetails(new_root, getRootDetails(), true);
		treeTable.erase(rootNode);
		fprintf(stderr, "Subtree rooted at %d grew a new root %d", rootNode, new_root);
		rootNode = new_root;
	}

	bool isBufferEmpty(node_t node){
		subtreeDetails_t& details = getDetails(node);
		return (details.bufferNumber < 0 || getBufferInfo(details.bufferNumber)->numberEntries == 0);
	}
	int verifyTree(){
		return treeFunctions.verify_tree(actualTree, getRootInfo(), getRootInfo()->rootLevel + 1);
	}

	subtreeDetails_t* getDetailsByNumber(int subtreeNumber){
		return subtreeDetails_t::getSubtreeNumber(subtreeNumber);
	}

	void deltaAdded(node_t node);
	void queryHit(node_t node);
	void tree_insert(index_key_t key, location_t* loc);
	void find_key_deffered(index_key_t key, location_t* loc);
	void empty_buffer(node_t nodeNumber, bool inPrematureEmptyingContext = false);
	void empty_non_leaf_buffer(node_t nodeNumber, bool dontAccount = false);
	void empty_leaf_buffer(node_t nodeNumber, bool dontAccount = false);
	void handle_leaf_subtree_split_up(vector<node_key_pair_t>* forParent, node_t leafSubtreeRoot);
	void handleSubtreeSplit(node_t node, vector<node_key_pair_t>* splitInto);
	void empty_root_buffer();
	void flush_all_buffers(bool partialOk);
	void get_all_children(node_t nodeNumber, vector<node_key_pair_t>* nodeList, int height, bool cheatMode, bool doParenting);
	void printSubtreeKeys(node_t nodeNumber, int height = -1);
	void flush();
	double getNodeReadCost();
	subtreeDetails_t& getDetails(node_t node);
	void setDetails(node_t node, subtreeDetails_t info, bool newFlag);
	bool isBufferNode(node_t node);
	bufferInfo_t* getBufferInfo(int bufferNumber);
	void destroyBufferMap();		
	int find_key_immediate(index_key_t key, location_t* loc);
	void prepareNodeState(node_t node, int level, int maxHeight);
	void rearrange_buffers(int newHeight);
	void accountBufferEmptyCost(node_t headNode, double cost, int forWhat);
	int split_buffer(int nodeNumber, vector<node_key_pair_t>* nodeList);
	int clearInteresting();
	int setInteresting(index_key_t key, node_t x);
	void checkForInteresting(tree_delta_t* delta);
	void checkForInterestingNodeSplit(vector<node_key_pair_t>* splitVector, node_t x);
	bool shouldEmpty(node_t x);
	int find_key_in_buffer(node_t& x, index_key_t key, location_t* loc);
	bool adjustOldQueryCosts(node_t x);
	void change_buffer(int subtreeNumber, int newMax);
	double getEntireSubtreeCost(int oper, subtreeDetails_t& details);
	void clearBufferEmptyAccountingCosts();
	void getSubtreeCosts(node_t x, double& nodeReadCost, double& nodeWriteCost); 
	int numberOfLeafNodes(subtreeDetails_t& details);
	//void getListOfToBeEmptiedBuffers(hangingBuffers_t* bufferStartPageList);
	void release_root_subtree();
	void reset_hit_rate();
    int scanBufferForRangeKeys(node_t x, range_t rangeToFind, vector<flash_bt_data_t>& keys);
    void get_range_satisfying_emptying_buffers(node_t nodeNumber, vector<node_t>& interestingSubtrees, range_t rangeToFind, range_t myRange, vector<flash_bt_data_t>& keysFound);
    void find_range(range_t* range, vector<flash_bt_data_t>& keys);
    void getAllNonEmptyBuffers(node_t nodeNumber, vector<node_t>& subtreesToFlush);
    void checkAndDoGc();
};

class subtreeEmptySorter{
    subtree_tree_t& tree;
    bool isDesc;
public:
    subtreeEmptySorter(subtree_tree_t& tree, bool isDesc): tree(tree), isDesc(isDesc) {}
    bool operator()(const node_t& a, const node_t& b) const{
        subtreeDetails_t& details_a = tree.getDetails(a);
        subtreeDetails_t& details_b = tree.getDetails(b);
        return (isDesc ? (details_a.subtreeLevel > details_b.subtreeLevel) : (details_b.subtreeLevel > details_a.subtreeLevel));
    }
};



struct add_transaction_t{
	vector<node_key_pair_t> nodeVector;
	int currentIndex;
	subtree_tree_t* tree;
	index_key_t lastKeyInserted;
	bool weSplit;
	index_key_t lastPivot;
	node_t& nodeStartedWith; /* This reference node tracks the current subtree root of the node we started with */
	/* It need not be the same as the node we started with -- we could create a new root for this subtree */

	inline subtreeDetails_t& getCurrentDetails(){
		return tree->getDetails(nodeVector[currentIndex].nodeNumber);
	}

	inline subTreeInfo_t* getCurrentTreeInfo(){
		return &(getCurrentDetails().treeDetails);
	}


	add_transaction_t(subtree_tree_t* tree, index_key_t firstPivot, node_t& node): currentIndex(0), tree(tree), lastKeyInserted(INVALID_KEY), weSplit(false), lastPivot(firstPivot), nodeStartedWith(node){
		/* fprintf(stderr, "\nStarting the transaction on the subtree rooted at %d", node); */
		nodeVector.push_back(node_key_pair_t(firstPivot, node));
	}
	void open_transaction(){
		tree->treeFunctions.open_tree(tree->actualTree, getCurrentTreeInfo());
	}
	void close_transaction(vector<node_key_pair_t>* forParent){
		tree->treeFunctions.close_tree(tree->actualTree, getCurrentTreeInfo());
		if (forParent) *forParent = nodeVector;
	}
	int add_pair(node_key_pair_t pair);
	int funnel_key(index_key_t key);
};

void latree_describe_self(index_func_t* f);
void print_average_buffer_empty_costs(FILE* of);
void open_file();
node_t subtree_tree_get_root();
extern const double FINAL_FLUSH_THRESH;
#endif
