#include <tree_structures.h>
#include <vector>
#include <cassert>
using namespace std;

void print_tree_delta(tree_delta_t* delta){
	fprintf(stderr, "\t[key: %d, location: XX]", delta->key); //not printing the location field as not clear what key it represents
}

bool deltaSorter(const tree_delta_t& a, const tree_delta_t& b){return (a.key < b.key);}

void print_node_key_pair_list(vector<node_key_pair_t>* pairList){
	int i;
	assert(pairList);
	fprintf(stderr, "\nPrinting the key pair list (size %d)", pairList->size());
	for(i=0;i< int(pairList->size());i++){
		node_key_pair_t& pair = pairList->at(i);
		fprintf(stderr, "entry: %d [nodeNumber: %d, pivot key: %d] ", i, pair.nodeNumber, pair.key);
	}
	fprintf(stderr, "\nDone printing the key pair list");
}

