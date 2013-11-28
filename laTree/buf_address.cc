#include <buf_address.h>

bool is_valid(address_t loc){
	return (loc != INVALID_ADDRESS);
}

address_t get_address(address_t addr){/* Can either mean the page numbers or mean the buf number */
	address_t mask = ADDR_MASK;
	return addr & mask;
}

bool is_mem_address(address_t addr){
	address_t mask = MEM_ADDR_BIT_MASK;
	return ((addr & mask) != 0);
}

address_t make_mem_address(int buf){
	address_t mask = MEM_ADDR_BIT_MASK;
	address_t memaddr = (((address_t)buf) | mask);
	assert(is_mem_address(memaddr));
	return memaddr;
}
