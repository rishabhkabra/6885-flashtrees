#ifndef BUF_ADDRESS_H
#define BUF_ADDRESS_H
#include <types.h>
#include <assert.h>

#define MEM_ADDR_BIT_MASK ((address_t)((address_t)(0x1) << (8*sizeof(address_t) - 1)))
#define ADDR_MASK ((address_t)(MEM_ADDR_BIT_MASK - (address_t)1))


bool is_valid(address_t loc);
address_t get_address(address_t addr);/* Can either mean the page numbers or mean the buf number */
bool is_mem_address(address_t addr);
address_t make_mem_address(int buf);

#endif
