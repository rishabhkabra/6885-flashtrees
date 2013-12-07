#! /usr/bin/python
import random
import sys

def skew_trace(ltu, num_queries, num_elements, range_keys):
  data_file = open("in.dat", 'w')
  query_file = open("query.dat", 'w')
  keys = []
  count = 0
  for i in range(10):
    keys.append([])
    for j in range(i*num_elements/55):
      count += 1
      x = random.randint(i*range_keys/10, (i+1)*range_keys/10)
      keys[i].append(x)
      data_file.write("%d %d " % (x,random.randint(1000, 938243)))
  lookups = ltu*num_queries
  inserts = num_queries - lookups
  for i in range(num_queries):
    j = random.randint(0,1)
    if j == 0 and lookups > 0:
      p = random.randint(0,9)
      num = 0
      try:
        num = random.choice(keys[p]) if random.randint(0,1)==0 else random.randint(0, range_keys)
      except:
        num = random.randint(0, range_keys)
      query_file.write("s %d\n" % num) 
      lookups -= 1
    else:
      if inserts > 0:
        inserts -= 1
        k = random.randint(0, range_keys)
#        print "value: %d, mod: %d" % ( k, range_keys/10)
        index = min(9,k / (range_keys/10))
#        print "Index", index
        keys[index].append(k)
        query_file.write("i %d %d\n" %(k,random.randint(1000, 938243)))
#  print "Tree initialized with %d entries" % count   
    

skew_trace(float(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))


    
