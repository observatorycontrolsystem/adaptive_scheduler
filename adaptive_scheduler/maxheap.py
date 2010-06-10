'''maxheap.py - Maxheap wrapper around the standard heapq library.
   
   Transparent wrapper around the standard library heapq functions, which orders 
   by maximum value rather than minimum. It does this by negating the value
   before storage.
   
Author: Eric Saunders (esaunders@lcogt.net)

June 2010
'''

import heapq


def heappush(heap, item):

    item = _negate(item)        
    heapq.heappush(heap, item)
    

def heappop(heap):

    item = heapq.heappop(heap)
    return _negate(item)
    

def _negate(item):
    if isinstance(item, tuple):
        # Tuples are immutable, so this horrendous hack creates a new tuple
        # with the contents of the old, but the first element negated. Both
        # arguments to the + operator need to be tuples for the flattening to
        # work.
        item = ((-1*item[0],) + item[1:])
    else:
        item *= -1
    
    return item

