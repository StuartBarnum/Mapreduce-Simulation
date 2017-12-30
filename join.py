import sys
from MapReduce import MapReduce

# Part 1
mr = MapReduce()

# Part 2
def mapper(record):
    # key: order id
    # value: all of the input tuple
    mr.emit_intermediate(record[1], record)

# Part 3
def reducer(ident, tuple_list):
    # key: order id
    # value: list of all the tuples for the id, on which the join is implemented
    for tuple1 in tuple_list:
        for tuple2 in tuple_list:
            if tuple1[0] == 'order' and tuple2[0] == 'line_item':
                mr.emit(tuple1 + tuple2)

# Part 4
inputdata = open("records.json")
#inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

#for line in inputdata:
#    print line
