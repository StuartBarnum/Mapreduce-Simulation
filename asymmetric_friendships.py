import sys
from MapReduce import MapReduce

# Part 1
mr = MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    mr.emit_intermediate((key,value), 1)

# Part 3
def reducer(key, value):
    # key: word
    # value: list of occurrence counts
    if (key[1], key[0]) not in mr.intermediate:
        mr.emit((key))
        mr.emit((key[1],key[0]))

# Part 4
inputdata = open("friends.json")
#inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
