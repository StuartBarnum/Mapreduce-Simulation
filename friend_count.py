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
    mr.emit_intermediate(key, 1)

# Part 3
def reducer(key, iterations):
    # key: word
    # value: list of occurrence counts
    number = sum(list(iterations))
    mr.emit((key, number))

# Part 4
inputdata = open("friends.json")
#inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
