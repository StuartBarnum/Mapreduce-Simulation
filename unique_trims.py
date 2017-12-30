import sys
from MapReduce import MapReduce

# Part 1
mr = MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    mr.emit_intermediate(record[0],record[1][:-10])

# Part 3
def reducer(key, value):
    # key: word
    # value: list of occurrence counts
    if value[0] not in mr.result:
        mr.emit(value[0])

# Part 4
inputdata = open("dna.json")
#inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
