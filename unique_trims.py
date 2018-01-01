import sys
from MapReduce import MapReduce

# Coursera exercise:

# Consider a set of key-value pairs where each key is sequence id and each value is a string of
# nucleotides, e.g., GCTTCCGAAATGCTCGAA....

# Write a MapReduce query to remove the last 10 characters from each string of nucleotides, then
# remove any duplicates generated.

# Map Input
# Each input record is a 2 element list [sequence id, nucleotides] where sequence id is a string
# representing a unique identifier for the sequence and nucleotides is a string representing a
# sequence of nucleotides

# Reduce Output
# The output from the reduce function should be the unique trimmed nucleotide strings.

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
