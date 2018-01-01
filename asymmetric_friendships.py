import sys
from MapReduce import MapReduce

# Coursera exercise:

# The relationship "friend" is often symmetric, meaning that if I am your friend, you are my
# friend. Implement a MapReduce algorithm to check whether this property holds. Generate a list
# of all non-symmetric friend relationships.

# Map Input
# Each input record is a 2 element list [personA, personB] where personA is a string representing
# the name of a person and personB is a string representing the name of one of personA's friends.
# Note that it may or may not be the case that the personA is a friend of personB.

# Reduce Output
# The output should be all pairs of individuals in the dataset such that one of the individuals is
# a friend of the other but not vice versa.


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
