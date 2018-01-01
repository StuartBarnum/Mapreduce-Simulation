import sys
from MapReduce import MapReduce

# Coursera exercise:
#
# Consider a simple social network dataset consisting of a set of key-value pairs (person, friend)
# representing a friend relationship between two people. Write a MapReduce algorithm (in Python) to
# count the number of friends for each person.

# Map Input
# Each input record is a 2 element list [personA, personB] where personA is a string representing the
# name of a person and personB is a string representing the name of one of personA's friends. Note that
# it may or may not be the case that the personA is a friend of personB.

# Reduce Output
# The output should be a pair (person, friend_count) where person is a string and friend_count is an
# integer indicating the number of friends associated with person.

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
