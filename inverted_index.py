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
    words = value.split()
    words_set = set(words)
    for w in words_set:
      mr.emit_intermediate(w, key)

# Part 3
def reducer(key, list_of_books):
    # key: word
    # value: list of occurrence counts
    for v in list_of_books:
        mr.emit((key, list_of_books))

# Part 4
inputdata = open("books.json")
#inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
