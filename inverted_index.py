import sys
from MapReduce import MapReduce

# Coursera exercise:

# Create an Inverted index. Given a set of documents, an inverted index is a dictionary where
# each word is associated with a list of the document identifiers in which that word appears.

# The mapper input is a 2 element list: [document_id, text], where document_id is a string
# representing a document identifier and text is a string representing the text of the document.
# The document text may have words in upper or lower case and may contain punctuation. You
# should treat each token as if it was a valid word; that is, you can just use value.split() to
# tokenize the string.

# The reducer output should be a (word, document ID list) tuple where word is a String and document ID
# list is a list of Strings.


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
