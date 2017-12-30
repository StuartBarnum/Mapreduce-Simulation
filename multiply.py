import sys
from MapReduce import MapReduce

# Part 1
mr = MapReduce()

# Part 2
def mapper(record):
    # key: matrix id, and the row if the the id is 'a', the column if the id is 'b'
    # value: the column if matrix 'a' and row if matrix 'b', and then the value
    if record[0] == 'a':
        mr.emit_intermediate((record[0],record[1]),(record[2],record[3]))
    if record[0] == 'b':
        mr.emit_intermediate((record[0],record[2]),(record[1],record[3]))


# Part 3
def reducer(key_a, pairs):
    # key: 'a' and row or 'b' and column
    # pairs: columns and values if 'a', rows and values if 'b'
    if key_a[0] == 'a':
        for key_b in mr.intermediate:
            if key_b[0] == 'b':
                dot_product =  0
                for pair_a in pairs:
                    for pair_b in mr.intermediate[key_b]:
                        if pair_a[0] == pair_b[0]:
                            dot_product += pair_a[1] * pair_b[1]
                if dot_product != 0:
                    mr.emit((key_a[1],key_b[1],dot_product))

# Part 4
inputdata = open("matrix.json")
#inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
