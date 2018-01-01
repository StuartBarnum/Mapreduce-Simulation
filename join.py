import sys
from MapReduce import MapReduce

# Coursera exercise:

# Implement a relational join as a MapReduce query

# Consider the following SQL query:
# SELECT *
# FROM Orders, LineItem
# WHERE Order.order_id = LineItem.order_id

# Each "Map" input record is a list of strings representing a tuple in the database. Each list
# element corresponds to a different attribute of the table

# The first item (index 0) in each record is a string that identifies the table the record
# originates from. This field has two possible values:

# "line_item" indicates that the record is a line item.
# "order" indicates that the record is an order.
# The second element (index 1) in each record is the order_id.

# LineItem records have 17 attributes including the identifier string.

# Order records have 10 elements including the identifier string.

# The "reducer" output should be a joined record: a single list of length 27 that contains the attributes
# from the order record followed by the fields from the line item record. Each list element should be a string.

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


