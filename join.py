import MapReduce
import sys

"""
 MapReduce function for a relational join from 2 tables 'Orders' and 'LineItem'.
 it returns as a sql query
 "SELECT * FROM Orders, LineItem WHERE Order.order_id = LineItem.order_id"
"""

mr = MapReduce.MapReduce()


def mapper(record):
    """
    assumes the input will be database records formatted as lists of Strings:
    The first item in the record is a string that identifies which table it originates from
    The second element in the record is the order_id
    """
    orderNum = record[1]
    table = record[0]
    order = record
    mr.emit_intermediate(orderNum, (table, order))

def reducer(key, list_of_values):
    tableA = []
    tableB = []
    ValTableA = list_of_values[0][0]
    for value in list_of_values:
        if value[0] == ValTableA:
            tableA.append(value[1])
        else:
            tableB.append(value[1])
    for line in tableA:
        for item in tableB:
            a = line[:]
            a.extend(item[:])
            mr.emit((a))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

