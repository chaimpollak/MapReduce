import MapReduce
import sys

"""
a MapReduce function for a matrix multiplication of two matrices A and B in a
sparse matrix format, where each record is of the form i, j, value. The output from the reduce
function will also be matrix row records formatted as tuples. Each tuple will have the format
(i, j, value) where each element is an integer.
"""

mr = MapReduce.MapReduce()


def mapper(record):
    """
    assumes the input will be matrix row records formatted as lists. Each list will
    have the format [matrix, i, j, value] where matrix is a string and i, j, and value
    are integers. The first item, matrix, is a string that identifies which matrix the
    record originates from.
    """
    rows = 5
    if record[0] == 'a':
        for i in range(rows):
            key = (record[1], i)
            mr.emit_intermediate(key, record)
    else:
        for i in range(rows):
            key = (i, record[2])
            mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    rows = 5
    a = {}
    b = {}
    for value in list_of_values:
        if value[0] == 'a':
            a[value[2]] = value[3]
        else:
            b[value[1]] = value[3]
    total = 0
    for i in range(rows):
        try:
            total += a[i]*b[i]
        except:
            continue
    if total != 0:
        mr.emit((key[0],key[1], total))



if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

