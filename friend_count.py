import MapReduce
import sys

"""
a MapReduce Function for a social network dataset consisting of key-value pairs
where each key is a person and each value is a friend of that person, counting
the number of friends. the output is a (person,  friend count) tuple.
"""

mr = MapReduce.MapReduce()


def mapper(record):
    """
    assumes the input will ba a 2 element list: [personA, personB]
    personA: Name of a person formatted as a string
    personB: Name of one of personA's friends formatted as a string
    DOES NOT imply that personB is a friend of personA
    """
    mr.emit_intermediate(record[0], 1)

def reducer(key, list_of_values):
    total = 0
    for f in list_of_values:
        total += f
    mr.emit((key, total))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

