import MapReduce
import sys

"""
a MapReduce function checking whether friendships are symmetric and outputs
a list of all non-symmetric friend relationships
"""

mr = MapReduce.MapReduce()


def mapper(record):
    """
    assumes the input will ba a 2 element list: [personA, personB]
    personA: Name of a person formatted as a string
    personB: Name of one of personA's friends formatted as a string
    DOES NOT imply that personB is a friend of personA
    """
    person = record[0]
    friend = record[1]
    if person > friend:
        key = (person,friend)
    else:
        key = (friend, person)
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    if sum(list_of_values) != 2:
        mr.emit(key)
        mr.emit((key[1],key[0]))
        

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

