import MapReduce
import sys

"""
a MapReduce Function for a set of key-value pairs where each key is sequence id
and each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA...
"""

mr = MapReduce.MapReduce()


def mapper(record):
    """
    assumes the input will ba a 2 element list: [sequence id, nucleotides]
    sequence id: Unique identifier formatted as a string
    nucleotides: Sequence of nucleotides formatted as a string
    """
    seq_id = record[0]
    nucleotides = record[1][:-10]#removes the last 10 chars from each string
    mr.emit_intermediate(nucleotides,seq_id)

def reducer(key, list_of_values):
    mr.emit((key))#removes duplicates


if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
