import MapReduce
import sys

"""
a mapreduce function for a set of documents creating a dictionary where
each word is associated with a list of the document identifiers in which
that word appears
"""
mr = MapReduce.MapReduce()

def mapper(record):
    """
    assumes an input of a 2 element list:[document_id, text]
    document_id: document identifier formatted as a string
    text: text of the document formatted as a string
    """
    docName = record[0]
    listWords = record[1]
    words = listWords.split()
    for w in words:
      mr.emit_intermediate(w, docName)

      
def reducer(key, list_of_values):
    new_list = []
    for doc in list_of_values:
        if doc not in new_list:
            new_list.append(doc)
    new_list.sort() #thought it will look better
    mr.emit((key, new_list))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

