class Partition(object):
    '''
    Represents a bucket of the hash table.
    It is composed by a list of records, and a list of timestamps
    of the form {DTSlast, ProbeTS}
    '''

    def __init__(self):
        self.records = []  # List of records
        #self.timestamps = []  # List of the form {DTSlas, ProbeTS}


class HashTable(object):
    '''
    Represents a hash table.
    It is composed by a list of partitions (buckets) of size n,
    where n is specified in "size".
    '''

    def __init__(self):
        self.size = 3
        self.partitions = [Partition() for x in xrange(self.size)]

    def getSize(self):
        return self.size

    def insertRecord(self, i, value):
        self.partitions[i].records.append(value)
