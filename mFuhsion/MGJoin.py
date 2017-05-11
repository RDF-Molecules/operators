from time import time
from baseline_ops.DataStructures import HashTable, Partition
import operator
import sys
import random
from multiprocessing import Queue
from baseline_ops.DataStructures import HashTable
import timeit
import itertools

class MJoin(object):

    def __init__(self, numstreams):
        self.numstreams = numstreams
        self.numauxiliary = numstreams - 2

        self.rjt_tables = []
        self.auxiliary_tables = []
        self.results = Queue()
        self.numComps = 0
        self.computedPairs = []

        for i in xrange(2**numstreams):
            table = dict()
            self.rjt_tables.append(table)

        # for i in xrange(self.numauxiliary):
        #     # auxtable = []
        #     auxtable = HashTable()
        #     self.auxiliary_tables.append(auxtable)

    def setVars(self, vars):
        # must be an array, e.g., ['x']
        self.vars = vars

    def execute(self, args):
        try:
            if self.numstreams!=len(args):
                raise ValueError
            print "MJoin: I have ", len(args), " arguments"

            # get input streams with tuples
            streams = []
            for i in xrange(self.numstreams):
                streams.append(args[i])

            # insert and probe against each other
            self.probeAndInsert(streams)

        except ValueError:
            print "Given number of input sreams is not equal to the initialized number"
            raise

    def probeAndInsert(self, streams):
        stops = [False for x in xrange(len(streams))]
        while any(x is False for x in stops):
            # for index in xrange(len(streams)):
            #     if streams[index].get(False)=="EOF":
            #         stops[index] = True
            for i in xrange(len(streams)):
                q = streams[i]
                if not q.empty():
                    tuple1 = q.get(False)
                    if tuple1=="EOF":
                        stops[i] = True
                        break
                    print "Tuple:", tuple1

                    resource = ""
                    for var in self.vars:
                        resource = resource + str(tuple1[var])

                    # probe against other tables
                    self.probe(tuple1, i, resource)

                    # insert step
                    table_index = 2**(self.numstreams-1) >> i
                    if resource in self.rjt_tables[table_index].keys():
                        self.rjt_tables[table_index][resource].append(tuple1)
                    else:
                        self.rjt_tables[table_index][resource] = [tuple1]

        self.results.put("EOF")

    def probe(self, tuple, streamindex, resource):
        # find all tables to probe
        indices = self.findTables(streamindex)
        bin_index = 2**(self.numstreams-1) >> streamindex

        # start probing from the closest to the output rjt
        for index in indices:
            probeTable = self.rjt_tables[index]
            if resource in probeTable.keys():
                newtuple = {}
                newtuple[resource] = probeTable[resource][:]
                #newtuple = dict.copy(probeTable[resource])
                newtuple[resource].append(tuple)
                if len(newtuple[resource])==self.numstreams:
                    #output result
                    self.generateOutput(newtuple, resource)
                    del probeTable[resource]
                else:
                    # put into the appropriate table
                    if resource in self.rjt_tables[index+bin_index].keys():
                        self.rjt_tables[index+bin_index][resource].append(newtuple)
                    else:
                        self.rjt_tables[index + bin_index][resource] = newtuple[resource]
                    del probeTable[resource]

    def findTables(self, k):
        # find only those indices that do not have 1 in the k-th position of a binary string
        probe_indices = []
        for i in xrange(1, 2**self.numstreams):
            if bin(i)[2:].zfill(self.numstreams)[k]!='1':
                probe_indices.append(i)
        # sort the list by number of 1s in the binary representation
        return sorted(probe_indices, key=lambda x: -sum(int(d) for d in bin(x)[2:]))

    def probeAux(self, tup):
        # probing sequence is 0..n
        # table with i=0 contains interim tuples joined with 2 streams, length=2
        # table with i=1 contains interim tuples joined with 3 streams, length=3
        # table with i=k contains interim tuples joined with n-1 streams, length=n-1
        # start with the highest intermediate results table -> reversed list

        resource = ""
        for var in self.vars:
            resource = resource + str(tup[var])
        ht_index = hash(resource) % self.auxiliary_tables[0].size

        for table in reversed(self.auxiliary_tables):
            # probe against an existing tuple in the auxiliary table in the corresponding partition
            for existing_tuple in table.partitions[ht_index].records:
                self.numComps += 1
                #print "Comparing ", tup, " with ", existing_tuple

                # add to computedPairs
                for entry in existing_tuple['tuple']:
                    self.computedPairs.append((tup, entry))

                join = True
                for entry in existing_tuple['tuple']:
                    for var in self.vars:
                        if entry[var] != tup[var]:
                            join = False
                            break
                if join:
                    newtuple = dict.copy(existing_tuple)
                    newtuple['tuple'].append(tup)
                    print self.vars
                    print newtuple['tuple']
                    if len(newtuple['tuple'])==self.numstreams:
                        self.generateOutput(newtuple)
                        #self.results.put(newtuple)
                        #print "Result ", newtuple
                        # remove intermediate result from the current table
                        #table.remove(existing_tuple)
                        existing_tuple['delete'] = True
                    else:
                        # length increased -> move to another table of higher magnitude
                        #print "New intermediate result", newtuple
                        #self.auxiliary_tables[len(newtuple['tuple'])-2].append(newtuple)
                        self.auxiliary_tables[len(newtuple['tuple'])-2].insertRecord(ht_index, newtuple)
                        # remove intermediate result from the current table
                        #table.remove(existing_tuple)
                        existing_tuple['delete'] = True
            #print "Aux Table before clear ", table
            self.clearAuxTable(table)
            #print "Aux Table after clear ", table


    def probeMain(self, tup, current_index):
        # obtain a list of tables without own table
        tables_list = list(xrange(self.numstreams))
        del tables_list[current_index]

        # find a hash index to compare with
        resource = ""
        for var in self.vars:
            resource = resource + str(tup[var])
        ht_index = hash(resource) % self.main_tables[0].size

        # probing sequence is 0..n
        for index in tables_list:
            table = self.main_tables[index]

            # probe against an existing tuple in one of the main tables
            for existing_tuple in table.partitions[ht_index].records:
                if (tup, existing_tuple) in self.computedPairs:
                    continue
                elif (existing_tuple, tup) in self.computedPairs:
                    continue
                else:
                    self.numComps += 1
                    #print "Comparing ", tup, " with ", existing_tuple

                    # add to computedPairs
                    self.computedPairs.append((tup, existing_tuple))

                    join = True
                    for var in self.vars:
                        if existing_tuple[var]!=tup[var]:
                            join = False
                            break
                    if join:
                        intermediate_tuple={'tuple': [tup, existing_tuple], 'delete':False}
                        #print "New intermediate result ", intermediate_tuple
                        # support for binary joins
                        if self.numauxiliary==0:
                            #self.results.append(intermediate_tuple)
                            self.generateOutput(intermediate_tuple)
                            #self.results.put(intermediate_tuple)
                        else:
                            # index 0 denotes a table with intermediate results after 2 matches
                            #self.auxiliary_tables[0].append(intermediate_tuple)
                            self.auxiliary_tables[0].insertRecord(ht_index, intermediate_tuple)

    def clearAuxTable(self, table):
        for partition in table.partitions:
            for element in partition.records:
                if element['delete']:
                    # print 'flusing', element
                    partition.records.remove(element)

    def generateOutput(self, newtuple, resource):
        # transform the result into joined tuples
        # non_key_vars = set(newtuple['tuple'].keys())-set(self.vars)
        # key_vars = set(newtuple['tuple'].keys())
        output_object = {}
        for obj in newtuple[resource]:
            for key in obj.keys():
                if key not in output_object:
                    output_object[key] = set()
                output_object[key].add(obj[key])

        sets = []
        for key in output_object.keys():
            sets.append(output_object[key])

        res = list(itertools.product(*sets))

        # len of the tuple in res equals to the number of keys
        # generate the anapsid/mulder output
        # print res
        for elem in res:
            output = {}
            for i in xrange(len(output_object.keys())):
                output[output_object.keys()[i]] = elem[i]
            # print output
            self.results.put(output)


def testmjoin():
    q1 = Queue()
    q2 = Queue()
    q3 = Queue()
    q4 = Queue()
    # q1.put({'x':1, 'y':2})
    # q1.put({'x':2, 'y':1})
    # q1.put({'x':3, 'y':3})
    # q1.put({'x':4, 'y':3})
    # q1.put({'x':5, 'y':2})
    # q1.put("EOF")
    # q2.put({'x':2, 'y':2})
    # q2.put({'x':1, 'y':1})
    # q2.put({'x':3, 'y':1})
    # q2.put({'x':4, 'y':2})
    # q2.put({'x':5, 'y':3})
    # q2.put("EOF")
    # q3.put({'x':1, 'y':3})
    # q3.put({'x':3, 'y':2})
    # q3.put({'x':2, 'y':3})
    # q3.put({'x':4, 'y':4})
    # q3.put({'x':5, 'y':1})
    # q3.put("EOF")
    # q4.put({'x':4, 'y':1})
    # q4.put({'x':3, 'y':4})
    # q4.put({'x':2, 'y':4})
    # q4.put({'x':1, 'y':4})
    # q1.put({'x':5, 'y':4})
    # q4.put("EOF")


    # q1.put({'x': 1, 'y': 2, 'z':1})
    # q1.put({'x':2, 'y':1, 'z':3})
    # q1.put({'x':3, 'y':3})
    # q1.put({'x':4, 'y':3})
    # q1.put({'x':5, 'y':2})
    # q1.put("EOF")
    # q2.put({'x':2, 'y':1, 'z':2})
    # q2.put({'x':1, 'y':3, 'z':3})
    # q2.put({'x':3, 'y':1})
    # q2.put({'x':4, 'y':2})
    # q2.put({'x':5, 'y':3})
    # q2.put("EOF")
    # q3.put({'x':1, 'y':2, 'z':4})
    # q3.put({'x':3, 'y':2})
    # q3.put({'x':2, 'y':1, 'z':5})
    # q3.put({'x':4, 'y':4})
    # q3.put({'x':5, 'y':1})
    # q3.put("EOF")

    # q4.put({'x':4, 'y':1})
    # q4.put({'x':3, 'y':4})
    # q4.put({'x':2, 'y':4})
    # q4.put({'x':1, 'y':4})
    # q1.put({'x':5, 'y':4})
    # q4.put("EOF")

    q1.put({'pt': "http://pf1", 'publisher': "http://pu1"})
    q1.put("EOF")
    q2.put({'pt': "http://pf1"})
    q2.put("EOF")
    q3.put({'pt': "http://pf1", 'label': "pt 1", 'date': "01-02-2017"})
    q3.put("EOF")

    mjoin = MJoin(3)
    mjoin.setVars(['pt'])
    queues = [q1,q2,q3]
    mjoin.execute(queues)
    print "Num comparisons ", mjoin.numComps
    print "Results:"
    count = 0
    while True:
        try:
            print mjoin.results.get(False)
            count +=1
        except:
            break

    print "Num results", count-1

# times = min(timeit.Timer(testmjoin).repeat(repeat=3, number=100))
# print times / 100
testmjoin()

def findTables(k):
    probe_indices = []
    for i in xrange(1,16):
        if bin(i)[2:].zfill(4)[k]!='1':
            probe_indices.append(i)
    # sort the list by number of 1s in the binary representation
    a = sorted(probe_indices, key=lambda x: -sum(int(d) for d in bin(x)[2:]))
    return a




