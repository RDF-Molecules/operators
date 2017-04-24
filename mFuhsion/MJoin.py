from time import time
from baseline_ops.DataStructures import HashTable, Partition
import operator
import sys
import random
from multiprocessing import Queue

class MJoin(object):

    def __init__(self, numstreams):
        self.numstreams = numstreams
        self.numauxiliary = numstreams - 2
        self.main_tables = []
        self.auxiliary_tables = []
        self.results = []
        self.numComps = 0

        for i in xrange(numstreams):
            table = []
            self.main_tables.append(table)

        for i in xrange(self.numauxiliary):
            auxtable = []
            self.auxiliary_tables.append(auxtable)

    def execute(self, *args):
        try:
            if self.numstreams!=len(args):
                raise ValueError
            print "I have ", len(args), " arguments"

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
        for k in xrange(3):
            for i in xrange(len(streams)):
                q = streams[i]
                if not q.empty():
                    tuple1 = q.get(False)
                    # insert
                    self.main_tables[i].append(tuple1)
                    # probe against aux tables
                    self.probeAux(tuple1)
                    # probe against other tables
                    self.probeMain(tuple1,i)

    def probeAux(self, tup):
        # probing sequence is 0..n
        # table with i=0 contains interim tuples joined with 2 streams, length=2
        # table with i=1 contains interim tuples joined with 3 streams, length=3
        # table with i=k contains interim tuples joined with n-1 streams, length=n-1
        # start with the highest intermediate results table -> reversed list
        for table in reversed(self.auxiliary_tables):
            # probe against an existing tuple in the auxiliary table
            for existing_tuple in table:
                self.numComps += 1
                print "Comparing ", tup, " with ", existing_tuple
                if existing_tuple['x'] == tup['x']:
                    existing_tuple['y'].append(tup['y'])
                    if len(existing_tuple['y'])==self.numstreams:
                        # max len, produce result
                        self.results.append(existing_tuple)
                        print "Result ", existing_tuple
                        # remove intermediate result from the current table
                        #table.remove(existing_tuple)
                        existing_tuple['delete']=True
                    else:
                        # length increased -> move to another table of higher magnitude
                        self.auxiliary_tables[len(existing_tuple)-2].append(existing_tuple)
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
        # probing sequence is 0..n
        for index in tables_list:
            table = self.main_tables[index]
            # probe against an existing tuple in one of the main tables
            for existing_tuple in table:
                self.numComps += 1
                print "Comparing ", tup, " with ", existing_tuple
                if existing_tuple['x']==tup['x']:
                    # TODO change the data structure of intermediate results
                    intermediate_tuple={'x':tup['x'], 'y':[existing_tuple['y'], tup['y']], 'delete':False}
                    print "New intermediate result ", intermediate_tuple
                    # support for binary joins
                    if self.numauxiliary==0:
                        self.results.append(intermediate_tuple)
                    else:
                        # index 0 denotes a table with intermediate results after 2 matches
                        self.auxiliary_tables[0].append(intermediate_tuple)

    def clearAuxTable(self, table):
        for element in table:
            if element['delete']:
                table.remove(element)


q1 = Queue()
q2 = Queue()
q3 = Queue()
q1.put({'x':1, 'y':2})
q1.put({'x':2, 'y':1})
q1.put({'x':3, 'y':3})
q2.put({'x':2, 'y':2})
q2.put({'x':1, 'y':1})
q2.put({'x':3, 'y':1})
q3.put({'x':1, 'y':3})
q3.put({'x':3, 'y':2})
q3.put({'x':2, 'y':3})
mjoin = MJoin(3)
mjoin.execute(q1, q2, q3)
print "Num comparisons ", mjoin.numComps


