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
        self.computedPairs = []

        for i in xrange(numstreams):
            table = []
            self.main_tables.append(table)

        for i in xrange(self.numauxiliary):
            auxtable = []
            self.auxiliary_tables.append(auxtable)

    def setVars(self, vars):
        # must be an array, e.g., ['x']
        self.vars = vars

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
        stops = [False for x in xrange(len(streams))]
        while any(x==False for x in stops):
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
                    existing_tuple['tuple'].append(tup)
                    if len(existing_tuple['tuple'])==self.numstreams:
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
                if (tup, existing_tuple) in self.computedPairs:
                    continue
                elif (existing_tuple, tup) in self.computedPairs:
                    continue
                else:
                    self.numComps += 1
                    print "Comparing ", tup, " with ", existing_tuple

                    # add to computedPairs
                    self.computedPairs.append((tup, existing_tuple))

                    join = True
                    for var in self.vars:
                        if existing_tuple[var]!=tup[var]:
                            join = False
                            break
                    if join:
                        # TODO change the data structure of intermediate results
                        intermediate_tuple={'tuple': [tup, existing_tuple], 'delete':False}
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
                # print 'flusing', element
                table.remove(element)


q1 = Queue()
q2 = Queue()
q3 = Queue()
q1.put({'x':1, 'y':2})
q1.put({'x':2, 'y':1})
q1.put({'x':3, 'y':3})
q1.put("EOF")
q2.put({'x':2, 'y':2})
q2.put({'x':1, 'y':1})
q2.put({'x':3, 'y':1})
q2.put("EOF")
q3.put({'x':1, 'y':3})
q3.put({'x':3, 'y':2})
q3.put({'x':2, 'y':3})
q3.put("EOF")
mjoin = MJoin(3)
mjoin.setVars(['x'])
mjoin.execute(q1, q2, q3)
print "Num comparisons ", mjoin.numComps
print "Num results", len(mjoin.results)
print "Results", mjoin.results


