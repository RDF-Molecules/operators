'''
Created on Mar 3rd, 2017

Implements a Similarity Hash Join operator.
The intermediate results are represented as Resource Tuple Lists in HashTables.
Doesn't work until all results arrived.

@author: Mikhail Galkin
'''
from time import time
from DataStructures import HashTable, Partition
import operator
import sys
import random
import requests
import json
from munkres import Munkres, make_cost_matrix, print_matrix

class SimilarityHashJoin(object):

    def __init__(self, threshold, simfunction):
        self.left_table  = HashTable()
        self.right_table = HashTable()
        self.results     = []
        self.toBeJoined = []
        self.computedJoins = []
        self.threshold = threshold
        self.simTimeTotal = 0
        self.operatorTimeTotal = 0
        self.simfunction = simfunction

    def execute_new(self, left_rtl_list, right_rtl_list):
        start_op_time = time()
        self.left = left_rtl_list
        self.right = right_rtl_list

        self.insertIntoHashTable(self.left, self.left_table)
        self.insertIntoHashTable(self.right, self.right_table)

        self.probeTables(self.left_table, self.right_table)
        finish_op_time = time()
        self.operatorTimeTotal = finish_op_time - start_op_time

    def insertIntoHashTable(self, list, hashTable):
        for rtl in list:
            sortedTail = sorted(rtl['tail'], key=lambda x:x['prop'])
            # print sortedTail
            stringMolecule = ''
            for var in sortedTail:
                stringMolecule += var['prop'] + str(var['value'])
            # print stringMolecule, hash(stringMolecule)
            i = hash(stringMolecule) % hashTable.size
            hashTable.insertRecord(i, rtl['head']['uri'])

    def probeTables(self, left_table, right_table):
        # left_table.size = right_table.size so either value can be used
        for i in xrange(left_table.size):
            self.computePerfectMatchingForPartitions(left_table.partitions[i], right_table.partitions[i])

    def computePerfectMatchingForPartitions(self, partition1, partition2):
        # compute similarities between RTLs in partitions
        # initialize a simialarity matrix
        if len(partition1.records) > 0 and len(partition2.records) > 0:
            simmatrix = [[0 for i in xrange(len(partition2.records))] for j in xrange(len(partition1.records))]

            for i in xrange(len(partition1.records)):
                for j in xrange(len(partition2.records)):
                    start_sim_time = time()
                    sim_score = self.sim(partition1.records[i], partition2.records[j])
                    finish_sim_time = time()
                    self.simTimeTotal += finish_sim_time - start_sim_time
                    if sim_score >= self.threshold:
                        simmatrix[i][j] = sim_score

            # run hungarian algorithm
            cost_matrix = make_cost_matrix(simmatrix, lambda cost: sys.maxsize - cost)
            m = Munkres()
            perfect_indexes = m.compute(cost_matrix)
            for a,b in perfect_indexes:
                self.results.append((partition1.records[a], partition2.records[b]))

    def sim(self, uri1, uri2):
        url = "http://localhost:9000/similarity/"+self.simfunction+"?minimal=true"
        data = {"tasks": [{"uri1": uri1[1:-1], "uri2": uri2[1:-1]}]}
        headers = {'content-type': "application/json"}
        response = requests.post(url, data=json.dumps(data), headers=headers)
        resp_object = json.loads(response.text)
        return resp_object[0]["value"]

    def execute(self, rtl1, rtl2, out):
        # Executes the Similarity Hash Join.
        self.left = rtl1
        self.right = rtl2
        #self.qresults = out

        # Initialize tuples.
        # tuple1 = None
        # tuple2 = None

        # Get the tuples from the queues.
        # while (not(tuple1 == "EOF") or not(tuple2 == "EOF")):
        #     # Try to get tuple from left queue.
        #     if not(tuple1 == "EOF"):
        #         try:
        #             tuple1 = qleft.get(False)
        #             #print tuple1
        #             self.left.append(tuple1)
        #         except Exception:
        #             # This catch:
        #             # Empty: in tuple2 = self.left.get(False), when the queue is empty.
        #             pass
        #
        #     # Try to get tuple from right queue.
        #     if not(tuple2 == "EOF"):
        #         try:
        #             tuple2 = qright.get(False)
        #             #print tuple2
        #             self.right.append(tuple2)
        #         except Exception:
        #             # This catch:
        #             # Empty: in tuple2 = self.right.get(False), when the queue is empty.
        #             pass

        # Get the variables to join.
        # if ((len(self.left) > 1) and (len(self.right) > 1)):
        #     # Iterate over the lists to get the tuples.
        #     while ((len(self.left) > 1) or (len(self.right) > 1)):
        #         if len(self.left) > 1:
        #             self.insertAndProbe(self.left.pop(0), self.left_table, self.right_table)
        #         if len(self.right) > 1:
        #             self.insertAndProbe(self.right.pop(0), self.right_table, self.left_table)
        #
        # # Put all the results in the output queue.
        # while self.results:
        #     self.qresults.put(self.results.pop(0))
        #
        # # Put EOF in queue and exit.
        # self.qresults.put("EOF")

        self.insertAndProbe(self.left, self.right_table, self.left_table, self.threshold, self.toBeJoined)
        self.insertAndProbe(self.right, self.left_table, self.right_table, self.threshold, self.toBeJoined)

    def insertAndProbe(self, rtl, other_table, own_table, threshold, output):
        # Insert the tuple in its corresponding partition and probe.
        #print tuple
        # Get the attribute(s) to apply hash.
        att = ''
        for var in rtl.tail:
            att = att + var['prop']+var['value']
        i = hash(att) % other_table.size

        # Insert record in partition.
        #record = Record(tuple, time(), 0)
        own_table.insertRecord(i, rtl['head']['uri'])

        # Probe the record against its partition in the other table.
        self.probe(rtl['head']['uri'], other_table.partitions[i], threshold, output)


    def probe(self, rtl_head, partition, threshold, output):
        # Probe a tuple if the partition is not empty.
        if partition:

            # For every record in the partition, check if it is duplicated.
            # Then, check if the tuple matches for every join variable.
            # If there is a join, concatenate the tuples and produce result.

            for existing_rtl in partition.records:
                # compare similarity
                if self.sim(rtl_head, existing_rtl) >= threshold:
                    output.append(rtl_head)







