'''
Created on Mar 3rd, 2017

Implements a Symmetric Similarity Hash Join operator.
The intermediate results are represented as Resource Tuple Lists in HashTables.
A non-blocking operator, works in real-time

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

class SymmetricSimilarityHashJoin(object):

    def __init__(self, threshold, simfunction):
        self.left_table = HashTable()
        self.right_table = HashTable()
        self.results = []
        self.toBeJoined = []
        self.threshold = threshold
        self.simTimeTotal = 0
        self.operatorTimeTotal = 0
        self.computedJoins = {}
        self.simfunction = simfunction

    def execute(self, rtl1, rtl2):
        start_op_time = time()
        self.left = rtl1
        self.right = rtl2

        # insert and probe
        self.insertIntoHashTable(self.left, self.left_table, self.right_table)
        self.insertIntoHashTable(self.right, self.right_table, self.left_table)

        finish_op_time = time()
        self.operatorTimeTotal += finish_op_time - start_op_time

    def insertIntoHashTable(self, rtl, hashTable, probeHashTable):
        sortedTail = sorted(rtl['tail'], key=lambda x: x['prop'])
        # print sortedTail
        stringMolecule = ''
        for var in sortedTail:
            stringMolecule += var['prop'] + str(var['value'])
        # print stringMolecule, hash(stringMolecule)
        i = hash(stringMolecule) % hashTable.size
        if rtl['head']['uri'] not in hashTable.partitions[i].records:
            hashTable.insertRecord(i, rtl['head']['uri'])

        # probe against other table
        self.probe(rtl['head']['uri'], probeHashTable, i)

    def probe(self, uri, hashTable, bucketIndex):
        # find in the relevant bucket all uris which similarity is higher than the threshold
        # TODO delete those ifs when a real similarity rest service is implemented
        for existingUri in hashTable.partitions[bucketIndex].records:
            if (uri, existingUri) in self.computedJoins:
                simscore = self.computedJoins[(uri, existingUri)]
            elif (existingUri, uri) in self.computedJoins:
                simscore = self.computedJoins[(existingUri, uri)]
            else:
                start_sim_time = time()
                simscore = self.sim(uri, existingUri)
                finish_sim_time = time()
                self.simTimeTotal += finish_sim_time - start_sim_time
                self.computedJoins[(uri,existingUri)] = simscore
                self.computedJoins[(existingUri,uri)] = simscore

            if simscore >= self.threshold:
                if ((existingUri, uri) not in self.results) and ((uri, existingUri) not in self.results):
                    self.results.append((uri, existingUri))


    def sim(self, uri1, uri2):
        url = "http://localhost:9000/similarity/"+self.simfunction+"?minimal=true"
        data = {"tasks": [{"uri1": uri1[1:-1], "uri2": uri2[1:-1]}]}
        headers = {'content-type': "application/json"}
        response = requests.post(url, data=json.dumps(data), headers=headers)
        resp_object = json.loads(response.text)
        return resp_object[0]["value"]
