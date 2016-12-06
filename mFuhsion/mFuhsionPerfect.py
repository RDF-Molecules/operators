import operator
import sys
from munkres import Munkres, make_cost_matrix, print_matrix

class MFuhsionPerfect:

    """
    FuhSen Join operator with 1-1 perfect matching
    similarityMatrix - the matrix with similarities
    threshold - float value in [0,1]
    table1 - a table which belongs to resource triple list 1
    table2 - a table which belongs to resource triple list 2
    toBeJoined - a list with produced join results
    computedJoins - a list of already computed joins, not to repeat already found results
    isRow - whether a join argument is located in the row or column of the similarity matrix
    """

    def __init__(self, similarity, threshold):
        self.similarityMatrix = similarity
        self.threshold = threshold
        self.table1 = []
        self.table2 = []
        self.toBeJoined = []
        self.computedJoins = []
        self.result_matrix = {}

    def execute(self, rtl1, rtl2):
        self.left = rtl1
        self.right = rtl2

        self.stage1(self.left, self.table2, self.table1,self.similarityMatrix, self.threshold, self.toBeJoined)
        self.stage1(self.right, self.table1, self.table2, self.similarityMatrix, self.threshold, self.toBeJoined)

    def stage1(self, rtl, other_table, own_table, similarity, threshold, output):
        # insert rtl1 into its own table
        if rtl not in own_table:
            own_table.append(rtl)

        # probe rtl against the other table
        self.probe(rtl, other_table, similarity, threshold, output)

    def probe(self, rtl, table, similarity, threshold, output):
        probing_head = rtl['head']
        probing_head_index = probing_head['index']
        for record in table:
            head = record['head']
            head_index = head['index']

            # either probing_head is row and head is column , or vice versa

            if (probing_head, head) not in self.computedJoins:
                # check similarity using the threshold
                test_sim = self.sim(probing_head, head, similarity)
                if test_sim > threshold:
                    # (record, rtl) and (rtl, record) are considered the same in our case, check if it's already in the results
                    # if (record, rtl) not in output:
                    #     output.append((rtl, record))
                    self.computedJoins.append((probing_head, head))
                    if probing_head['row']:
                        self.result_matrix[(probing_head_index, head_index)] = test_sim
                    else:
                        self.result_matrix[(head_index, probing_head_index)] = test_sim

                else:
                    self.computedJoins.append((probing_head, head))
                    if probing_head['row']:
                        self.result_matrix[(probing_head_index, head_index)] = 0
                    else:
                        self.result_matrix[(head_index, probing_head_index)] = 0

    def sim(self, incoming, existing, similarity):
        # if the incoming element is located in rows of the similarity matrix, then indexing will be [inc][exist]
        if incoming['row']:
            return similarity[incoming['index']][existing['index']]
        else:
            # otherwise the element is in columns, then indexing is [exist][inc]
            return similarity[existing['index']][incoming['index']]

    def transformDictToArray(self):
        indexes = sorted(self.result_matrix, key=operator.itemgetter(0, 1))

        # take the last element to find the size of the array
        i, j = indexes[-1]
        result = [[0 for x in range(j+1)] for y in range(i+1)]
        for (a, b) in indexes:
            result[a][b] = self.result_matrix[(a, b)]

        return result

    def computePerfectMatching(self):
        # transform dict to array
        inputMatrix = self.transformDictToArray()

        # compute perfect matching
        cost_matrix = make_cost_matrix(inputMatrix, lambda cost: sys.maxsize - cost)
        m = Munkres()
        perfect_indexes = m.compute(cost_matrix)

        # produce the final result
        # find if rowIndex is in table1 or table2
        # if self.table1[0]['head']['row']:
        #     # index a is in table1
        #     for (a, b) in perfect_indexes:
        #         # find elements with indexes a,b in the left and right tables
        #
        #         self.toBeJoined.append(())
        # else:
        #     # index a is in table2
        #     for (a, b) in perfect_indexes
        #
        #

        return perfect_indexes
