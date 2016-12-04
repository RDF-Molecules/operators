
class MFuhsion():

    """
    FuhSen Join operator
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

    def execute(self, rtl1, rtl2):
        self.left = rtl1
        self.right = rtl2

        result1 = self.stage1(self.left, self.table2, self.table1,self.similarityMatrix, self.threshold, self.toBeJoined)
        result2 = self.stage1(self.right, self.table1, self.table2, self.similarityMatrix, self.threshold, self.toBeJoined)

    def stage1(self, rtl, other_table, own_table, similarity, threshold, output):
        # insert rtl1 into its own table
        if rtl not in own_table:
            own_table.append(rtl)

        # probe rtl against the other table
        return self.probe(rtl, other_table, similarity, threshold, output)

    def probe(self, rtl, table, similarity, threshold, output):
        probing_head = rtl['head']
        for record in table:
            head = record['head']

            if (probing_head, head) not in self.computedJoins:
                # check similarity using the threshold
                if self.sim(probing_head, head, similarity) > threshold:
                    # (record, rtl) and (rtl, record) are considered the same in our case, check if it's already in the results
                    if (record, rtl) not in output:
                        output.append((rtl, record))
                self.computedJoins.append((probing_head, head))

        return output

    def sim(self, incoming, existing, similarity):
        # if the incoming element is located in rows of the similarity matrix, then indexing will be [inc][exist]
        if incoming['row']:
            return similarity[incoming['index']][existing['index']]
        else:
            # otherwise the element is in columns, then indexing is [exist][inc]
            return similarity[existing['index']][incoming['index']]

# def mFuhsionOperator(rtl1, rtl2, similarity, threshold, table1, table2, toBeJoined):
#
#
#     result1 = stage1(rtl1,table2, table1, similarity, threshold, toBeJoined)
#
#     result2 = stage1(rtl2, table1, table2, similarity, threshold, toBeJoined)
#
#
#     return result1.append(result2)
#
# def stage1(rtl, dif_table, own_table, similarity, threshold, toBeJoined):
#
#     # probe
#     result = probe(rtl, dif_table, similarity, threshold, toBeJoined)
#     #insert
#     own_table.append(rtl1)
#     return result
#
# def probe(rtl1, table, similarity, threshold, toBeJoined):
#
#     probing_head = rtl1.head
#     for some_rtl in table:
#         head = some_rtl.head
#
#         # check similarity and threshold
#         if sim(probing_head, head, similarity) > threshold:
#             #produce join
#             toBeJoined.append((rtl1, some_rtl))
#
#     return toBeJoined
#
# def sim(default_head, head, matrix):
#
#     return matrix[default_head.index][head.index]
#
# mFuhsionOperator(rtl1, rtl2, similarity, threshold, table1, table2, toBeJoined)