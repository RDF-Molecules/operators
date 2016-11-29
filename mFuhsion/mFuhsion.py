rtl1 = {
    "head": {"uri": "http://dbpedia.org/resource/Drug1",
             "index": 0},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop1",
         "value": 11},
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "abc"}
    ]
}

rtl2 = {
    "head": {"uri":"http://dbpedia.org/resource/Drug2",
             "index": 2},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "zyx"},
        {"prop": "http://dbpedia.org/prop/prop3",
         "value": 1000 }
    ]
}

similarity = [0.8]
threshold = 0.5

table1 = []
table2 = []

toBeJoined = []

def mFuhsionOperator(rtl1, rtl2, similarity, threshold, table1, table2, toBeJoined):


    result1 = stage1(rtl1,table2, table1, similarity, threshold, toBeJoined)

    result2 = stage1(rtl2, table1, table2, similarity, threshold, toBeJoined)


    return result1.append(result2)

def stage1(rtl, dif_table, own_table, similarity, threshold, toBeJoined):

    # probe
    result = probe(rtl, dif_table, similarity, threshold, toBeJoined)
    #insert
    own_table.append(rtl1)
    return result

def probe(rtl1, table, similarity, threshold, toBeJoined):

    probing_head = rtl1.head
    for some_rtl in table:
        head = some_rtl.head

        # check similarity and threshold
        if sim(probing_head, head, similarity) > threshold:
            #produce join
            toBeJoined.append((rtl1, some_rtl))

    return toBeJoined

def sim(default_head, head, matrix):

    return matrix[default_head.index][head.index]





mFuhsionOperator(rtl1, rtl2, similarity, threshold, table1, table2, toBeJoined)