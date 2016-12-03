from mFuhsion import MFuhsion

rtl1 = {
    "head": {"uri": "http://dbpedia.org/resource/Drug1",
             "index": 0,
             "row": True},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop1",
         "value": 11},
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "abc"}
    ]
}

rtl2 = {
    "head": {"uri":"http://wikidata.org/Drug1",
             "index": 0,
             "row": False},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "zyx"},
        {"prop": "http://dbpedia.org/prop/prop3",
         "value": 1000}
    ]
}

rtl3 = {
    "head": {"uri": "http://dbpedia.org/resource/Drug2",
             "index": 1,
             "row": True},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop1",
         "value": 11},
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "abc"}
    ]
}

rtl4 = {
    "head": {"uri": "http://dbpedia.org/resource/Drug3",
             "index": 2,
             "row": True},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop1",
         "value": 11},
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "abc"}
    ]
}

rtl5 = {
    "head": {"uri":"http://wikidata.org/Drug2",
             "index": 1,
             "row": False},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "zyx"},
        {"prop": "http://dbpedia.org/prop/prop3",
         "value": 1000}
    ]
}

rtl6 = {
    "head": {"uri":"http://wikidata.org/Drug3",
             "index": 2,
             "row": False},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "zyx"},
        {"prop": "http://dbpedia.org/prop/prop3",
         "value": 1000}
    ]
}
"""
similarity matrix
                    wikidata1(rtl2)   wikidata2(rtl5)   wikidata3(rtl6)
dbpedia1(rtl1)       sim1               sim2                sim3
dbpedia2(rtl3)       sim4               sim5                sim6
dbpedia3(rtl4)       sim7               sim8                sim9
"""
similarity = [[0.8, 0.6, 0.5], [0.76, 0.54, 0.32], [0.9, 0.4, 0.83]]
threshold = 0.5

dbp_source = [rtl1, rtl3, rtl4]
wd_source = [rtl2, rtl5, rtl6]
fusion_op = MFuhsion(similarity, threshold)
for dbe in dbp_source:
    for wde in wd_source:
        fusion_op.execute(dbe, wde)
#fusion_op.execute(rtl1, rtl2)

for tbj in fusion_op.toBeJoined:
    for ent in tbj:
        print ent['head']['uri']
    print '\n'

print len(fusion_op.toBeJoined)


