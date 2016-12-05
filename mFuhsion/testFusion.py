from mFuhsion import MFuhsion
from mergeOperator import MergeOp
import rdflib
from rdflib.plugins.sparql import prepareQuery

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
    print "\n"

print len(fusion_op.toBeJoined)

print "Merging the RTLs"
mergeOp = MergeOp("/Users/mikhailgalkin/Downloads/DBpedia_Ontology/dbpedia_2014.owl")
for tbj in fusion_op.toBeJoined:
    merged = mergeOp.execute(tbj)
    print merged
    print "\n"



# g = rdflib.Graph()
# g.load("/Users/mikhailgalkin/Downloads/DBpedia_Ontology/dbpedia_2014.owl")
# query_result = g.query("""
#             PREFIX owl: <http://www.w3.org/2002/07/owl#>
#             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#             ASK { <http://dbpedia.org/ontology/weight> rdf:type owl:FunctionalProperty . }
#         """)
# for row in query_result:
#     print bool(row)
#print query_result[0]['askAnswer']
#
# query2 = prepareQuery("ASK { ?property rdf:type owl:FunctionalProperty . }",
#                       initNs={"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
#                               "owl": "http://www.w3.org/2002/07/owl#"})
# prop = rdflib.URIRef("http://dbpedia.org/ontology/weight")
# res = g.query(query2, initBindings={'property': prop})
# for row in res:
#     print bool(row)
