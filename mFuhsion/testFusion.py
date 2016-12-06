from mFuhsion import MFuhsion
from mFuhsionPerfect import MFuhsionPerfect
from mergeOperator import MergeOp
import codecs
import operator
import sys
from munkres import Munkres, make_cost_matrix, print_matrix
import rdflib
from rdflib.plugins.sparql import prepareQuery
from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3
import json

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
similarity = [[0.8, 0.6, 0.5],
              [0.76, 0.54, 0.32],
              [0.9, 0.4, 0.83]]
threshold = 0.4


def testImplementation():
    dbp_source = [rtl1, rtl3, rtl4]
    wd_source = [rtl2, rtl5, rtl6]
    fusion_op = MFuhsion(similarity, threshold)
    for dbe in dbp_source:
        for wde in wd_source:
            fusion_op.execute(dbe, wde)

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


def load_drugs_dbp(file_name, isDbp):
    """

    :param dbp_file: molecules in dbpedia drugs dump
    :param isRow: whether the dump is dbp or db
    :return: reference to the files with RTLs
    """

    if isDbp:
        molecules_file = codecs.open(file_name, "r")
        endpoint = "https://dydra.com/collarad/dbpedia_drugs/sparql"
        output_file = codecs.open("/Users/mikhailgalkin/Downloads/gades_drugs/dbp_rtl.txt", "w")
    else:
        molecules_file = codecs.open(file_name, "r")
        endpoint = "https://dydra.com/collarad/drugbank/sparql"
        output_file = codecs.open("/Users/mikhailgalkin/Downloads/gades_drugs/drugbank_rtl.txt", "w")

    query_template = """
        SELECT ?p ?o WHERE { <%s> ?p ?o . }
    """
    endpoint = SPARQLWrapper(endpoint)
    endpoint.setReturnFormat(JSON)
    i = 0
    for line in molecules_file:
        # create RTL for each subject
        rtl = {}
        rtl['head'] = {}
        rtl['head']['uri'] = line.strip()
        rtl['head']['index'] = i
        if isDbp:
            rtl['head']['row'] = True
        else:
            rtl['head']['row'] = False
        rtl['tail'] = []
        #print query_template % line.strip()
        endpoint.setQuery(query_template % line.strip())
        results = endpoint.query().convert()
        for result in results['results']['bindings']:
            pv_pair = {}
            pv_pair['prop'] = result['p']['value']
            pv_pair['value'] = result['o']['value']
            rtl['tail'].append(pv_pair)
        json.dump(rtl, output_file)
        output_file.write("\n")
        print i
        i += 1
        # if i==10:
        #     break

    molecules_file.close()
    output_file.close()

#load_drugs_dbp("/Users/mikhailgalkin/Downloads/gades_drugs/molecules_dbp_gades.txt", True)
#load_drugs_dbp("/Users/mikhailgalkin/Downloads/gades_drugs/molecules_db_gades.txt", False)

def load_and_run():
    dbp_rtls_path = "/Users/mikhailgalkin/Downloads/gades_drugs/dbp_rtl.txt"
    drugbank_rtl_path = "/Users/mikhailgalkin/Downloads/gades_drugs/drugbank_rtl.txt"

    dbp_rtls = []
    drugbank_rtls = []
    with codecs.open(dbp_rtls_path, "r") as f:
        for line in f:
            newline = line.replace('\"row\": true', '\"row\":True')
            dbp_rtls.append(eval(newline))

    with codecs.open(drugbank_rtl_path, "r") as f2:
        for line in f2:
            newline = line.replace('\"row\": false', '\"row\":False')
            drugbank_rtls.append(eval(newline))

    # prepare similarity matrix
    similarity_matrix = []
    with open("/Users/mikhailgalkin/Downloads/gades_drugs/results_gades.txt") as matrix:
        for i in xrange(0,10):
            matrix_line = matrix.readline().split("\t")[:10]
            similarity_matrix.append([eval(n) for n in matrix_line])
    print similarity_matrix

def testPerfectOperator():
    dbp_source = [rtl4, rtl1, rtl3]
    wd_source = [rtl2, rtl5, rtl6]
    perfectOp = MFuhsionPerfect(similarity, threshold)

    for dbe in dbp_source:
        for wde in wd_source:
            perfectOp.execute(dbe, wde)

    print perfectOp.result_matrix

    results = perfectOp.computePerfectMatching()
    print results

    print "Unsorted table1"
    print perfectOp.table1
    print 'Sorted table1'
    sorted1 = sorted(perfectOp.table1, key=lambda x: x['head']['index'])
    sorted2 = sorted(perfectOp.table2, key=lambda x: x['head']['index'])
    for a,b in results:
        print "Join: %s and %s"%(sorted1[a]['head']['uri'], sorted2[a]['head']['uri'])
    # indexes = sorted(perfectOp.result_matrix, key=operator.itemgetter(0, 1))
    # i, j = indexes[-1]
    # result = [[0 for x in range(j + 1)] for y in range(i + 1)]
    #
    #
    # for a,b in indexes:
    #     result[a][b] = perfectOp.result_matrix[(a,b)]
    # print result
    #
    # cost_matrix = make_cost_matrix(result, lambda cost: sys.maxsize - cost)
    # m = Munkres()
    # sims = m.compute(cost_matrix)
    #print_matrix(result, msg="Lowest values are ")


testPerfectOperator()


