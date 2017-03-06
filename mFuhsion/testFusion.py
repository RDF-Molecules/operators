from mFuhsion import MFuhsion
from mFuhsionPerfect import MFuhsionPerfect
from mergeOperator import MergeOp
from baseline_ops.SimHashJoin import SimilarityHashJoin
from baseline_ops.SymmetricSimHasJoin import SymmetricSimilarityHashJoin
import codecs
import operator
import sys
from munkres import Munkres, make_cost_matrix, print_matrix
import rdflib
from rdflib.plugins.sparql import prepareQuery
import random
from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3
import json
import re

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
         "value": 22},
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "xyz"}
    ]
}

rtl4 = {
    "head": {"uri": "http://dbpedia.org/resource/Drug3",
             "index": 2,
             "row": True},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop1",
         "value": 99},
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "qwerty"}
    ]
}

rtl5 = {
    "head": {"uri":"http://wikidata.org/Drug2",
             "index": 1,
             "row": False},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "poi"},
        {"prop": "http://dbpedia.org/prop/prop3",
         "value": 106}
    ]
}

rtl6 = {
    "head": {"uri":"http://wikidata.org/Drug3",
             "index": 2,
             "row": False},
    "tail": [
        {"prop": "http://dbpedia.org/prop/prop2",
         "value": "string"},
        {"prop": "http://dbpedia.org/prop/prop3",
         "value": 432}
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
threshold = 0.8
nonBlockingGoldStandard = []

def testMFuhsion(path1, path2):
    dbp_source = [rtl1, rtl3, rtl4]
    wd_source = [rtl2, rtl5, rtl6]

    print "Semantic Join Non-Blocking Operator"
    dbp_rtls_path = path1
    drugbank_rtl_path = path2

    dbp_rtls = []
    drugbank_rtls = []
    print "Reading ",path1
    with codecs.open(dbp_rtls_path, "r") as f:
        for line in f:
            newline = line.replace('\"row\": true', '\"row\":True')
            dbp_rtls.append(eval(newline))
    print "Reading ", path2
    with codecs.open(drugbank_rtl_path, "r") as f2:
        for line in f2:
            newline = line.replace('\"row\": false', '\"row\":False')
            drugbank_rtls.append(eval(newline))
    print "Executing Join"
    fusion_op = MFuhsion(threshold)
    for dbe in dbp_rtls:
        for wde in drugbank_rtls:
            fusion_op.execute_new(dbe, wde)

    # for a,b in fusion_op.toBeJoined:
    #     print "Identified joins:", a, " --- ", b

    print "Semantic Join Non-Blocking Perfect Operator"
    print "Overall ",str(len(fusion_op.toBeJoined))," pairs"
    print "Overall time ",fusion_op.total_op_time," seconds including ",fusion_op.total_sim_time, " seconds for sim"
    print "Clean time is ", fusion_op.total_op_time-fusion_op.total_sim_time

    # compute precision and recall
    global nonBlockingGoldStandard
    nonBlockingGoldStandard = fusion_op.toBeJoined[:]
    print "Precision: 1.000"
    print "Recall: 1.000"
    print "-----"

    # print "Merging the RTLs"
    # mergeOp = MergeOp("/Users/mikhailgalkin/Downloads/DBpedia_Ontology/dbpedia_2014.owl")
    # for tbj in fusion_op.toBeJoined:
    #     merged = mergeOp.execute(tbj)
    #     print merged
    #     print "\n"

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


def load_drugs_dbp(file_name, isDbp, limit):
    """

    :param dbp_file: molecules in dbpedia drugs dump
    :param isRow: whether the dump is dbp or db
    :return: reference to the files with RTLs
    """

    if isDbp:
        molecules_file = codecs.open(file_name, "r")
        endpoint = "https://dydra.com/collarad/dbpedia_drugs/sparql"
        newfilename = "dbp_rtl"+str(limit)+".txt"
        output_file = codecs.open("/Users/mikhailgalkin/Downloads/gades_drugs/"+newfilename, "w")
    else:
        molecules_file = codecs.open(file_name, "r")
        endpoint = "https://dydra.com/collarad/drugbank/sparql"
        newfilename = "drugbank_rtl"+str(limit)+".txt"
        output_file = codecs.open("/Users/mikhailgalkin/Downloads/gades_drugs/"+newfilename, "w")

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
        if i==limit:
            break

    molecules_file.close()
    output_file.close()

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

goldStandardPairs = []
def prepareGoldStandardForPrecRec(path1, path2):

    with codecs.open(path1, "r") as f1:
        uris1 = f1.readlines()

    with codecs.open(path2, "r") as f2:
        uris2 = f2.readlines()

    for i in xrange(len(uris1)):
        name1 = re.sub('[<>]', '', uris1[i].strip())
        name2 = re.sub('[<>]', '', uris2[i].strip())
        goldStandardPairs.append((name1,name2))


def testSimHashJoin(path1, path2):
    dbp_source = [rtl4, rtl1, rtl3]
    wd_source = [rtl2, rtl5, rtl6]

    print "Hash Join Blocking Perfect Operator"
    dbp_rtls_path = path1
    drugbank_rtl_path = path2

    dbp_rtls = []
    drugbank_rtls = []
    print "Reading", path1
    with codecs.open(dbp_rtls_path, "r") as f:
        for line in f:
            newline = line.replace('\"row\": true', '\"row\":True')
            dbp_rtls.append(eval(newline))
    print "Reading", path2
    with codecs.open(drugbank_rtl_path, "r") as f2:
        for line in f2:
            newline = line.replace('\"row\": false', '\"row\":False')
            drugbank_rtls.append(eval(newline))
    print "Executing Join"
    simHashOp = SimilarityHashJoin(threshold)
    simHashOp.execute_new(dbp_rtls, drugbank_rtls)

    # for (a,b) in simHashOp.results:
    #     print "Identified joins: ",a," --- ",b

    print "Hash Join Blocking Perfect Operator"
    print "Overall ", str(len(simHashOp.results)), "pairs"
    print "Total operator time ",simHashOp.operatorTimeTotal, " seconds including ",simHashOp.simTimeTotal, " seconds for similarity"
    print "Clean time is ",simHashOp.operatorTimeTotal - simHashOp.simTimeTotal, " seconds"
    # compute precision and recall
    intersection = 0
    for a, b in simHashOp.results:
        if ((a, b) in goldStandardPairs) or ((b, a) in goldStandardPairs):
            intersection += 1
    precision = float(intersection) / len(simHashOp.results)
    recall = float(intersection) / len(goldStandardPairs)

    print "Precision", precision
    print "Recall", recall
    print "-----"

def testSymmetricSimHashJoin(path1, path2):
    dbp_source = [rtl4, rtl1, rtl3]
    wd_source = [rtl2, rtl5, rtl6]
    print "Symmetric Hash Join Non-Blocking Operator"
    dbp_rtls_path = path1
    drugbank_rtl_path = path2

    dbp_rtls = []
    drugbank_rtls = []
    print "Reading", path1
    with codecs.open(dbp_rtls_path, "r") as f:
        for line in f:
            newline = line.replace('\"row\": true', '\"row\":True')
            dbp_rtls.append(eval(newline))
    print "Reading", path2
    with codecs.open(drugbank_rtl_path, "r") as f2:
        for line in f2:
            newline = line.replace('\"row\": false', '\"row\":False')
            drugbank_rtls.append(eval(newline))
    print "Executing Join"
    symSimOp = SymmetricSimilarityHashJoin(threshold)
    for s1 in dbp_rtls:
        for s2 in drugbank_rtls:
            symSimOp.execute(s1, s2)

    # for a, b in symSimOp.results:
    #     print "Identified joins: ", a, " --- ", b
    # print "-----"

    print "Symmetric Hash Join Non-Blocking Operator"
    print "Overall ", str(len(symSimOp.results)), "pairs"
    print "Total operator time ", symSimOp.operatorTimeTotal, "seconds including ", symSimOp.simTimeTotal, "seconds for similarity"
    print "Clean time is",symSimOp.operatorTimeTotal - symSimOp.simTimeTotal, " seconds "

    # compute precision and recall as to Semantic Join Non-Blocking
    intersection = 0
    for a,b in symSimOp.results:
        if ((a,b) in nonBlockingGoldStandard) or ((b,a) in nonBlockingGoldStandard):
            intersection += 1
    precision = float(intersection)/len(symSimOp.results)
    recall = float(intersection)/len(nonBlockingGoldStandard)
    print "Precision as to Semantic Non-Blocking: ", precision
    print "Recall as to Semantic Non-Blocking: ", recall
    print "-----"

def testMFuhsionPerfect(path1, path2):
    dbp_source = [rtl4, rtl1, rtl3]
    wd_source = [rtl2, rtl5, rtl6]
    print "Semantic Join Blocking Perfect Operator"
    dbp_rtls_path = path1
    drugbank_rtl_path = path2
    dbp_rtls = []
    drugbank_rtls = []
    print "Reading", path1
    with codecs.open(dbp_rtls_path, "r") as f:
        for line in f:
            newline = line.replace('\"row\": true', '\"row\":True')
            dbp_rtls.append(eval(newline))
    print "Reading", path2
    with codecs.open(drugbank_rtl_path, "r") as f2:
        for line in f2:
            newline = line.replace('\"row\": false', '\"row\":False')
            drugbank_rtls.append(eval(newline))
    print "Executing Join"
    perfectOp = MFuhsionPerfect(threshold)

    perfectOp.execute_new(dbp_rtls, drugbank_rtls)
    # for a,b in perfectOp.toBeJoined:
    #     print "Identified joins: ",a," --- ",b

    print "Semantic Join Blocking Perfect Operator"
    print "Overall ",str(len(perfectOp.toBeJoined))," pairs"
    print "Total operator time ",perfectOp.total_op_time, "seconds including ",perfectOp.total_sim_time,"seconds for similarity"
    print "Clean time is ",perfectOp.total_op_time - perfectOp.total_sim_time, "seconds"
    # compute precision and recall
    intersection = 0
    for a, b in perfectOp.toBeJoined:
        if ((a, b) in goldStandardPairs) or ((b, a) in goldStandardPairs):
            intersection += 1
    precision = float(intersection) / len(perfectOp.toBeJoined)
    recall = float(intersection) / len(goldStandardPairs)

    print "Precision", precision
    print "Recall", recall
    print "-----"

#load_drugs_dbp("/Users/mikhailgalkin/Downloads/gades_drugs/molecules_dbp_gades.txt", True, 1568)
#load_drugs_dbp("/Users/mikhailgalkin/Downloads/gades_drugs/molecules_db_gades.txt", False, 1568)
prepareGoldStandardForPrecRec("/Users/mikhailgalkin/Downloads/gades_drugs/subjects_drugs_dbpedia.txt","/Users/mikhailgalkin/Downloads/gades_drugs/subjects_drugs_drugbank.txt")
random.seed(9)
testMFuhsion("/Users/mikhailgalkin/Downloads/gades_drugs/dbp_rtl100.txt","/Users/mikhailgalkin/Downloads/gades_drugs/drugbank_rtl100.txt")
testSymmetricSimHashJoin("/Users/mikhailgalkin/Downloads/gades_drugs/dbp_rtl100.txt","/Users/mikhailgalkin/Downloads/gades_drugs/drugbank_rtl100.txt")
testMFuhsionPerfect("/Users/mikhailgalkin/Downloads/gades_drugs/dbp_rtl100.txt","/Users/mikhailgalkin/Downloads/gades_drugs/drugbank_rtl100.txt")
testSimHashJoin("/Users/mikhailgalkin/Downloads/gades_drugs/dbp_rtl100.txt","/Users/mikhailgalkin/Downloads/gades_drugs/drugbank_rtl100.txt")






