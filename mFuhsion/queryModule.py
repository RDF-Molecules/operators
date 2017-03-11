from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3
import json
import codecs
import random
import os

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def queryDBpediaSplitted(molecules_list, isDbp):

    if isDbp:
        endpoint = "https://dydra.com/mgalkin/dbp_people0/sparql"
        newfilename = "dbp0_rtl"+str(len(molecules_list))+".txt"
        output_file = codecs.open("/Users/mikhailgalkin/Downloads/gades_dbpedia_people/"+newfilename, "w", encoding='utf-8')
    else:
        endpoint = "https://dydra.com/mgalkin/dbp_people2/sparql"
        newfilename = "dbp2_rtl"+str(len(molecules_list))+".txt"
        output_file = codecs.open("/Users/mikhailgalkin/Downloads/gades_dbpedia_people/"+newfilename, "w", encoding='utf-8')

    query_template = """
            SELECT ?p ?o WHERE { %s ?p ?o . }
        """
    endpoint = SPARQLWrapper(endpoint)
    endpoint.setReturnFormat(JSON)
    i = 0
    for line in molecules_list:
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
        # print query_template % line.strip()
        results = None
        while results is None:
            try:
                endpoint.setQuery(query_template % line.strip())
                results = endpoint.query().convert()
            except:
                print "reconnect"
                pass
        for result in results['results']['bindings']:
            pv_pair = {}
            pv_pair['prop'] = result['p']['value']
            pv_pair['value'] = result['o']['value']
            rtl['tail'].append(pv_pair)
        json.dump(rtl, output_file, ensure_ascii=False)
        output_file.write("\n")
        i += 1
        print i
    output_file.close()


def queryDBP_Wikidata(molecules_list_file, isDbp):

    molecules_list = codecs.open(molecules_list_file, "r").readlines()

    if isDbp:
        endpoint = "http://dydra.com/collarad/dbpedia_people/sparql"
        newfilename = "dbp_rtl"+str(len(molecules_list))+".txt"
        output_file = codecs.open(os.path.dirname(molecules_list_file)+"/"+newfilename, "w", encoding='utf-8')
    else:
        endpoint = "http://dydra.com/collarad/wikidata_people/sparql"
        newfilename = "wikidata_rtl"+str(len(molecules_list))+".txt"
        output_file = codecs.open(os.path.dirname(molecules_list_file)+"/"+newfilename, "w", encoding='utf-8')

    query_template = """
            SELECT ?p ?o WHERE { %s ?p ?o . }
        """
    endpoint = SPARQLWrapper(endpoint)
    endpoint.setReturnFormat(JSON)
    i = 0
    for line in molecules_list:
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
        # print query_template % line.strip()
        results = None
        while results is None:
            try:
                endpoint.setQuery(query_template % line.strip())
                results = endpoint.query().convert()
            except:
                print "reconnect"
                pass
        for result in results['results']['bindings']:
            pv_pair = {}
            pv_pair['prop'] = result['p']['value']
            pv_pair['value'] = result['o']['value']
            rtl['tail'].append(pv_pair)
        json.dump(rtl, output_file, ensure_ascii=False)
        output_file.write("\n")
        i += 1
        print i
    output_file.close()

def loadSplittedDumps(filepath_0, filepath_2):
    # limit = 500 molecules inside
    molecules0 = codecs.open(filepath_0, "r").readlines()
    molecules2 = codecs.open(filepath_2, "r").readlines()

    queryDBpediaSplitted(molecules0, True)
    queryDBpediaSplitted(molecules2, False)


def sampleDBP_Wikidata(filename, sampleSize):
    with codecs.open(filename, "r") as gs:
        subs_dbp = []
        subs_wikidata = []
        for line in gs:
            subs = line.split(" ")
            subs_dbp.append(subs[0])
            subs_wikidata.append(subs[2])

    num_subs = xrange(len(subs_dbp))
    chosen_indices = random.sample(num_subs, sampleSize)

    # save chosen indices
    with codecs.open("/Users/mikhailgalkin/Downloads/gades_wd_dbp_people/indices"+str(sampleSize)+".txt", "w") as indexfile:
        sorted_indices = sorted(chosen_indices)
        for index in sorted_indices:
            indexfile.write("%i\n"%index)

    # create files with sampled entities
    dbp_dump = "dbp_list"+str(sampleSize)+".txt"
    with codecs.open("/Users/mikhailgalkin/Downloads/gades_wd_dbp_people/"+dbp_dump, "w") as dbp:
        for index in chosen_indices:
            dbp.write(subs_dbp[index]+"\n")

    wd_dump = "wd_list"+str(sampleSize)+".txt"
    with codecs.open("/Users/mikhailgalkin/Downloads/gades_wd_dbp_people/"+wd_dump, "w") as wd:
        for index in chosen_indices:
            wd.write(subs_wikidata[index]+"\n")

    # create a gold standard for this sample size
    with codecs.open("/Users/mikhailgalkin/Downloads/gades_wd_dbp_people/goldStandard"+str(sampleSize)+".txt", "w") as newgs:
        for index in chosen_indices:
            newgs.write("%s,%s\n" % (subs_dbp[index], subs_wikidata[index]))


"""
    EXP 2
"""
def loadDbpWikidata(filepath1, filepath2):
    queryDBP_Wikidata(filepath1, True)
    queryDBP_Wikidata(filepath2, False)

loadDbpWikidata("/Users/mikhailgalkin/Downloads/gades_wd_dbp_people/100/dbp_list100.txt","/Users/mikhailgalkin/Downloads/gades_wd_dbp_people/100/wd_list100.txt")