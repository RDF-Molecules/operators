from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3
import json
import codecs

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


def loadSplittedDumps(filepath_0, filepath_2):
    # limit = 500 molecules inside
    molecules0 = codecs.open(filepath_0, "r").readlines()
    molecules2 = codecs.open(filepath_2, "r").readlines()

    queryDBpediaSplitted(molecules0, True)
    queryDBpediaSplitted(molecules2, False)
