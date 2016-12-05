import rdflib
import copy
from rdflib.plugins.sparql import prepareQuery

class MergeOp:

    def __init__(self, ontology):
        self.ontology = rdflib.Graph()
        print "Loading ontology"
        self.ontology.load(ontology)
        print "Ontology is loaded"
        print "Compiling the query templates"
        self.askQuery = prepareQuery("ASK { ?property rdf:type owl:FunctionalProperty . }",
                              initNs={"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                      "owl": "http://www.w3.org/2002/07/owl#"})
        print "Queries are compiled"
        print "Ready"

    def execute(self, (rtl1, rtl2)):
        self.left = copy.deepcopy(rtl1['tail'])
        self.right = copy.deepcopy(rtl2['tail'])
        self.merged = {}

        # retain URI of the first RTL, doesn't matter
        self.merged['head'] = copy.deepcopy(rtl1['head'])
        self.merged['head']['uri'] = self.merged['head']['uri'] + rtl2['head']['uri'].split("/")[-1]
        self.merged['tail'] = []

        # check tails of RTLs and find relevant merge cases
        for pv_left in self.left:
            for pv_right in self.right:
                # if p1 = p2 and v1 = v2 then just pass the pair into the merged entity
                if (pv_left['prop']==pv_right['prop']) and (pv_left['value']==pv_right['value']):
                    self.merged['tail'].append(pv_left)
                    continue
                # or if p1 = p2 but v1 != v2
                elif pv_left['prop']==pv_right['prop']:
                    # prop case
                    # might have to check the similarity between values of properties
                    # check if prop is functional
                    if self.checkFunctionalProperty(pv_left['prop']):
                        # TODO merge values of pv_left and pv_right
                        self.merged['tail'].append(pv_left)
                    else:
                        # primitive case
                        self.merged['tail'].append(pv_left)
                    continue
                # or if p1 != p2 but v1 = v2
                elif pv_left['value']==pv_right['value']:
                    # value case
                    # compute similarity between the properties
                    self.merged['tail'].append(pv_left)
                    self.merged['tail'].append(pv_right)
                    continue
            # no relevant pair for pv_left , consider as a standalone pv pair
            self.merged['tail'].append(pv_left)

        return self.merged

    def checkFunctionalProperty(self, propertyStr):
        prop = rdflib.URIRef(propertyStr)
        res = self.ontology.query(self.askQuery, initBindings={'property': prop})
        for row in res:
            return bool(row)





