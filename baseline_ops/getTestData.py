import codecs
import os

def getTestData(big_filepath, subfile1, subfile2):

    subjects1 = []
    with codecs.open(subfile1, "r") as subs1:
        for line in subs1:
            sub1 = line.strip("\n")[1:-1]
            subjects1.append(sub1)

    subjects2 = []
    with codecs.open(subfile2, "r") as subs2:
        for line in subs2:
            sub2 = line.strip("\n")[1:-1]
            subjects2.append(sub2)

    count1 =0
    count2 = 0
    finalDump0 = codecs.open(os.path.dirname(big_filepath)+"/dbp"+str(len(subjects1))+".nt", "w")
    finalDump2 = codecs.open(os.path.dirname(big_filepath)+"/wd"+str(len(subjects2))+".nt", "w")

    with codecs.open(big_filepath, "r") as bigfile:
        for line in bigfile:
            subject = line.split(" ")[0][1:-1]
            if subject in subjects1:
                finalDump0.write(line)
                count1 +=1
            elif subject in subjects2:
                finalDump2.write(line)
                count2 +=1
    print "First dump entities extracted ", count1
    print "Second dump entities extracted ", count2

    finalDump0.close()
    finalDump2.close()



getTestData("/Users/mikhailgalkin/Downloads/enriched/Wikipedia/together_enriched.nt", "/Users/mikhailgalkin/git/Test-DataSets/DBpedia-Wikidata/operators_evaluation/10000/dbp_list10000.txt", "/Users/mikhailgalkin/git/Test-DataSets/DBpedia-Wikidata/operators_evaluation/10000/wd_list10000.txt")