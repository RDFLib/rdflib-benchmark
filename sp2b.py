"""

Run the SP2Bench SPARQL Benchmark suite

http://dbis.informatik.uni-freiburg.de/index.php?project=SP2B

Run like:

python sp2b.py [comma seperated list of stores] [comma separated list of datasets] [comma separated list of query file names] 

The datasets are in ./sp2b 
The query files are in ./sp2b/queries

Example: 

python sp2b.py Sleepycat,default 500,2000,16000

Outputs tab separated table for copy pasting into spreadsheet of your choice

"""

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

import sys

from os import listdir
from shutil import rmtree
from bz2 import BZ2File
from time import time
from collections import defaultdict
from tempfile import mktemp

import rdflib



# do each query this many times
ITERATIONS=1

tmp=None

def setup(store, graph): 
    global tmp
    if store=='Sleepycat': 
        tmp=mktemp('bsddb', 'tmpsp2btest')
        graph.open(tmp, create=True)

def teardown(store, graph):
    if store=='Sleepycat':         
        graph.close()
        rmtree(tmp)
    

def testsp2b(stores, inputs, queries):
    queries=_read_queries(queries)
    res=defaultdict(lambda : defaultdict(dict))
    for i in inputs: 
        logging.info("Doing input %s"%i)
        data=rdflib.Graph()
        with BZ2File("sp2b/%s.n3.bz2"%i) as f:
            data.parse(f, format='n3')
        
        for s in stores:
            logging.info("Doing store %s"%s)
            g=rdflib.Graph(store=s)
            setup(s, g)
            start=time()
            g+=data
            res[s][i]["load"]=time()-start
            logging.info("Load took %.2f"%res[s][i]["load"])
            
            for qname,q in queries: 
                logging.info("Doing query %s"%qname)
                start=time()
                for _ in range(ITERATIONS):
                    list(g.query(q))

                res[s][i][qname]=time()-start
                logging.info("Took %.2f"%res[s][i][qname])

            teardown(s, g)

    return res

def _read_queries(q):
    return [(x, file("sp2b/queries/"+x).read()) for x in q]

def _all_queries(): 
    return listdir("sp2b/queries")

if __name__=='__main__':

    if len(sys.argv)>1:
        stores=sys.argv[1].split(",")
    else: 
        stores=["default"]
    
    if len(sys.argv)>2 and sys.argv[2]!="*":
        inputs=sys.argv[2].split(",")
    else: 
        inputs=[500*2**x for x in range(11)]
    
    if len(sys.argv)>3:
        queries=sys.argv[3].split(",")
    else: 
        queries=_all_queries()
    

        
    res=testsp2b(stores, inputs, queries)
    qs=sorted(res.items()[0][1].items()[0][1])
    for store in res: 
        print "Store: ", store
        print "triples\t"+"\t".join(qs)
        for k in sorted(res[store].keys(), key=lambda x: int(x)):
            qs=sorted(res[store][k])
            print "%s\t"%k+("\t".join("%.2f"%res[store][k][x] for x in qs))
            
