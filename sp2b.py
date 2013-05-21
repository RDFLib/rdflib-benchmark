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

    

def testsp2b(stores, open_urls, inputs, queries):
    queries=_read_queries(queries)
    res=defaultdict(lambda : defaultdict(dict))
    for i in inputs: 
        logging.info("Doing input %s"%i)
        data=rdflib.Graph()
        with BZ2File("sp2b/%s.n3.bz2"%i) as f:
            data.parse(f, format='n3')
        
        for s,o in zip(stores, open_urls):
            logging.info("Doing store %s"%s)
            g=rdflib.Graph(store=s)

            if o: 
                logging.info("Opening %s"%o)
                if o=="__tmp__":
                    tmp = mktemp()
                    g.open(tmp, create=True)
                else:
                    tmp = None
                    g.open(o, create=True)
            
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
            
            if o: 
                g.close()
                if tmp: 
                    rmtree(tmp)

    return res

def _read_queries(q):
    return [(x, file("sp2b/queries/"+x).read()) for x in q]

def _all_queries(): 
    return listdir("sp2b/queries")

if __name__=='__main__':

    from optparse import OptionParser

    parser = OptionParser()
    
    parser.add_option("-s", "--stores", 
                      action="append", 
                      dest="stores", 
                      help="store to test (default is IOMemory)")

    parser.add_option("-q", "--queries", 
                      action="append", 
                      dest="queries", 
                      help="queries to test (default is all)")

    parser.add_option("-d", "--data", 
                      action="append", 
                      dest="data", 
                      help="data to test (default is all)")

    parser.add_option("-o", "--open", 
                      action="append", 
                      dest="open", 
                      help="open uris (or __tmp__)")

    parser.add_option("-i", "--iter", 
                      dest="ITERATIONS", 
                      help="repeat each query this many times")


    opts, args = parser.parse_args()

    if opts.stores:
        stores=opts.stores
    else: 
        stores=["default"]
    
    if opts.data:
        inputs=opts.data
    else: 
        inputs=[500*2**x for x in range(11)]
    
    if opts.queries:
        queries=opts.queries
    else: 
        queries=_all_queries()
    
    open_urls = opts.open or []
    open_urls+=[None]*(len(stores)-len(open_urls))
        
    res=testsp2b(stores, open_urls, inputs, queries)
    qs=sorted(res.items()[0][1].items()[0][1])
    for store in res: 
        print "Store: ", store
        print "triples\t"+"\t".join(qs)
        for k in sorted(res[store].keys(), key=lambda x: int(x)):
            qs=sorted(res[store][k])
            print "%s\t"%k+("\t".join("%.2f"%res[store][k][x] for x in qs))
            
