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
        print "Doing input ",i
        data=rdflib.Graph()
        with BZ2File("sp2b/%s.n3.bz2"%i) as f:
            data.parse(f, format='n3')
        
        for s in stores:
            print "Doing store ",s
            g=rdflib.Graph(store=s)
            setup(s, g)
            start=time()
            g+=data
            res[s][i]["load"]=time()-start
            print "Load took %.2f"%res[s][i]["load"]
            
            for qname,q in queries: 
                print "Doing query ", qname
                start=time()
                for _ in range(ITERATIONS):
                    list(data.query(q))

                res[s][i][qname]=time()-start
                print "Took %.2f"%res[s][i][qname]

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
        inputs=[500*2**x for x in range(12)]
    
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
            print "%s"%k,
            qs=sorted(res[store][k])
            print "\t".join("%.2f"%res[store][k][x] for x in qs)
            
