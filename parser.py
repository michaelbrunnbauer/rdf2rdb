
import rdflib
import time
import base64
import socket
import urllib
import settings
import sys
from log import log

class parser(object):

    def __init__(self,source,output_triples=False):
        self.output_triples=output_triples
        graph=rdflib.ConjunctiveGraph()
        format=source.split('.')[-1]
        format=format.lower()
        if format in settings.filenameextensions:
            graph.parse(source,format=settings.filenameextensions[format])
        else:
            graph.parse(source)
        self.graph=graph
        self.source=source
        self.parsetime=time.time()
        self.hostname=socket.gethostname()

    def skolemize(self,tc):
        if type(tc) is not rdflib.BNode: return tc
        uri=self.hostname+'_'+self.source+'_'+unicode(tc)+'_'+unicode(self.parsetime)
        uri=uri.encode('utf8')
        uri=urllib.quote_plus(uri)
        uri=settings.skolem_uri_prefix+uri
        return rdflib.URIRef(uri)

    def triplegenerator(self):
        for subj,pred,obj in self.graph:
            if self.output_triples:
                graph=rdflib.ConjunctiveGraph()
                graph.add((subj,pred,obj))
                nt=graph.serialize(format='nt').strip()
                print nt.encode(settings.outputencoding,'ignore')
                sys.stdout.flush()
            if type(subj) is rdflib.Literal:
                log('literal as subject: %s',unicode(subj))
                continue
            subj=self.skolemize(subj)
            if type(pred) is not rdflib.URIRef:
                log('non-uri in predicate position: %s',unicode(pred))
                continue
            obj=self.skolemize(obj)

            assert type(subj) is rdflib.URIRef
            assert type(pred) is rdflib.URIRef
            assert type(obj) in (rdflib.Literal,rdflib.URIRef)
            yield (subj,pred,obj)
