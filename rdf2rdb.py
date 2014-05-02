#!/usr/bin/python

from log import log
from parser import parser
import app
from datatype import convert_literal
import settings
import sys
import rdflib
from optparse import OptionParser
import traceback

assert settings.max_uri_length>34
assert settings.max_label_length>3

op=OptionParser(usage = "usage: %prog [options] file/url file/url ...")
op.add_option('--drop',dest='drop_database',action='store_true',
              default=False,help='if database already exists drop and recreate it')
op.add_option("-d", dest="database",
              help="use database name DB instead of the one in settings.py", metavar="DB")
op.add_option('-n',dest='entailment',action='store_false',
              default=True,help='disable entailments/reasoning')
op.add_option('-t',dest='with_tbox',action='store_true',
              default=False,help='also convert T-box triples')
op.add_option('-r',dest='delete_thing_tables',action='store_true',
              default=False,help='delete tables that relate to owl:Things after running (important information for incremental runs may be lost)')
op.add_option('-o',dest='output_triples',action='store_true',
              default=False,help='write triples to stdout as they are processed (as ntriples)')
op.add_option('-l',dest='with_sql_logging',action='store_true',
              default=False,help='log SQL commands that change data to stdout')
op.add_option('-q',dest='with_sql_query_logging',action='store_true',
              default=False,help='log SQL commands that query data to stdout')
op.add_option('-p',dest='dry_run',action='store_true',
              default=False,help='parse only mode, check files/urls for parsingerrors')
op.add_option('-v',dest='version',action='store_true',
              default=False,help='output version and exit')

(options,args)=op.parse_args()

if options.version:
    print "rdf2rdb.py version",app.version
    sys.exit(0)

if not args:
    op.print_help()
    sys.exit(1)

if options.database:
    settings.dbname=options.database

if not options.dry_run: app.init(options)

try:
    for source in args:
        log('parsing %s',source)
        p=parser(source,output_triples=options.output_triples)
        for s,p,o in p.triplegenerator():
            s=unicode(s)
            if len(s)>settings.max_uri_length:
                log('uri too long: %s',s)
                continue

            p=unicode(p)
            if len(p)>settings.max_uri_length:
                log('uri too long: %s',p)
                continue

            datatype=None
            if type(o) is rdflib.Literal:
                datatype,o=convert_literal(o)
                if datatype is None or o is None:
                    continue
            else:
                o=unicode(o)
                if len(o)>settings.max_uri_length:
                    log('uri too long: %s',o)
                    continue

            if not options.dry_run: app.new_triple(s,p,o,datatype,options)

except Exception:
    traceback.print_exc()
    if not options.dry_run:
        app.save_state()
    sys.exit(1)

if not options.dry_run:
    app.cleanup(options)
    app.save_state()
