import sys,time
import settings
from log import log

if settings.dbtype=='mysql':
    import MySQLdb
    import _mysql_exceptions
elif settings.dbtype=='postgres':
    import psycopg2
    import psycopg2.extensions
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
else:
    log("Unknown database type "+settings.dbtype+" in settings.py")
    sys.exit(1)

import sys
from contextlib import closing

class dbconnection(object):

    def __init__(self,dropdb=False,logsql=False, log_queries=False):
        self.type=settings.dbtype
        self.logsql=logsql
        self.log_queries = log_queries
        self.connectiontime=str(time.time()).replace('.','_')
        self.labelcounter=0
        if self.type=='mysql':
            self.integrityerror=_mysql_exceptions.IntegrityError
            self.conn=MySQLdb.connect(host=settings.dbhost,
                                      user=settings.dbuser,
                                      passwd=settings.dbpasswd,
                                      use_unicode=True,
                                      charset='utf8')
            self.conn.autocommit(True)
        elif self.type=='postgres':
            self.integrityerror=psycopg2.IntegrityError
            self.conn=psycopg2.connect(host=settings.dbhost,
                                       database=settings.initial_dbname if dropdb else settings.dbname,
                                       user=settings.dbuser,
                                       password=settings.dbpasswd)
            self.conn.set_client_encoding('UTF8')
            self.conn.autocommit=True
        if dropdb:
            self.execute('drop database IF EXISTS '+settings.dbname)
            if self.type=='mysql':
                self.execute('create database '+settings.dbname+' default character set = utf8')
            else:
                self.execute('create database '+settings.dbname)

        if self.type=='mysql':
            self.execute('use '+settings.dbname)
        elif self.type=='postgres' and dropdb:
            self.conn.close()
            self.conn=psycopg2.connect(host=settings.dbhost,
                                       database=settings.dbname,
                                       user=settings.dbuser,
                                       password=settings.dbpasswd)
            self.conn.set_client_encoding('UTF8')
            self.conn.autocommit=True

    def getuniquelabel(self):
        self.labelcounter+=1
        return 'l_'+self.connectiontime+'_'+str(self.labelcounter)

    def sql_escape(self,s):
        s = s.replace("\\","\\\\")
        s = s.replace("'","\\'")
        s = s.replace("\"","\\\"")
        return s

    def execute(self, sql, *args, **kwds):
        if args:
            assert not kwds
        else:
            args = kwds
        if self.logsql:
            args1=[]
            for arg in args:
                args1.append("'"+self.sql_escape(unicode(arg))+"'")
            print (sql % tuple(args1)).encode(settings.outputencoding,'ignore')+';'
            sys.stdout.flush()
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(sql, args)

    def query(self, sql, *args, **kwds):
        if args:
            assert not kwds
        else:
            args = kwds
        if self.log_queries:
            args1=[]
            for arg in args:
                args1.append("'"+self.sql_escape(unicode(arg))+"'")
            print (sql % tuple(args1)).encode(settings.outputencoding,'ignore')+';'
            sys.stdout.flush()
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(sql, args)
            return tuple(cursor)
