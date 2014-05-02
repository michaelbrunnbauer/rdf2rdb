
import MySQLdb
import sys
import settings
from contextlib import closing

class dbconnection(object):

    def __init__(self,dropdb=False,logsql=False, log_queries=False):
        self.logsql=logsql
        self.log_queries = log_queries
        self.conn=MySQLdb.connect(host=settings.dbhost, user=settings.dbuser, passwd=settings.dbpasswd, use_unicode=True, charset='utf8')
        if dropdb:
            self.execute('drop database '+settings.dbname)
            self.execute('create database '+settings.dbname+' default character set = utf8')
        self.execute('use '+settings.dbname)

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
