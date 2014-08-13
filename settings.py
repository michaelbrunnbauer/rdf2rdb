from datetime import datetime as _datetime
import iso8601 as _iso8601

# set to 'mysql' or 'postgres'
dbtype='mysql'
dbhost=''
dbname='rdf2rdb'
dbuser='rdf2rdb'
dbpasswd=''

# postgresql does not allow to connect to a server without specifying a
# database and does not allow to drop the database you are connected to.
# connect to this database to drop the database specified in dbname when
# command line option --drop is specified:
initial_dbname=''

# mapping from filename extension to RDFLib format
filenameextensions={
'n3':'n3',
'nt':'nt',
}

# replace with http://<yourwebsite>/.well-known/genid/ if suitable
skolem_uri_prefix='skolem:'

functionalproperties=[
'http://www.w3.org/2000/01/rdf-schema#label',
'http://www.w3.org/2000/01/rdf-schema#comment',
]

# multilingual database schemas currently not supported
# triples with language tags not included here will be ignored
# language tag information is lost in database
allowed_language_tags=[None,'en']

# mapping from datatype URI to internal datatype
# default mapping is 'string'
datatypemap={
'http://www.w3.org/2001/XMLSchema#date':'date',
'http://www.w3.org/2001/XMLSchema#dateTime':'datetime',
'http://www.w3.org/2001/XMLSchema#double':'double precision',
'http://www.w3.org/2001/XMLSchema#float':'float',
'http://www.w3.org/2001/XMLSchema#integer':'int',
'http://www.w3.org/2001/XMLSchema#boolean':'boolean',
}

# the sql datatype specified for "string" should support this size
max_string_size=65535

def date_conversion(value):
    return value[: 10] # drop timezone if present

# TODO: handle DST correctly
def datetime_conversion(value):
    try:
        dt = _iso8601.parse_date(value)
    except TypeError:
        return None
    diff = dt - _datetime.now(_iso8601.Utc())
    return str(_datetime.now() + diff)

def string_conversion(value):
    if len(value)>max_string_size:
        return None
    return value

def bool_conversion(value):
    if value.lower() in ['true','1']:
        return '1'
    elif value.lower() in ['false','0']:
        return '0'
    else:
        return None

# mapping from internal datatype to sql datatype and value conversion function
# default is internal datatype name and unmodified value
datetime_name='datetime' if dbtype=='mysql' else 'timestamp'
sql_datatypemap={
'date': ('date', date_conversion),
'datetime': (datetime_name, datetime_conversion),
'string':('text',string_conversion),
'boolean':('boolean',bool_conversion),
}

# this tool should support IRIs and stores them as UTF8 in the database
# we need indices on IRIs and the maximum index size in MySQL is 1000 bytes
# as every UTF8 character can have up to 3 bytes we cannot support longer IRIs
max_uri_length=333

max_label_length=40

# for stdout
outputencoding='ascii'

dblabel_allowed_characters="_"

dblabel_character_mapping={
' ':'_',
'-':'_',
'.':'_',
';':'_',
':':'_',
}

rdfns=u'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
rdfsns=u'http://www.w3.org/2000/01/rdf-schema#'
owlns=u'http://www.w3.org/2002/07/owl#'

sql_reserved_words=[
"accessible","add","all","alter","analyze","and","as","asc","asensitive","before","between","bigint","binary","blob","both","by","call","cascade","case","change","char","character","check","collate","column","columns","condition","connection","constraint","continue","convert","create","cross","current_date","current_time","current_timestamp","current_user","cursor","database","databases","day_hour","day_microsecond","day_minute","day_second","dec","decimal","declare","default","delayed","delete","desc","describe","deterministic","distinct","distinctrow","div","double","drop","dual","each","else","elseif","enclosed","escaped","exists","exit","explain","false","fetch","fields","float","float4","float8","for","force","foreign","from","fulltext","goto","grant","group","having","high_priority","hour_microsecond","hour_minute","hour_second","if","ignore","in","index","infile","inner","inout","insensitive","insert","int","int1","int2","int3","int4","int8","integer","interval","into","is","iterate","join","key","keys","kill","label","leading","leave","left","like","limit","linear","lines","load","localtime","localtimestamp","lock","long","longblob","longtext","loop","low_priority","match","mediumblob","mediumint","mediumtext","middleint","minute_microsecond","minute_second","mod","modifies","natural","not","no_write_to_binlog","null","numeric","on","optimize","option","optionally","or","order","out","outer","outfile","precision","primary","privileges","procedure","purge","raid0","range","read","reads","read_only","read_write","real","references","regexp","release","rename","repeat","replace","require","restrict","return","revoke","right","rlike","schema","schemas","second_microsecond","select","sensitive","separator","set","show","smallint","soname","spatial","specific","sql","sqlexception","sqlstate","sqlwarning","sql_big_result","sql_calc_found_rows","sql_small_result","ssl","starting","straight_join","table","tables","terminated","then","tinyblob","tinyint","tinytext","to","trailing","trigger","true","undo","union","unique","unlock","unsigned","update","upgrade","usage","use","using","utc_date","utc_time","utc_timestamp","values","varbinary","varchar","varcharacter","varying","when","where","while","with","write","x509","xor","year_month","zerofill","user"
]
