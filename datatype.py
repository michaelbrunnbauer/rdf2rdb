
import settings
import rdflib

# return internal datatype name and converted value (None if constraints are violated)
def convert_literal(literal):
    assert type(literal) is rdflib.Literal
    dt=str(literal.datatype)
    if dt in settings.datatypemap:
        datatype=settings.datatypemap[dt]
    else:
        datatype='string'
    if literal.datatype is None and literal.language not in settings.allowed_language_tags:
        return None,None
    value=unicode(literal)
    if datatype in settings.sql_datatypemap:
        return datatype,settings.sql_datatypemap[datatype][1](value)
    return datatype,value

# return sql datatype name for internal datatype name
def getsqldatatypename(datatype):
    if datatype in settings.sql_datatypemap:
        return settings.sql_datatypemap[datatype][0]
    return datatype
