
import settings
from datatype import getsqldatatypename
import database
import copy

class dbclass(object):
    def __init__(self,uri):
        self.uri=uri
        self.nextid=1

    def get_uri_id(self,uri,db):
        for row in db.query('select id from uris where uri=%s AND class=%s',uri,self.uri):
            return row[0]
        return None

    def insert_uri(self,uri,dblabels,db):
        tb=dblabels[self.uri]
        pk=tb+"_id"
        oid=self.nextid
        self.nextid+=1
        db.execute('insert into '+tb+' ('+pk+',uri) values (%s,%s)',oid,uri)
        db.execute('insert into uris (uri,class,id) values (%s,%s,%s)',uri,self.uri,oid)
        return oid

    def delete_id(self,id,dblabels,db):
        tb=dblabels[self.uri]
        pk=tb+"_id"
        db.execute('delete from '+tb+' where '+pk+'=%s',id)
        db.execute('delete from uris where id=%s AND class=%s',id,self.uri)

    def create_table(self,dblabels,db):
        tb=dblabels[self.uri]
        pk=tb+"_id"
        uritype='varchar('+str(settings.max_uri_length)+') binary not null'
        db.execute('create table '+tb+' ('+pk+' int not null, primary key ('+pk+'), uri '+uritype+')')

    def rename_table(self,dblabels,oldlabels,db):
        tb=dblabels[self.uri]
        pk=tb+"_id"
        tb_old=oldlabels[self.uri]
        pk_old=tb_old+"_id"
        db.execute('alter table '+tb_old+' change column '+pk_old+' '+pk+' int not null')
        db.execute('alter table '+tb_old+' rename to '+tb)

    def drop_table(self,dblabels,db):
        tb=dblabels[self.uri]
        db.execute('drop table '+tb)

class dbdatatypeproperty(object):
    def __init__(self,classuri,propertyuri,datatype,onlydatatype,staysfunctional):
        self.classuri=classuri
        self.propertyuri=propertyuri
        self.datatype=datatype
        self.sql_datatype=getsqldatatypename(self.datatype)
        self.isfunctional=True
        self.onlydatatype=onlydatatype
        self.staysfunctional=staysfunctional

    def get_all_values(self,dblabels,db):
        if self.isfunctional:
            tb=dblabels[self.classuri]
            field=dblabels[self.propertyuri]
            if not self.onlydatatype:
                field+='_'+self.datatype
            pk=tb+"_id"
            sql='select '+pk+','+field+' from '+tb+' where '+field+' is not NULL'
        else:
            tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
            field1=dblabels[self.classuri]+'_id'
            field2=dblabels[self.propertyuri]
            sql='select '+field1+','+field2+' from '+tb
        rueck=[]
        for row in db.query(sql):
            rueck.append((row[0],row[1]))
        return rueck

    def delete_id(self,id,dblabels,db):
        assert not self.isfunctional
        tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
        field1=dblabels[self.classuri]+'_id'
        db.execute('delete from '+tb+' where '+field1+'=%s',id)

    def drop_if_empty(self,dblabels,db):
        assert not self.isfunctional
        tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
        field=dblabels[self.classuri]+'_id'
        found=False
        for row in db.query('select '+field+' from '+tb+' limit 1'):
            found=True
        if not found:
            db.execute('drop table '+tb)
            return True
        return False

    def has_value(self,id,value,dblabels,db):
        if self.isfunctional:
            tb=dblabels[self.classuri]
            field=dblabels[self.propertyuri]
            if not self.onlydatatype:
                field+='_'+self.datatype
            pk=tb+"_id"
            sql='select '+field+' from '+tb+' where '+pk+'=%s AND '+field+'=%s'
        else:
            tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
            field1=dblabels[self.classuri]+'_id'
            field2=dblabels[self.propertyuri]
            sql='select '+field1+' from '+tb+' where '+field1+'=%s AND '+field2+'=%s'
        for row in db.query(sql,id,value):
            return True
        return False

    def get_values(self,id,dblabels,db):
        if self.isfunctional:
            tb=dblabels[self.classuri]
            field=dblabels[self.propertyuri]
            if not self.onlydatatype:
                field+='_'+self.datatype
            pk=tb+"_id"
            sql='select '+field+' from '+tb+' where '+pk+'=%s'
            rows=db.query(sql,id)
            assert len(rows)==1
            if rows[0][0] is None:
                return []
            return [rows[0][0]]
        else:
            tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
            field1=dblabels[self.classuri]+'_id'
            field2=dblabels[self.propertyuri]
            rueck=[]
            for row in db.query('select '+field2+' from '+tb+' where '+field1+'=%s',id):
                rueck.append(row[0])
            return rueck

    # returns False if property has to be made non-functional
    def insert_value(self,id,value,dblabels,db):
        if self.has_value(id,value,dblabels,db): return True
        if self.isfunctional:
            oldvalues=self.get_values(id,dblabels,db)
            if oldvalues:
                if self.staysfunctional:
                    return True
                return False
            tb=dblabels[self.classuri]
            field=dblabels[self.propertyuri]
            if not self.onlydatatype:
                field+='_'+self.datatype
            pk=tb+"_id"
            db.execute('update '+tb+' set '+field+'=%s where '+pk+'=%s',value,id)
        else:
            tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
            field1=dblabels[self.classuri]+'_id'
            field2=dblabels[self.propertyuri]
            db.execute('insert into '+tb+' ('+field1+','+field2+') values (%s,%s)',id,value)
        return True

    def create(self,dblabels,db):
        assert self.isfunctional
        tb=dblabels[self.classuri]
        field=dblabels[self.propertyuri]
        if not self.onlydatatype:
            field+='_'+self.datatype
        db.execute('alter table '+tb+' add column '+field+' '+self.sql_datatype)

    def drop(self,dblabels,db):
        if self.isfunctional:
            tb=dblabels[self.classuri]
            field=dblabels[self.propertyuri]
            if not self.onlydatatype:
                field+='_'+self.datatype
            db.execute('alter table '+tb+' drop column '+field)
        else:
            tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
            db.execute('drop table '+tb)

    def notonlydatatype(self,dblabels,db):
        assert self.isfunctional
        tb=dblabels[self.classuri]
        field=dblabels[self.propertyuri]
        db.execute('alter table '+tb+' change column '+field+' '+field+'_'+self.datatype+' '+self.sql_datatype)

    def make_functional(self,dblabels,db):
        tb=dblabels[self.classuri]
        field=dblabels[self.propertyuri]
        pk=tb+"_id"
        tb_old=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
        field1_old=dblabels[self.classuri]+'_id'
        field2_old=dblabels[self.propertyuri]
        if not self.onlydatatype:
            field+='_'+self.datatype
        db.execute('alter table '+tb+' add column '+field+' '+self.sql_datatype)
        rueck=[]
        for row in db.query('select '+field1_old+','+field2_old+' from '+tb_old):
            id=row[0]
            value=row[1]
            db.execute('update '+tb+' set '+field+'=%s where '+pk+'=%s',value,id)
        db.execute('drop table '+tb_old)

    def make_nonfunctional(self,dblabels,db):
        tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
        field1=dblabels[self.classuri]+'_id'
        field2=dblabels[self.propertyuri]
        db.execute('create table '+tb+' ('+field1+' int not null, '+field2+' '+self.sql_datatype+',index '+field1+'_i ('+field1+'))')
        tb_old=dblabels[self.classuri]
        field_old=dblabels[self.propertyuri]
        if not self.onlydatatype:
            field_old+='_'+self.datatype
        db.execute('insert into '+tb+' select '+field1+','+field_old+' from '+tb_old+' where '+field_old+' is not NULL')
        db.execute('alter table '+tb_old+' drop column '+field_old)

    def rename(self,dblabels,oldlabels,db):
        if self.isfunctional:
            tb=dblabels[self.classuri]
            field=dblabels[self.propertyuri]
            field_old=oldlabels[self.propertyuri]
            if not self.onlydatatype:
                field+='_'+self.datatype
                field_old+='_'+self.datatype
            db.execute('alter table '+tb+' change column '+field_old+' '+field+' '+self.sql_datatype)
        else:
            tb=dblabels[self.classuri]+'_'+dblabels[self.propertyuri]+'_'+self.datatype
            field1=dblabels[self.classuri]+'_id'
            field2=dblabels[self.propertyuri]
            tb_old=oldlabels[self.classuri]+'_'+oldlabels[self.propertyuri]+'_'+self.datatype
            field1_old=oldlabels[self.classuri]+'_id'
            field2_old=oldlabels[self.propertyuri]
            db.execute('alter table '+tb_old+' change column '+field1_old+' '+field1+' int not null')
            db.execute('alter table '+tb_old+' change column '+field2_old+' '+field2+' '+self.sql_datatype)
            db.execute('alter table '+tb_old+' rename to '+tb)

class dbobjectproperty(object):
    def __init__(self,subjectclassuri,propertyuri,objectclassuri):
        self.subjectclassuri=subjectclassuri
        self.propertyuri=propertyuri
        self.objectclassuri=objectclassuri

    def ids_for_subject(self,id,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field1=dblabels[self.subjectclassuri]+'_id1'
        field2=dblabels[self.objectclassuri]+'_id2'
        rueck=[]
        for row in db.query('select '+field2+' from '+tb+' where '+field1+'=%s',id):
            rueck.append(row[0])
        return rueck

    def get_ids(self,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field1=dblabels[self.subjectclassuri]+'_id1'
        field2=dblabels[self.objectclassuri]+'_id2'
        rueck=[]
        for row in db.query('select '+field1+','+field2+' from '+tb):
            rueck.append((row[0],row[1]))
        return rueck

    def get_subjectids(self,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field1=dblabels[self.subjectclassuri]+'_id1'
        rueck=[]
        for row in db.query('select '+field1+' from '+tb):
            rueck.append(row[0])
        return rueck

    def get_objectids(self,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field2=dblabels[self.objectclassuri]+'_id2'
        rueck=[]
        for row in db.query('select '+field2+' from '+tb):
            rueck.append(row[0])
        return rueck

    def ids_for_object(self,id,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field1=dblabels[self.subjectclassuri]+'_id1'
        field2=dblabels[self.objectclassuri]+'_id2'
        rueck=[]
        for row in db.query('select '+field1+' from '+tb+' where '+field2+'=%s',id):
            rueck.append(row[0])
        return rueck

    def connect(self,id1,id2,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field1=dblabels[self.subjectclassuri]+'_id1'
        field2=dblabels[self.objectclassuri]+'_id2'
        db.execute('insert ignore into '+tb+' ('+field1+','+field2+') values (%s,%s)',id1,id2)

    def delete_subject_id(self,id,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field=dblabels[self.subjectclassuri]+'_id1'
        db.execute('delete from '+tb+' where '+field+'=%s',id)

    def delete_object_id(self,id,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field=dblabels[self.objectclassuri]+'_id2'
        db.execute('delete from '+tb+' where '+field+'=%s',id)

    def create_table(self,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field1=dblabels[self.subjectclassuri]+'_id1'
        field2=dblabels[self.objectclassuri]+'_id2'
        db.execute('create table '+tb+' ('+field1+' int not null, '+field2+' int not null,primary key ('+field1+','+field2+'),index '+field2+'_i ('+field2+'))')

    def drop_if_empty(self,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field=dblabels[self.subjectclassuri]+'_id1'
        found=False
        for row in db.query('select '+field+' from '+tb+' limit 1'):
            found=True
        if not found:
            db.execute('drop table '+tb)
            return True
        return False

    def drop_table(self,dblabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        db.execute('drop table '+tb)

    def rename_table(self,dblabels,oldlabels,db):
        tb=dblabels[self.subjectclassuri]+'_'+dblabels[self.propertyuri]+'_'+dblabels[self.objectclassuri]
        field1=dblabels[self.subjectclassuri]+'_id1'
        field2=dblabels[self.objectclassuri]+'_id2'
        tb_old=oldlabels[self.subjectclassuri]+'_'+oldlabels[self.propertyuri]+'_'+oldlabels[self.objectclassuri]
        field1_old=oldlabels[self.subjectclassuri]+'_id1'
        field2_old=oldlabels[self.objectclassuri]+'_id2'
        db.execute('alter table '+tb_old+' change column '+field1_old+' '+field1+' int not null')
        db.execute('alter table '+tb_old+' change column '+field2_old+' '+field2+' int not null')
        db.execute('alter table '+tb_old+' rename to '+tb)

class dbschema(object):
    def __init__(self,db):
        self.db=db
        self.dblabels={} # uri -> db label
        self.dblabels1={} # uri -> rdfs:label, db label,suffixcount
        self.class_tables={} # uri -> dbclass
        self.datatypeproperty_tables={} # propertyuri,classuri -> datatype -> dbdatatypeproperty
        self.objectproperty_tables={} # propertyuri,subjectclassuri,objectclassuri -> dbobjectproperty
        self.nextbnodesuffix=1

    def copy(self,new_class=False,new_datatypeproperty=False,new_objectproperty=False):
        c=dbschema(self.db)
        c.dblabels=dict(self.dblabels)
        # we don't need dblabels1 in these copies
        if new_class:
            c.class_tables=dict(self.class_tables)
        else:
            c.class_tables=self.class_tables
        if new_datatypeproperty:
            # datatypeproperties are the only objects that get changed at runtime so we need a deep copy here
            c.datatypeproperty_tables=copy.deepcopy(self.datatypeproperty_tables)
        else:
            c.datatypeproperty_tables=self.datatypeproperty_tables
        if new_objectproperty:
            c.objectproperty_tables=dict(self.objectproperty_tables)
        else:
            c.objectproperty_tables=self.objectproperty_tables
        c.nextbnodesuffix=self.nextbnodesuffix
        return c

    def validsqlname(self,text):
        def hasalpha(s):
            for c in s:
                if c.isalnum() and not c.isdigit(): return True
            return False
        text=text.lower().encode('ascii','ignore')
        text1=''
        for c in text:
            if c in settings.dblabel_character_mapping:
                c=settings.dblabel_character_mapping[c]
            elif not c.isalnum() and c not in settings.dblabel_allowed_characters:
                continue
            text1+=c
        while text1.startswith('_'): text1=text1[1:]
        while text1.endswith('_'): text1=text1[:-1]
        if not hasalpha(text1): return ''
        return text1[:settings.max_label_length-3]

    def generatedblabel(self,uri,label=None):
        if label:
            label=self.validsqlname(label)
            if label: return label
        if '#' in uri and not uri.endswith('#'):
            label=self.validsqlname(uri.split('#')[-1])
            if label: return label
        label=self.validsqlname(uri.split('/')[-1])
        if label: return label
        return 'dummylabel'

    def rename(self,uri,dblabels,oldlabels):

        if uri in self.class_tables:
            self.class_tables[uri].rename_table(dblabels,oldlabels,self.db)

        for datatypes in self.datatypeproperty_tables.values():
            for datatypeproperty in datatypes.values():
                if uri==datatypeproperty.classuri or uri==datatypeproperty.propertyuri:
                    datatypeproperty.rename(dblabels,oldlabels,self.db)

        for objectproperty in self.objectproperty_tables.values():
            if uri==objectproperty.subjectclassuri or uri==objectproperty.propertyuri or uri==objectproperty.objectclassuri:
                objectproperty.rename_table(dblabels,oldlabels,self.db)

    def hasnamecollision(self):
        class_labels=set(['uris','labels'])
        field_labels={}
        property_labels=set()

        for uri in self.class_tables:
            if self.dblabels[uri] in settings.sql_reserved_words:
                return True
            if self.dblabels[uri] in class_labels:
                return True
            class_labels.add(self.dblabels[uri])
            field_labels[uri]=set()
            field_labels[uri].add(self.dblabels[uri]+'_id')
            field_labels[uri].add('uri')

        for datatypes in self.datatypeproperty_tables.values():
            for datatypeproperty in datatypes.values():
                if datatypeproperty.isfunctional:
                    label=self.dblabels[datatypeproperty.propertyuri]
                    if not datatypeproperty.onlydatatype:
                        label+='_'+datatypeproperty.datatype
                    if label in settings.sql_reserved_words:
                        return True
                    if label in field_labels[datatypeproperty.classuri]:
                        return True
                    field_labels[datatypeproperty.classuri].add(label)
                else:
                    label=self.dblabels[datatypeproperty.classuri]+'_'
                    label+=self.dblabels[datatypeproperty.propertyuri]+'_'
                    label+=datatypeproperty.datatype
                    if label in property_labels:
                        return True
                    property_labels.add(label)
                    if self.dblabels[datatypeproperty.propertyuri] in settings.sql_reserved_words:
                        return True
                    if self.dblabels[datatypeproperty.classuri]+'_id'==self.dblabels[datatypeproperty.propertyuri]:
                        return True

        for objectproperty in self.objectproperty_tables.values():
            label=self.dblabels[objectproperty.subjectclassuri]+'_'
            label+=self.dblabels[objectproperty.propertyuri]+'_'
            label+=self.dblabels[objectproperty.objectclassuri]
            if label in property_labels:
                return True
            property_labels.add(label)
        return False

    def check_label(self,uri,oldschema,rdfslabel=None):
        oldrdfslabel,oldlabel,suffixcount=self.dblabels1.get(uri,(None,None,0))
        assert not (oldrdfslabel and rdfslabel)

        if not oldlabel: # new property/class. check for rdfs:label already seen
            assert not rdfslabel and not oldrdfslabel
            found=False
            for classuri,id in self.get_class_ids_from_uri(uri):
                if found: break
                k=(settings.rdfsns+'label',classuri)
                if k not in self.datatypeproperty_tables: continue
                datatypes=self.datatypeproperty_tables[k]
                for datatype,datatypeproperty in datatypes.items():
                    if found: break
                    if datatype!='string': continue
                    for value in datatypeproperty.get_values(id,self.dblabels,self.db):
                        rdfslabel=value
                        found=True
                        break

        if rdfslabel:
            dblabel=self.generatedblabel(uri,label=rdfslabel)
            suffixcount=0
        elif oldlabel:
            if not self.hasnamecollision():
                return
            dblabel=oldlabel
            suffixcount+=1
        else:
            # nice default labels for skolemized blank nodes
            if uri.startswith(settings.skolem_uri_prefix):
                dblabel=self.generatedblabel(uri,label='bnode')
                suffixcount=self.nextbnodesuffix
                self.nextbnodesuffix+=1
            else:
                dblabel=self.generatedblabel(uri)

        while suffixcount<100:
            if suffixcount:
                suffix=str(suffixcount)
            else:
                suffix=''
            suffixcount+=1
            self.dblabels[uri]=dblabel+suffix
            self.dblabels1[uri]=(rdfslabel,dblabel,suffixcount-1)
            if not self.hasnamecollision():
                break
        assert suffixcount<100

        if oldlabel:
            if oldschema.dblabels[uri]!=self.dblabels[uri]:
                oldschema.rename(uri,self.dblabels,oldschema.dblabels)
                self.db.execute('update labels set dblabel=%s where uri=%s',self.dblabels[uri],uri)
        else:
            self.db.execute('insert into labels (uri,dblabel) values (%s,%s)',uri,self.dblabels[uri])

    def set_label(self,uri,label):
        assert uri in self.dblabels
        oldschema=self.copy()
        self.check_label(uri,oldschema,rdfslabel=label)

    def get_class(self,uri):
        if uri in self.class_tables:
            return self.class_tables[uri]
        oldschema=self.copy(new_class=True)
        c=dbclass(uri)
        self.class_tables[uri]=c
        self.check_label(uri,oldschema)
        c.create_table(self.dblabels,self.db)
        return self.class_tables[uri]

    def get_datatypeproperty(self,classuri,propertyuri,datatype,staysfunctional):
        assert classuri in self.class_tables
        k=(propertyuri,classuri)
        if k in self.datatypeproperty_tables:
            if datatype in self.datatypeproperty_tables[k]:
                return self.datatypeproperty_tables[k][datatype]
        else:
            self.datatypeproperty_tables[k]={}
        datatypes=self.datatypeproperty_tables[k]
        oldschema=self.copy(new_datatypeproperty=True)
        onlydatatype=False
        first_datatype=None
        if not len(datatypes):
            onlydatatype=True
        elif len(datatypes)==1:
            first_datatype=datatypes.values()[0]
            first_datatype.onlydatatype=False
        p=dbdatatypeproperty(classuri,propertyuri,datatype,onlydatatype,staysfunctional)
        datatypes[datatype]=p
        self.check_label(propertyuri,oldschema)
        if first_datatype:
            first_datatype.notonlydatatype(self.dblabels,self.db)
        p.create(self.dblabels,self.db)
        return p

    def datatypeproperty_safe_insert(self,datatypeproperty,id,value):
        if not datatypeproperty.insert_value(id,value,self.dblabels,self.db):
            oldschema=self.copy(new_datatypeproperty=True)
            datatypeproperty.isfunctional=False
            self.check_label(datatypeproperty.propertyuri,oldschema)
            datatypeproperty.make_nonfunctional(self.dblabels,self.db)
            assert datatypeproperty.insert_value(id,value,self.dblabels,self.db)

    def get_objectproperty(self,subjectclassuri,propertyuri,objectclassuri):
        assert subjectclassuri in self.class_tables
        assert objectclassuri in self.class_tables
        k=(propertyuri,subjectclassuri,objectclassuri)
        if k in self.objectproperty_tables:
            return self.objectproperty_tables[k]
        oldschema=self.copy(new_objectproperty=True)
        p=dbobjectproperty(subjectclassuri,propertyuri,objectclassuri)
        self.objectproperty_tables[k]=p
        self.check_label(propertyuri,oldschema)
        p.create_table(self.dblabels,self.db)
        return p

    def has_a_class(self,uri):
        for row in self.db.query('select class from uris where uri=%s limit 1',uri):
            return True
        return False

    def get_class_ids_from_uri(self,uri):
        for row in self.db.query('select class,id from uris where uri=%s',uri):
            classuri=row[0]
            if type(classuri) is str: # mysql module binary collation bug
                classuri=unicode(classuri,'utf8')
            yield (classuri,row[1])

    def get_uri_from_class_id(self,classuri,id):
        for row in self.db.query('select uri from uris where id=%s AND class=%s',id,classuri):
            uri=row[0]
            if type(uri) is str: # mysql module binary collation bug
                uri=unicode(uri,'utf8')
            return uri
        assert False

    def get_uris_from_class(self,classuri):
        for row in self.db.query('select uri from uris where class=%s',classuri):
            uri=row[0]
            if type(uri) is str: # mysql module binary collation bug
                uri=unicode(uri,'utf8')
            yield uri

    def delete_tables_with(self,classuri):
        if classuri not in self.class_tables: return
        self.class_tables[classuri].drop_table(self.dblabels,self.db)
        del self.class_tables[classuri]
        for datatypes in self.datatypeproperty_tables.values():
            for datatype,datatypeproperty in datatypes.items():
                if datatypeproperty.classuri!=classuri: continue
                if not datatypeproperty.isfunctional:
                    datatypeproperty.drop(self.dblabels,self.db)
                del datatypes[datatype]
        for k,objectproperty in self.objectproperty_tables.items():
            if objectproperty.subjectclassuri==classuri or objectproperty.objectclassuri==classuri:
                objectproperty.drop_table(self.dblabels,self.db)
                del self.objectproperty_tables[k]

    def delete_thing(self,classuri,id):
        cl=self.get_class(classuri)
        cl.delete_id(id,self.dblabels,self.db)
        for (propertyuri,classuri1),datatypes in self.datatypeproperty_tables.items():
            if classuri1!=classuri: continue
            for datatype,datatypeproperty in datatypes.items():
                if datatypeproperty.isfunctional: continue
                datatypeproperty.delete_id(id,self.dblabels,self.db)
        for (propertyuri,subjectclassuri,objectclassuri),objectproperty in self.objectproperty_tables.items():
            if classuri==subjectclassuri:
                objectproperty.delete_subject_id(id,self.dblabels,self.db)
            if classuri==objectclassuri:
                objectproperty.delete_object_id(id,self.dblabels,self.db)

    def drop_empty_properties(self):
        for (propertyuri,classuri),datatypes in self.datatypeproperty_tables.items():
            for datatype,datatypeproperty in datatypes.items():
                if datatypeproperty.isfunctional: continue
                if datatypeproperty.drop_if_empty(self.dblabels,self.db):
                    del datatypes[datatype]
        for k,objectproperty in self.objectproperty_tables.items():
            if objectproperty.drop_if_empty(self.dblabels,self.db):
                del self.objectproperty_tables[k]

    def remove_redundant_things(self):
        thingclass=self.get_class(settings.owlns+'Thing')
        for uri in self.get_uris_from_class(settings.owlns+'Thing'):
            for classuri,id in self.get_class_ids_from_uri(uri):
                if classuri!=settings.owlns+'Thing':
                    id_as_thing=thingclass.get_uri_id(uri,self.db)
                    assert id_as_thing
                    self.delete_thing(settings.owlns+'Thing',id_as_thing)
                    break
        self.drop_empty_properties()
