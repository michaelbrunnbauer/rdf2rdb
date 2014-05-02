
from dbschema import dbschema
from rdfschema import rdfschema
from log import log
import database
import rdflib
import pickle
import settings
import sys,os
import _mysql_exceptions

version=0.52

rdfns=settings.rdfns
rdfsns=settings.rdfsns
owlns=settings.owlns

def init(conf):
    global dbs,rdfs,db,dblabels
    try:
        f=open('rdf2rdb.pickle','r')
        if conf.drop_database:
            log('rdf2rdb.pickle found but --drop specified, exiting')
            sys.exit(1)
        log('loading state from rdf2rdb.pickle')
        (pversion,dbs,rdfs)=pickle.load(f)
        assert int(pversion) <= version
        f.close()
        db=database.dbconnection(logsql=conf.with_sql_logging)
        dbs.db=db
        try:
            db.query('select uri from uris limit 1')
            db.query('select uri from labels limit 1')
        except _mysql_exceptions.ProgrammingError:
            log('rdf2rdb.pickle found but database tables are missing, exiting')
            sys.exit(1)
        os.unlink('rdf2rdb.pickle')
    except IOError:
        db=database.dbconnection(dropdb=conf.drop_database,logsql=conf.with_sql_logging, log_queries=conf.with_sql_query_logging)
        uritype='varchar('+str(settings.max_uri_length)+') binary not null'
        # no primary key due to key length restrictions
        db.execute('create table uris (uri '+uritype+',class '+uritype+',id int,index uri_i (uri),index id_i (id),index class_i (class))')
        db.execute('create table labels (uri '+uritype+',dblabel varchar('+str(settings.max_label_length)+') not null,primary key (uri),index dblabel_i (dblabel))')
        dbs=dbschema(db)
        rdfs=rdfschema()
    dblabels=dbs.dblabels

def insert_into_class(uri,classuri):
    insert_into_class1(uri,classuri)
    for classuri1 in rdfs.superclasses.get(classuri,set()):
        insert_into_class1(uri,classuri1)

def insert_into_class1(uri,classuri):
    cl=dbs.get_class(classuri)
    id=cl.get_uri_id(uri,db)
    if id:
        return
    id=cl.insert_uri(uri,dblabels,db)

    # new uri in this class. copy properties in domain/range from other classes of this uri
    for classuri1,id1 in dbs.get_class_ids_from_uri(uri):
        if classuri1==classuri: continue

        # copy datatypeproperties
        for (propertyuri,classuri2),datatypes in dbs.datatypeproperty_tables.items():
            if classuri2!=classuri1: continue
            if propertyuri in rdfs.domains and classuri not in rdfs.domains[propertyuri]:
                continue
            for datatype,datatypeproperty1 in datatypes.items():
                for value in datatypeproperty1.get_values(id1,dblabels,db):
                    staysfunctional=propertyuri in rdfs.functionalproperties
                    datatypeproperty=dbs.get_datatypeproperty(classuri,propertyuri,datatype,staysfunctional)
                    dbs.datatypeproperty_safe_insert(datatypeproperty,id,value)

        # copy objectproperties
        for (propertyuri,subjectclassuri,objectclassuri),objectproperty1 in dbs.objectproperty_tables.items():
            if classuri1==subjectclassuri and (propertyuri not in rdfs.domains or classuri in rdfs.domains[propertyuri]):
                for id2 in objectproperty1.ids_for_subject(id1,dblabels,db):
                    objectproperty=dbs.get_objectproperty(classuri,propertyuri,objectclassuri)
                    objectproperty.connect(id,id2,dblabels,db)
                    if classuri1==objectclassuri and id2==id1 and (propertyuri not in rdfs.ranges or classuri in rdfs.ranges[propertyuri]):
                        objectproperty=dbs.get_objectproperty(classuri,propertyuri,classuri)
                        objectproperty.connect(id,id,dblabels,db)
            if classuri1==objectclassuri and (propertyuri not in rdfs.ranges or classuri in rdfs.ranges[propertyuri]):
                for id2 in objectproperty1.ids_for_object(id1,dblabels,db):
                    objectproperty=dbs.get_objectproperty(subjectclassuri,propertyuri,classuri)
                    objectproperty.connect(id2,id,dblabels,db)

def subpropertyof(subj,obj):
    if subj in rdfs.superproperties and obj in rdfs.superproperties[subj]:
        return
    new=rdfs.addsuperproperty(subj,obj)
    for subj,obj in new:
        if obj in rdfs.domains:
            for domain in rdfs.domains[obj]:
                rdfsdomain(subj,domain)
        if obj in rdfs.ranges:
            for range in rdfs.ranges[obj]:
                rdfsrange(subj,range)
    for subj,obj in new:
        subpropertyof1(subj,obj)

def subpropertyof1(subj,obj):
    for datatypes in dbs.datatypeproperty_tables.values():
        for datatype,datatypeproperty in datatypes.items():
            if datatypeproperty.propertyuri!=subj: continue
            for id,value in datatypeproperty.get_all_values(dblabels,db):
                uri=dbs.get_uri_from_class_id(datatypeproperty.classuri,id)
                for classuri,id in dbs.get_class_ids_from_uri(uri):
                    if obj not in rdfs.domains or classuri in rdfs.domains[obj]:
                        staysfunctional=obj in rdfs.functionalproperties
                        datatypeproperty1=dbs.get_datatypeproperty(classuri,obj,datatype,staysfunctional)
                        dbs.datatypeproperty_safe_insert(datatypeproperty1,id,value)

    for objectproperty in dbs.objectproperty_tables.values():
        if objectproperty.propertyuri!=subj: continue
        for id1,id2 in objectproperty.get_ids(dblabels,db):
            suri=dbs.get_uri_from_class_id(objectproperty.subjectclassuri,id1)
            ouri=dbs.get_uri_from_class_id(objectproperty.objectclassuri,id2)
            for sclassuri,id1 in dbs.get_class_ids_from_uri(suri):
                if obj not in rdfs.domains or sclassuri in rdfs.domains[obj]:
                    for oclassuri,id2 in dbs.get_class_ids_from_uri(ouri):
                        if obj not in rdfs.ranges or oclassuri in rdfs.ranges[obj]:
                            objectproperty1=dbs.get_objectproperty(sclassuri,obj,oclassuri)
                            objectproperty1.connect(id1,id2,dblabels,db)

def rdfsdomain(subj,obj):
    if subj in rdfs.domains and obj in rdfs.domains[subj]: return
    rdfs.adddomain(subj,obj)
    for datatypes in dbs.datatypeproperty_tables.values():
        for datatypeproperty in datatypes.values():
            if datatypeproperty.propertyuri!=subj: continue
            for id,pvalue in datatypeproperty.get_all_values(dblabels,db):
                uri=dbs.get_uri_from_class_id(datatypeproperty.classuri,id)
                insert_into_class(uri,obj)
            if datatypeproperty.classuri not in rdfs.domains[subj]:
                datatypeproperty.drop(dblabels,db)
                del datatypes[datatypeproperty.datatype]
    for k,objectproperty in dbs.objectproperty_tables.items():
        if objectproperty.propertyuri!=subj: continue
        for id in objectproperty.get_subjectids(dblabels,db):
            uri=dbs.get_uri_from_class_id(objectproperty.subjectclassuri,id)
            insert_into_class(uri,obj)
        if objectproperty.subjectclassuri not in rdfs.domains[subj]:
            objectproperty.drop_table(dblabels,db)
            del dbs.objectproperty_tables[k]

def rdfsrange(subj,obj):
    if subj in rdfs.ranges and obj in rdfs.ranges[subj]: return
    rdfs.addrange(subj,obj)
    for k,objectproperty in dbs.objectproperty_tables.items():
        if objectproperty.propertyuri!=subj: continue
        for id in objectproperty.get_objectids(dblabels,db):
            uri=dbs.get_uri_from_class_id(objectproperty.objectclassuri,id)
            insert_into_class(uri,obj)
        if objectproperty.objectclassuri not in rdfs.ranges[subj]:
            objectproperty.drop_table(dblabels,db)
            del dbs.objectproperty_tables[k]

def new_triple(subj,pred,obj,datatype,conf):
    preds=rdfs.superproperties.get(pred,set())
    preds.add(pred)
    for pred in preds:
        new_triple1(subj,pred,obj,datatype,conf)

def new_triple1(subj,pred,obj,datatype,conf):
    tbox=False

    if pred==rdfns+'type':
        tbox=True
        if not datatype:
            if conf.entailment and obj==owlns+'FunctionalProperty':
                rdfs.functionalproperties.add(subj)
                for datatypes in dbs.datatypeproperty_tables.values():
                    for datatypeproperty in datatypes.values():
                        if datatypeproperty.propertyuri!=subj: continue
                        datatypeproperty.staysfunctional=True
                        if datatypeproperty.isfunctional: continue
                        oldschema=dbs.copy(new_datatypeproperty=True)
                        datatypeproperty.isfunctional=True
                        dbs.check_label(datatypeproperty.propertyuri,oldschema)
                        datatypeproperty.make_functional(dblabels,db)
            if conf.with_tbox or (not obj.startswith(rdfsns) and not obj.startswith(rdfns) and not obj.startswith(owlns)):
                insert_into_class(subj,obj)

    elif pred==rdfsns+'label' and subj in dblabels:
        tbox=True
        if datatype=='string':
            rdfslabel,dblabel,suffixcount=dbs.dblabels1.get(subj)
            if not rdfslabel:
                dbs.set_label(subj,obj)

    elif pred==rdfsns+'domain':
        tbox=True
        if conf.entailment and not datatype and obj!=owlns+'Thing':
            rdfsdomain(subj,obj)
            if subj in rdfs.subproperties:
                for prop in rdfs.subproperties[subj]:
                    rdfsdomain(prop,obj)

    elif pred==rdfsns+'range':
        tbox=True
        if conf.entailment and not datatype and obj!=owlns+'Thing':
            rdfsrange(subj,obj)
            if subj in rdfs.subproperties:
                for prop in rdfs.subproperties[subj]:
                    rdfsrange(prop,obj)

    elif pred==rdfsns+'subClassOf':
        tbox=True
        if conf.entailment and not datatype:
            if subj not in rdfs.superclasses or obj not in rdfs.superclasses[subj]:
                for cl,superclass in rdfs.addsuperclass(subj,obj):
                    for uri in dbs.get_uris_from_class(cl):
                        insert_into_class(uri,superclass)

    elif pred==owlns+'equivalentClass':
        tbox=True
        if conf.entailment and not datatype:
            if subj not in rdfs.superclasses or obj not in rdfs.superclasses[subj]:
                for cl,superclass in rdfs.addsuperclass(subj,obj):
                    for uri in dbs.get_uris_from_class(cl):
                        insert_into_class(uri,superclass)
            if obj not in rdfs.superclasses or subj not in rdfs.superclasses[obj]:
                for cl,superclass in rdfs.addsuperclass(obj,subj):
                    for uri in dbs.get_uris_from_class(cl):
                        insert_into_class(uri,superclass)

    elif pred==rdfsns+'subPropertyOf':
        tbox=True
        if conf.entailment and not datatype:
            subpropertyof(subj,obj)

    elif pred==owlns+'equivalentProperty':
        tbox=True
        if conf.entailment and not datatype:
            subpropertyof(subj,obj)
            subpropertyof(obj,subj)

    elif pred==owlns+'sameAs':
        tbox=True
        if conf.entailment and not datatype:
            # FIXME: currently not used
            rdfs.addsameas(subj,obj)

    if tbox and not conf.with_tbox:
        return

    # generate rdfs:domain entailments
    if pred in rdfs.domains:
        for classuri in rdfs.domains[pred]:
            insert_into_class(subj,classuri)

    # generate rdfs:range entailments
    if not datatype and pred in rdfs.ranges:
        for classuri in rdfs.ranges[pred]:
            insert_into_class(obj,classuri)

    # if we don't know a class for the subject we put it in owl:Thing
    if not dbs.has_a_class(subj):
        insert_into_class(subj,owlns+'Thing')

    inserted=False

    if datatype:
        # literal as object
        for classuri,id in dbs.get_class_ids_from_uri(subj):
            if pred in rdfs.domains and classuri not in rdfs.domains[pred]:
                continue
            staysfunctional=pred in rdfs.functionalproperties
            datatypeproperty=dbs.get_datatypeproperty(classuri,pred,datatype,staysfunctional)
            dbs.datatypeproperty_safe_insert(datatypeproperty,id,obj)
            inserted=True
    else:
        # URI as object
        # if we don't know a class for the object we put it in owl:Thing
        if not dbs.has_a_class(obj):
            insert_into_class(obj,owlns+'Thing')

        for classuri1,id1 in dbs.get_class_ids_from_uri(subj):
            if pred in rdfs.domains and classuri1 not in rdfs.domains[pred]:
                continue
            for classuri2,id2 in dbs.get_class_ids_from_uri(obj):
                if pred in rdfs.ranges and classuri2 not in rdfs.ranges[pred]:
                    continue
                objectproperty=dbs.get_objectproperty(classuri1,pred,classuri2)
                objectproperty.connect(id1,id2,dblabels,db)
                inserted=True

    assert inserted

def cleanup(conf):
    dbs.remove_redundant_things()

    if conf.delete_thing_tables:
        dbs.delete_tables_with(owlns+'Thing')

def save_state():
    log('saving state to rdf2rdb.pickle')
    dbs.db=None
    f=open('rdf2rdb.pickle','w')
    pickle.dump((version,dbs,rdfs),f)
    f.close()
