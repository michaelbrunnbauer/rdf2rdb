
import settings

class rdfschema(object):

    def __init__(self):
        self.functionalproperties=set(settings.functionalproperties)
        self.ranges={} # property -> set of ranges
        self.domains={} # property -> set of domains
        self.superclasses={} # class -> set of superclasses
        self.superproperties={} # property -> set of superproperties
        self.subproperties={} # property -> set of subproperties
        self.sameas={} # uri -> set of uris
        self.new=set()

    def add(self,s,a,b):
        if a==b: return False # ignore reflexivity
        if a not in s:
            s[a]=set()
        if b not in s[a]:
            s[a].add(b)
            self.new.add((a,b))
            return True
        return False

    def entail_symmetric_transitive(self,s):
        changed=True
        while changed:
            changed=False
            for a,l in s.items():
                for b in list(l):
                    changed=self.add(s,b,a)
            for a,l in s.items():
                for b in list(l):
                    if b not in s: continue
                    for c in list(s[b]):
                        if c not in l:
                            l.add(c)
                            changed=True

    def entail_transitive(self,s):
        changed=True
        while changed:
            changed=False
            for a,l in s.items():
                for b in list(l):
                    if b not in s: continue
                    for c in list(s[b]):
                        if c not in l:
                            l.add(c)
                            changed=True

    def addrange(self,property,range):
        if property not in self.ranges:
            self.ranges[property]=set()
        self.ranges[property].add(range)

    def adddomain(self,property,domain):
        if property not in self.domains:
            self.domains[property]=set()
        self.domains[property].add(domain)

    def addsuperclass(self,cl,superclass):
        self.new=set()
        self.add(self.superclasses,cl,superclass)
        self.entail_transitive(self.superclasses)
        return tuple(self.new)

    def addsuperproperty(self,property,superproperty):
        self.add(self.subproperties,superproperty,property)
        self.entail_transitive(self.subproperties)
        self.new=set()
        self.add(self.superproperties,property,superproperty)
        self.entail_transitive(self.superproperties)
        return tuple(self.new)

    def addsameas(self,uri1,uri2):
        self.new=set()
        self.add(self.sameas,uri1,uri2)
        self.add(self.sameas,uri2,uri1)
        self.entail_symmetric_transitive(self.sameas)
        return tuple(self.new)
