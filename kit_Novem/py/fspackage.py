import getpass
from astrodata import Lookups
import os
import shutil

class FSPackage(object):
    setref = None
    storename = None
    elements = {}
   
    def __init__(self, setref = None):
        self.elements["user"] = getpass.getuser()
        info = Lookups.compose_multi_table( "*/warehouse_settings", 
                                            "warehouse_elements", 
                                            "shelf_addresses", 
                                            "type_shelf_names",
                                            "type_store_precedence")
        self.warehouse_elements = info["warehouse_elements"]
        self.shelf_addresses    = info["shelf_addresses"]
        self.type_shelf_names   = info["type_shelf_names"]
        self.type_store_precedence    = info["type_store_precedence"]
        #print "fs20: ts_prec", self.type_store_precedence
        #print "fs21: sh_names", self.type_shelf_names
        if setref:
             self.elements_from_setref(setref)   
             #self.get_storename()
             
    def elements_from_setref(self, setref):
        self.setref = setref
        year = setref.meta("year")
        month = setref.meta("month")
        day = setref.meta("day")
        if day and month and year:
            self.elements.update({"year":year, "day":day, "month":month})
        self.elements.update(self.warehouse_elements)
        if year and month and day:
            current_day = "%4d%02d%02d" % (year, month, day)
            self.elements["complete_day"] = current_day
        # type
        srtypes = setref.get_types()
        # note that it should only be ONE of these types
        # but we stop at the first
        settype = None
        for typ in self.type_store_precedence:
            #print "fs42: typ", typ
            if typ in srtypes:
                settype = typ
                break
                
        if not settype:
            settype = "&".join(srtypes)
        #print "fs48: settype",settype
        self.elements["type"] = settype

    def format_storage_location(self, shelfname, elements = {}):
        fargs = self.elements
        fargs.update(elements)
        #print "dw65 FARGS",fargs
        wareobj = self.shelf_addresses[shelfname]
        if isinstance(wareobj, basestring):
            wareobj = {"path_templ": wareobj}
        
        requires = None
        if "requires" in wareobj:
            requires = wareobj["requires"]
            
        try:
            path = wareobj["path_templ"].format( **fargs )
        except KeyError:
            print "can't compose %s" % wareobj["path_templ"]
            print "using fargs   %s" % fargs
            raise
        fullpath = path
        return fullpath
    
    def get_store_dirname(self, setref = None):
        if not setref:
            setref = self.setref
        self.elements_from_setref(setref)
        settype = self.elements["type"]
        if settype in self.type_shelf_names:
            shelfname = self.type_shelf_names[settype]
            storepath = self.format_storage_location(shelfname)
        else:
            storepath = self.format_storage_location(shelfname)
        self.store_dirname = storepath
        return storepath
        
    def get_store_path(self, setref = None):
        storepath = self.get_store_dirname(setref)
        storename = os.path.join(storepath, setref.basename)
        self.storename = storename
        return storename
        
    def transport_to_warehouse(self):
        workpath = self.setref.filename
        storedir = self.get_store_dirname()
        storepath    = os.path.join(storedir, self.setref.basename)
        sr_storepath = os.path.join(storedir, os.path.basename(self.setref.setref_fname))
        print "dw111:",workpath, storepath
        # ensure target dir exists
        if not os.path.exists(storedir):
            os.makedirs(storedir)
        
        # copy workfile to store    
        if os.path.exists(workpath):
            shutil.move(workpath, storepath)
        sr_workpath = self.setref._make_setref_fname(type="input")
        # copy setref to store
        if os.path.exists(sr_workpath):
            shutil.move(sr_workpath, sr_storepath)
        return True 
        
    def transport_from_warehouse(self):
        # doesn't do anything
        pass
        
