
import sys,os
import json
import pprint
from astrodata.adutils import ksutil
from astrodata import generaldata
from copy import copy, deepcopy
from glob import glob
import re 
import shutil
from astrodata.adutils import termcolor as tc
from astrodata.Errors import Error
from partlocator import PartLocator

class SetrefError(Error):
    message = "Set Reference Data Object Error"

class SetrefData(generaldata.GeneralData):
    
    _setref = None
    data_object = None
    setref_fname = None
    _setref_fname = None
    initarg = None
    _x_prop_alias = None
    
    def __init__(self, initarg, force_load = None):
    
        super(SetrefData, self) .__init__( initarg)
        # child defined
        self._setref = {}
        self._initialize_()
        self.initarg = initarg
        self._accept_initarg(initarg)
        
        if force_load:
            self.load()
        else:
            self.load_header()
        
            
    def _initialize_(self):
        if not self._setref:
            self._setref = {}
        self._prop_alias = {}
        
        if not self.output_directory:
            self.output_directory  = os.getcwd()
        self.put("_data.setref", True)
        # print "jd30: Setref.__init__ filename", self.filename
        
    def __repr__(self):
        pstr = super(SetrefData, self).__repr__()
        rstr = "%s:%s" % (pstr, self.filename)
        return rstr
        
    def _accept_initarg(self, initarg):
        if isinstance(initarg, basestring):
            self.filename = initarg
        else:
            if hasattr(initarg, "filename"):
                self.filename = initarg.filename
        # OPTIMIZEd: memory, keeps initializer around, could be other object!
        # REMOVED:self.initarg = initarg
    # metadata... just those with alias'
    def _get_metadata(self):
        """creates a new structure containing metadata collected from _setref member.
            See add_prop_alias(..), keys are considered relevant metadata.
        """
        keys = self._prop_alias.keys()
        retdict = {}
        for key in keys:
            retdict[key] = self.meta(key)
        if self._setref:
            self.put("_meta.summary", retdict);
        return retdict
        
    metadata = property(_get_metadata)
    
    def _get_prop_alias(self):
        return self._x_prop_alias
        
    def _set_prop_alias(self, val):
        self._x_prop_alias = val
        if self._setref:
            self.put("_prop_alias", val)
        return
    _prop_alias = property(_get_prop_alias, _set_prop_alias)
    
    def _get_setref_fname(self):
        self._make_setref_fname()
        return self._setref_fname
        
    def _make_setref_fname(self, type="output"):
        #use output directory
        #print "jd45:", self.filename, self.output_directory
        setrefn = "%s.setref" % os.path.basename(self.filename)
        self._setref_fname = os.path.join(
                                self.output_directory, setrefn)
        
        if type == "output":
            return self._setref_fname
        elif type == "input":
            indir = os.path.dirname(self.filename)
            setrefin = os.path.join(indir, setrefn)
            print "sr106: indir",indir
            print "sr107: setrefn", setrefn
            print "sr108: setrefin", setrefin 
            return setrefin

    setref_fname = property(_get_setref_fname)
    
    ### Private Functions Section ##
    #
    # file stack
    def _getref(self, keystring, put = False, pytype = str, struct = None):
        d = None
        #print "sr81:", d, keystring, put, pytype, struct
        if struct:
            d = struct
        else:
            d = self._setref
        loc = PartLocator(keystring, d, pytype=pytype)
        tc.COLOR_ON = True
        #print "sr83:\n",loc.terminal_string()
        if put:
            #print "sr85:", put
            loc.create_location()
            #print "sr87:", loc.struct
            #print "sr89:\n", loc.terminal_string()
        ref = loc.get_reference()
        #print "sr87:", ref, 
        return ref
        
        
        
    def _OLD_getref(self, keystring, put=False, struct = None):
        import re
        d = None
        if struct:
            d = struct
        else:
            d = self._setref
        keys = keystring.split(".")
        
        t = d
        allbutlast = keys[:-1]
        if len(allbutlast):
            for key in allbutlast:
                if not put and not key in t:
                    return (None, None)
                if not key in t:
                    t[key] = {}
                t = t[key]
        else:
            key = keys[-1]
            if not put and not key in t:
                return (None, None)
        return t, keys[-1]
        
    def _push_file_stack(self):
        """ 
        1) check if filename exists
            1) if it exists, move it out of the way
                1) glob for all file named "<filename>;*"
                    1) for all found:
                        1) re.match last version using "^.*;([0-9]+)+$"
                        2) put versions in list
                    2) NEWVER = max version, add one
                2) if no files, NEWVER = 1
                3) MV file to self.filename;NEWVER
                4) -done-
            2) doesn't exist
                1) -done-      
        """
        frev = 0;
        if os.path.exists(self.filename):
            # break down filename
            flist = glob("_fs_versions/%s;*"%self.basename)
            if len(flist):
                vers = []
                for f in flist:
                    m = re.match(r"^.*;([0-9]+)+$",f)
                    if m:
                        vers.append(int(m.group(1)))
                frev = max(vers)+1
            else:
                frev = 1
            movepath = os.path.join(self.dirname, "_fs_versions")
            if not os.path.exists(movepath):
                os.makedirs(movepath)
            movebname = "%s;%d" % (self.basename, frev)
            movefname = os.path.join(movepath, movebname)
            shutil.move(self.filename, movefname)
            if os.path.exists(self.setref_fname):
                
                movesetref_basename = "%s;%d" % (os.path.basename(self.setref_fname), frev)
                movesetref_fname = os.path.join(movepath, movesetref_basename)
                
                shutil.move(self.setref_fname, movesetref_fname) 
    #       
    # private functions section (end)
    #################################
    def allow_extant_write(self):
        return True # we just us the file stack  
                      
    def load_header(self):
        """ By default, looks for a setref file in the current directory.
        However, for some data, such as setref only, there might be a setref file
        in the target directory.
        @@REVIEW: is it safe, and if not can it be made safe, to use 
        setref that are not in the same directory as the file being loaded
        Ideas: 
        () We could put setref files in the input directories as an ingestion step.  
        () We can checksum, and put that in the set
        
        """
        # setref is all header
        setrefout = self._make_setref_fname()
        setrefin = self._make_setref_fname(type = "input")
        print "sr218:\nout\t%s\nin\t%s" % (setrefout, setrefin)
        in_setrefn = None
        if os.path.exists(setrefin):
            print "sr221: setrefin", in_setrefn
            in_setrefn = setrefin
        else:
            print "sr224: setrefout", setrefout
            in_setrefn = setrefout
        if os.path.exists(in_setrefn):
            # print "jd57: load_header setref_fname = "+self.setref_fname
            jsonfile = open(in_setrefn)
            self._setref = json.load(jsonfile)
            jsonfile.close()
        else:
            self.put("filename", self.filename)
            self.put("setref_fname", self.setref_fname)
            
    def load(self, initarg = None, force_load = False):
        self.load_header()
        pass
            
    def do_write(self, fname, rename = False):
        self._push_file_stack()
        srfn = self.setref_fname # fname+".setref"
        srf = open(srfn, "w")
        json.dump(self._setref, srf, sort_keys=True, indent =4)
        srf.close()
        pass
        
    def pretty_string(self, start_indent = 0):
        retstr = ""
        
        filename = "%s" % tc.colored("filename  :", attrs=["bold"]) + " %s/%s"  % (self.dirname,
                                                                                    tc.colored( self.basename, 
                                                                                                attrs=["bold"])
                                                                                   )
                                      
        datatypes  = "%s" % (tc.colored("data types:", attrs=["bold"]) + " %s" % repr(self.get_types()))
        reprstring = "%s" % (tc.colored("data_obj  :",attrs=["bold"])    + " %s" % repr(self))
        
        retstr += "\n%s%s" % (ksutil.calc_fulltab(start_indent) , filename)
        retstr += "\n%s%s" % (ksutil.calc_fulltab(start_indent) , datatypes)
        retstr += "\n%s%s" % (ksutil.calc_fulltab(start_indent) , reprstring)
        
        
        return retstr
    
    def pretty_setref(self, start_indent = 0):
        retstr = ""
        retstr += ksutil.dict2pretty("_setref", self._setref, indent = start_indent)
        return retstr
    
    def nativeStorage(self):
        return None # no action, no storage
    
    ### finalizing ###
    def close(self):
        pass
    
    ### ### ### PROPERTIES ### ### ###
    def add(self, keystring, val, unique = False):
        """ used to add to a list member """
        curlist = self.get(keystring, create_path = True)
        #print "jd68:", curlist
        if not curlist:
            nlist = []
            self.put(keystring, nlist)
            curlist = self.get(keystring)   
        #print "jd73:", curlist
        if not (val in curlist) or (not unique):
            curlist.append(val)
    def get(self, keystring, create_path = False, pytype = None):
        #print "sr239:", keystring, create_path, pytype
        loc = PartLocator(keystring, self._setref, pytype = pytype)
        return loc.property_value()
        
    def prop_exists(self, keystring):
        t,key = self._getref(keystring)
        #print "sr205:", t,key, keystring
        if t:
            return True
        else:
            return False           
    def put(self, keystring, val, pytype = None):
        loc = PartLocator(keystring, self._setref, pytype = pytype)
        #print "sr put 261:", keystring, pytype
        loc.create_location()
        #print "sr259:\n", loc.terminal_string()
        loc.property_value(val)
        
    def meta(self, key, pytype = None):
        if key in self._prop_alias:
            aliasinfo = self._prop_alias[key]
            propname = aliasinfo["addr"]
            #print "sr279:",aliasinfo
            if "pytype" in aliasinfo:
                pytype = aliasinfo["pytype"]
            #print "sr282:meta pytype", pytype
        else:
            propname = key
        return self.get(propname, pytype = pytype)
        
    def add_prop_alias(self, key, propname, pytype = None):
        # @@TODO: might want to ensure the alias isn't already defined
        self._prop_alias[key] = {"addr":propname, "pytype" : pytype, "key":key}
    
    # this is for GeneralData which calls them with these names for subclass-specific property storage
    prop_get = get
    prop_put = put
    prop_add = add
    
class ReferenceOnlyData(SetrefData):
    """ Sort of thought up as a generalization of the situation where
        I have a table that is not standard csv to parse.  We want to track it
        eventually to parse it... no need to copy the data out of the source directory though.
    """
    def _push_file_stack(self):
        # don't do this, we don't write files
        return 
        
    def write(self, *args, **argv):
        """ Some data, like text format, does not want to write IT's own data, but just keep the set reference.
        It's essentially read only, but you can write the metadata into the .setref file """
        self.put("_data.filename", self.filename)
        super(ReferenceOnlyData, self).do_write(self.filename)
        return False

