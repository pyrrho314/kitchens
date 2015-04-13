import sys,os
import json
import pprint
from astrodata.adutils import ksutil
from astrodata.adutils import ksutil as ks
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
    """
    The SetrefData class is meant to be used as a parent class, though it can be
    used directly with json data, for data meant to be handled by the 
    "primitives".

    Design:

        The SetrefData class descends from astrodta.generaldata.GeneralData.
    These classes integrate the datatype to the basic requirements of
    the recipe system, and try to perfom the regular sequences of loading,
    writing, and metadata access by calling method members which are meant
    to be implemented by the child class and called at the correct moment,
    something like a callback, or a virtual functions.

    Features:
    
        Loading Semantics:
            SetrefData loads data using the initarg to the constructor,
            So that children have a chance to partake in loading from
            a given type of initarg, SetrefData will call 
            `self._accept_initarg_(..)`. Further loading of metadata
            or "header" information is considered separate from loading
            target data, to facilitate large data sets in cases where
            only metadata is required.
        Writing Semantics:
            When writing, SetrefData takes some steps to prevent accidentally
            overwritting extant data. The recipe system is prone to this
            insofar as running the same recipe twice will produce identically
            named files though parameters to the recipe may have changed the
            output.  Given the purpose is automation it's desired to be able
            to recover and detect such situations. GeneralData allows a
            child class to implement an "extant overwrite" functionality which
            SetrefData implements. The writing semantics are controlled in
            GeneralData.write(..) which will call the childs do_write(..)
            function to do the actual writing. SetrefData performs two
            services, firstly, it will handle writing the metadata to
            filename.ext.setref, and two, it will check to see if files
            of the output names already exist, and if they do, will push
            them in a datastack, in a subdirectory, suffixes with ";N" where
            "N" is a unique, increasing, integer.  The child class doesn't
            partake in this safety measure, and when it's implementation of
            "do_write" executes, it can assume a clean disk. Note, it
            must call super(self.__class__, self).do_write() BEFORE
            writing. Additionally, GeneralData itself uses some overrideable
            members to keep track of if datasets need writing eventually,
            i.e. at the end of the recipe, or if they are unchanged since
            loading or have not changed since the last writing. The user
            must cooperate by calling `needs_write()`, unless the child
            type specifically detects changes, e.g. by intercepting
            property puts, and relevant _accept_initarg_ initializations.
        Properties:
            SetrefData handles properties in a general way which makes 
            them distinct from object members, which can sometimes be thought
            of as properties.  In this case we are talking about "Set Reference"
            properties, those which will go in the ".setref" file and which are
            thus the persistent part of the metadata, especialy any metadata
            which cannot be recovered from target datasets, which was added
            for example, by the system to track data semantics.
            The goal is to be able to fluidly store these properties on disk,
            in mongo, and send them to javascript where they may be stored in
            IndexedDB. Property values should therefore be json and/or mongo
            serializable. The primary issue here is, use basic python types,
            dictionary, list, integer, floating point, and strings. Additonally,
            python datetime can be used with mongo, but when stored on disk 
            this and any other datatype not json serializable will be converted
            to string. This can be dealt with in a client `load(..)` or 
            `load_header(..)` function, or `_accept_initarg(..)` function,
            which could convert the datetime strings. In this case both 
            python and mongo ISODate formats should be taken into account.
            
            In memory, all the Set Reference Properties are stored in the
            `_setref` nested dictionary.  Since the dictionary is nested
            and can be quite deep, complicated, and so on, SetrefData 
            sports member functions put, get, and add, which accept
            keys in the style of javascript syntax, and on puts where the
            intervening dictionaries and lists do not exist, these
            intervening structures will be created. 
        Cooperation with the type system:
            GeneralData allows its children to define their own "property"
            get and set functions, and SetrefData uses this _setref properties. This 
            in turn allows GeneralDataClassifications to probe the metadata they
            otherwise do not understand, since the specific child type
            will have populated the initial properties of the dataset reference
            by the time type checking is done. 
            
    """
    
    
    _setref = None
    data_object = None
    setref_fname = None
    _setref_fname = None
    initarg = None
    _x_prop_alias = None
    _loaded_by = None
    _typepre  = "unkn"
    _typepost = "xdt"
    ### properties
    ###
    _filename = None
    
    def build_filename (self):
        """Used to build a standard filename based on ._id"""
        sid = self.get("_id")
        print sid
        if not sid:
            sid = ks.rand_file_id()
        fname = "%s-%s.%s" % (self._typepre, sid, self._typepost)
        return fname        
    def get_filename(self):
        if not self._filename:
            self._filename = self.build_filename()
        return self._filename
    def set_filename(self, fin):
        self._filename = fin
        self.put("filename", fin)
        
    filename = property(get_filename, set_filename)    
    
    def __init__(self, initarg = None, force_load = None):
        """
        """
    
        super(SetrefData, self) .__init__( initarg)
        # child defined
        self._setref = {}
        self._initialize_()
        #  @@NOTE:@@TODO: remove this, extraneous reference 
        self.initarg = initarg
        #print "sr35:",initarg
        self._accept_initarg(initarg)
        #print "sr37:", self.filename
        if force_load:
            self.load_header()
            self.load()
        else:
            self.load_header()

    ### Properties Section ###
    #
    def get_field_id(self):
        return self.get("_id")
    
    def set_field_id(self, sID):
        self.put("_id", sID)
        
    _id = property(get_field_id, set_field_id)
    
    #
    ### Private Functions Section ##
    #

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
        print "sr187: initarg", initarg
        if isinstance(initarg, basestring):
            self.filename = initarg
            self._loaded_by = "filename"
        elif isinstance(initarg, dict):
            if "filename" in initarg:
                self.filename = initarg["filename"]
            if "setref" in initarg:
                self._setref = initarg["setref"]
                #self.filename = "hb_shape:%s" % self._setref["_id"]
            self._loaded_by = "setref"
        else:
            if hasattr(initarg, "filename"):
                self.filename = initarg.filename
            self._loaded_by = self.filename
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
            hidden = self._prop_alias[key]["hidden"]
            if not hidden:
                retdict[key] = self.meta(key)
        if self._setref:
            self.put("_meta.summary", retdict);
        return retdict
    
    metadata  = property(_get_metadata)    
    aliasdata = property(_get_metadata)
    
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
        #print "sr97:", self.filename, self.output_directory
        
        if self.filename.endswith(".setref"):
            setrefn = self.filename
        else:
            setrefn = "%s.setref" % os.path.basename(self.filename)
        self._setref_fname = os.path.join(
                                self.output_directory, setrefn)
        
        if type == "output":
            return self._setref_fname
        elif type == "input":
            indir = os.path.dirname(self.filename)
            setrefin = os.path.join(indir, setrefn)
            # print "sr106: indir",indir
            # print "sr107: setrefn", setrefn
            # print "sr108: setrefin", setrefin 
            return setrefin

    setref_fname = property(_get_setref_fname)

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
#        print "sr185:",self.filename, os.path.exists(self.filename)
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
        return True # we just use the file stack  

    def get_setref_copy(self):
        srcopy = deepcopy(self._setref)
        return srcopy

    def fill_dict(self, tdict):
        for key in tdict:
            val = tdict[key]
            if isinstance(val, basestring):
                #print "sr222",val, self.meta(val)
                tdict[key] = self.meta(val)
            else:
                self.fill_dict(val)
        return tdict                  
    
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
        if not self.filename:
            return
        print "sr354: filename", self.filename
        setrefout = self._make_setref_fname()
        #print "sr221:",setrefout
        setrefin = self._make_setref_fname(type = "input")
        print "sr357:\nout\t%s\nin\t%s" % (setrefout, setrefin)
        in_setrefn = None
        if os.path.exists(setrefin):
            print "sr360: setrefin", setrefin
            in_setrefn = setrefin
        else:
            print "sr363: setrefout", setrefout
            in_setrefn = setrefout
        if os.path.exists(in_setrefn):
            print "sr366: load_header setref_fname = "+self.setref_fname
            try:
                jsonfile = open(in_setrefn)
                self._setref = json.load(jsonfile)
                jsonfile.close()
            except:
                print "sr258:MALFORMED SETREF:", in_setrefn
                raise
        else:
            self.put("filename", self.filename)
            self.put("setref_fname", self.setref_fname)
            
    def load(self, initarg = None, force_load = False):
        print "sr379: load do nothin"
        # self.load_header()
        pass
            
    def do_write(self, fname, rename = False):
        self._push_file_stack()
        srfn = self.setref_fname # fname+".setref"
        srf = open(srfn, "w")
        types = self.get_types()
        self.put("_data.types", types)
        def fval(obj):
            return str(obj)
        json.dump(self._setref, srf, sort_keys=True, indent =4, default=fval)
        srf.close()
        pass
        
    def pretty_string(self, start_indent = 0):
        retstr = ""
        
        filename = "%s" % tc.colored("filename  :", 
                            attrs=["bold"]) + " %s/%s"  % (self.dirname,
                                                            tc.colored( self.basename, 
                                                                        attrs=["bold"])
                                                           )
                                      
        datatypes  = "%s" % (tc.colored("data types:", attrs=["bold"]) + " %s" % repr(self.get_types()))
        reprstring = "%s" % (tc.colored("data_obj  :", attrs=["bold"])    + " %s" % type(self))
        
        retstr += "%s%s" % (ksutil.calc_fulltab(start_indent) , filename)
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
        
           
    # other members
    def use_storage(self, storetype = None):
        # I prefer hdf5 at the moment
        name, ext = os.path.splitext(self.filename)
        if storetype in self._supported_storage:
            newfilename = self._supported_storage[storetype] % {"basename":name}
        if newfilename == self.filename:
            return False # no change needed
        else:    
            oldfilename = self.filename
            self.filename = newfilename
            self.put("filename",self.filename)   
            self.add("history.previous_filenames", oldfilename)
            return True # ok, changed 
    
    def add_setref_method(methodname, propname, pytype = None, hidden = False):
        from types import MethodType
        
        cls_patch = """
                    def _get_{propname}(self, pytype={pytype}, hidden={hidden}):
                        return self.get("{propname}")
                    self._get_{methodname} = MethodType(self, _get_{propname})
                    def _set_{propname}(self, value):
                        return self.put("{propname}", value)
                    self._set_{methodname} = MethodType(self, _set_{propname}
                    
                    """
                    
    def add_prop_alias(self, key, propname, pytype = None, hidden=False):
        """ hidden simply means don't return in .metadata property """
        # @@TODO: might want to ensure the alias isn't already defined
        self._prop_alias[key] = {"addr":propname, "pytype" : pytype, "key":key, "hidden":hidden}
    
    # this is for GeneralData which calls them with these names for subclass-specific property storage
    prop_get = get
    prop_put = put
    prop_add = add
    
class ReferenceCompleteData(SetrefData):
    def do_write(self, *args, **argv):
        super(ReferenceCompleteData, self).do_write(self, *args, **argv)
        cwd = os.getcwd()
        symname = os.path.relpath(self.filename)
        targname = os.path.relpath(self.setref_fname)
        # print("sr348:", targname, symname)
        os.symlink(targname, symname)
        return True
        
        
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

