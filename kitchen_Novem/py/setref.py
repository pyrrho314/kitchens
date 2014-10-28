
import sys,os
import json
import pprint
from astrodata.adutils import ksutil
from astrodata import generaldata
from copy import copy, deepcopy
from glob import glob
import re 
import shutil
import termcolor as tc

class SetrefData(generaldata.GeneralData):
    
    _setref = None
    data_object = None
    setref_fname = None
    _setref_fname = None
    initarg = None
    
    
    def __init__(self, initarg, force_load = None):
        super(SetrefData, self) .__init__( initarg)
        
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
        # OPTIMIZE: memory, keeps initializer around, could be other object!
        self.initarg = initarg
        
    def _get_setref_fname(self):
        self._make_setref_fname()
        return self._setref_fname    
    
    def _make_setref_fname(self):
        #use output directory
        #print "jd45:", self.filename, self.output_directory
        setrefn = "%s.setref" % os.path.basename(self.filename)
        self._setref_fname = os.path.join(
                                self.output_directory, setrefn)
        return self._setref_fname

    setref_fname = property(_get_setref_fname)
    
    ### Private Functions Section ##
    #
    # file stack
    def _getref(self, keystring, put=False, struct = None):
        d = None
        if struct:
            d = struct
        else:
            d = self._setref
        keys = keystring.split(".")
        t = d
        for key in keys[0:-1]:
            if not put and not key in t:
                return (None, None)
            if not key in t:
                t[key] = {}
            t = t[key]
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
        # setref is all header
        self._make_setref_fname()
        if os.path.exists(self.setref_fname):
            # print "jd57: load_header setref_fname = "+self.setref_fname
            jsonfile = open(self.setref_fname)
            self._setref = json.load(jsonfile)
            jsonfile.close()
        else:
            self.put("filename", self.filename)
            self.put("setref_fname", self.setref_fname)
    def load(self, initarg = None):
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
        
    def nativeStorage(self):
        return None # no action, no storage
      
    def get(self, keystring, create_path = False):
        t,key = self._getref(keystring, put = create_path)
        
        if (key and t) and (key in t):
            return t[key]
        else:
            return None
    def prop_exists(self, keystring):
        t,key = self._getref(keystring)
        if t:
            return True
        else:
            return False           
    def put(self, keystring, val):
        t,key = self._getref(keystring, put=True)
        t[key] = val
        
    def close(self):
        pass
        
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
        """ text does not want to write IT's own data, but just keep the set reference """
        self.put("_data.filename", self.filename)
        super(ReferenceOnlyData, self).do_write(self.filename)
        return False

