import sys,os
import json
import pprint
from astrodata.adutils import ksutil
from astrodata import generaldata
from copy import copy, deepcopy
from glob import glob
import re 
import shutil
from astrodata import termcolor as tc
import pandas as pd

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
    
       

class TxtData(ReferenceOnlyData):
    """Used for detecting how to load a text file,
        including csv and json. 
    """
    def load_header(self):
        txtfile = open(self.filename)        
        
        line0 = txtfile.readline()
        self.put("loaded_from.format", "txt")
        self.add("type_hints", "TXT", unique=True)
        
        if "," in line0:
            table = self.put("table", {})
       
            self.put("loaded_from.format", "csv")
            
            headrow = line0.split(",")
            self.put("table.headrow", headrow)
            self.add("type_hints", "TABLE")
        numrows = 0
        
        if False: #don't load
            for line in txtfile:
                row = line.split(",")
                rows.append(row)
                numrows += 1
            print "loaded %d rows" % numrows    
            
        txtfile.close();   

    

class JSONData(SetrefData):
    json = None
    filename = None
    initarg = None
    
    def __init__(self, initarg, defer_load = True):
        super(JSONData, self) .__init__( initarg)
        self.json = {}
        self.initarg = None
        
        if defer_load:
            self.load_header(initarg)
        elif not defer_load:
            self.load(initarg)
        
    def load_header(self):
        pass # no header
            
    def do_write(self, fname, rename = False):
        if rename:
            self.filename = fname
        
        tfile = open(self.filename, "w")
        tfile.write(json.dumps(self.json))
        tfile.close()
    
    def load(self, initarg):
        self.filename = initarg
        jsonfile = open(initarg)
        self.json = json.load(jsonfile)
        jsonfile.close()
        #print "jd14:",self.pprint()
        
    def pretty_string(self):
        retstr = ksutil.dict2pretty(self.filename, self.json)
        return retstr
    
    #### modifications of the json dictionary            
    def _getref(self, keystring, put=False):
        keys = keystring.split(".")
        d = self.json
        t = d
        for key in keys[0:-1]:
            if put and not key in t:
                t[key] = {}
            t = t[key]
        return t, keys[-1]

    def add(self, keystring, val, unique = False):
        """ used to add to a list member """
        t,k = self._getref(keystring)
        #print "jd134:", keystring, t,k
        if k not in t:
            t[k] = []
        
        if not unique or (not val in t[k]):
            t[k].append(val)
                
    def get(self, keystring):
        t,key = self._getref(keystring)
        return t[key]
       
    def put(self, keystring, val):
        t,key = self._getref(keystring, put=True)
        t[key] = val
        
    def close(self):
        pass
        

class PandasData(SetrefData):
    assumed_type = "TABLE"
    _dataframe = None
    _loaded = False
    _supported_storage = {
            "h5" : "%(basename)s.h5",
            "csv": "%(basename)s.csv",
            "xls": "%(basename)s.xls"
        }
    # user hasattr rather than none_dataframe = None
    #def load_header(self, initarg, **args):
    #    super(PandasData, self).load_header(self, initarg, **args)
    
    def __init__(self, initarg, **argv):
        if isinstance(initarg, list):
            self._initialize_()
            self.dataframe = pd.DataFrame(initarg)
            self._loaded = True
            self.filename = "unnamed.h5"
        if isinstance(initarg, pd.DataFrame):
            self._initialize_()
            self.dataframe = initarg
            self._loaded = True 
            self.filename = "unnamed.h5"   
        else:
            super(PandasData, self).__init__(initarg, **argv)
    ## properties ##
    #
    def _get_dataframe(self):
        if not self._loaded:
            self.load()
        return self._dataframe
    
    def _set_dataframe(self, val):
        #self.is_changed()
        self._dataframe = val
        return
    
    def _del_dataframe(self):
        self._dataframe = None
    
    dataframe = property(_get_dataframe, _set_dataframe, _del_dataframe)
    
    #
    ## properties (end) ##
    
    def clean_data(self):
        # NOT NEEDED ATM
        return
        
        df = self.dataframe
        types = df.dtypes
        
        infered = df.apply (lambda x: pd.lib.infer_dtype(x.values)) 
        print "jd390:df.dtypes\n", types
        print "jd392:df infer dtype\n", type(infered)
        print "jd393:infered", infered
        print "jd394: -----"
        for col in infered.index[infered == "mixed"]:
            print "jd394: col =", col
            print "jd397: ", df[col].index
            def enc(x):
                if isinstance(x, basestring):
                    return x.encode("utf-8")
                else:
                    import traceback
                    print tc.colored("-"*20, "white", "on_grey")
                    traceback.print_stack()
                    print tc.colored("-"*20, "white", "on_grey")
                    return str(x)                
            for ind in df[col].index:
                df[col][ind] =  enc(df[col][ind])
        
        for col in infered.index[infered == "mixed"]:
            print "jd394: col =", col
            print "jd397: ", df[col].index
            def enc(x):
                if isinstance(x, basestring):
                    return str(x)
                else:
                    import traceback
                    print tc.colored("-"*20, "white", "on_grey")
                    traceback.print_stack()
                    print tc.colored("-"*20, "white", "on_grey")
                    return str(x)                
            for ind in df[col].index:
                df[col][ind] =  enc(df[col][ind])
    
    def do_write(self, filename, rename=False):
        
        #print "jd209: dowrite",filename
        # this writes the setref loose header/data note
        super(PandasData, self).do_write(filename, rename)
        #print "jd212: pandas write", self.filename
        # this writes the dataframe
        # @@TODO: change to use the self._supported_storage dictionary
        if  self.filename.endswith(".h5"):
            self.dataframe.to_hdf(self.filename, "table")
        elif self.filename.endswith(".csv"):
            self.dataframe.to_csv(self.filename)
        elif self.filename.endswith(".xls"):
            sheetname = self.get("_data.sheetname")
            print "jd348: sheetname =", sheetname
            self.dataframe.to_excel(self.filename, sheetname)
                    
    def load(self, df=None):
        super(PandasData, self).load()
        self._loaded = True
        if not df:
            df = self.filename
        
        import pandas as pd
        
        
        if df.endswith(".txt") or df.endswith(".csv"):
            self.dataframe = pd.read_csv(df)
        
        elif df.endswith(".xls") or df.endswith(".xlsx"):
            ef = pd.ExcelFile(df)
            sheet = ef.sheet_names[0]
            self.put("_data.sheetname", sheet)
            
            self.dataframe = ef.parse(sheet).dropna()
            
            #self.clean_data()
        elif df.endswith(".h5"):
            self.dataframe = pd.read_hdf(df, "table")
                        
    def nativeStorage(self):
        self.use_storage("h5")
    native_storage = nativeStorage
    
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
    
    def supports_storage(self, storage):
        return storage in self._supported_storage
