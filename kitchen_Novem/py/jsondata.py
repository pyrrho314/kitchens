import sys,os
import json
import pprint
from astrodata.adutils import ksutil
from astrodata import generaldata
from copy import copy, deepcopy

class SetrefData(generaldata.GeneralData):
    
    _setref = None
    data_object = None
    setref_fnam = None
    
    def __init__(self, initarg, defer_load = True):
        super(SetrefData, self) .__init__( initarg)
        #print "jd15: SetrefData.__init__", initarg, defer_load
        self._setref = {}
        self.initarg = initarg
        
        if defer_load:
            self.load_header(initarg)
        elif not defer_load:
            self.load(initarg)
        
    def load_header(self, initarg):
        # setref is all header
        #print "jd25: setref load_header", initarg
        if (isinstance(initarg, basestring)):
            setreffile = initarg+".setref"
            self.filename = initarg
            self.setref_fname = setreffile
            self.put("properties.setref_file",self.setref_fname)
            if os.path.exists(self.setref_fname):
                jsonfile = open(self.setref_fname)
                self._setref = json.read(jsonfile)
                jsonfile.close()
        elif (isinstance(initarg, SetrefData)):
            #print ("jd 33: adapting setref")
            #print "jd34:", initarg.pretty_string()
            self.filename = initarg.filename
            self._setref = initarg._setref
        pass # no header
    
    def load(self, initarg):
        pass
            
    def do_write(self, fname, rename = False):
        pass
        
    def pretty_string(self):
        retstr = ksutil.dict2pretty(self.filename, self._setref)
        return retstr
    
    def _getref(self, keystring, put=False, struct = None):
        d = None
        if struct:
            d = struct
        else:
            d = self._setref
        keys = keystring.split(".")
        t = d
        for key in keys[0:-1]:
            if put and not key in t:
                t[key] = {}
            t = t[key]
        return t, keys[-1]

    def add(self, keystring, val, unique = False):
        """ used to add to a list member """
        curlist = self.get(keystring)
        #print "jd68:", curlist
        if not curlist:
            nlist = []
            self.put(keystring, nlist)
            curlist = self.get(keystring)   
        #print "jd73:", curlist
        if not (val in curlist) or (not unique):
            curlist.append(val)
        
                
    def get(self, keystring):
        t,key = self._getref(keystring)
        if key in t:
            return t[key]
        else:
            return None
               
    def put(self, keystring, val):
        t,key = self._getref(keystring, put=True)
        t[key] = val
        
    def close(self):
        pass
        


class TxtData(SetrefData):
    """Used for detecting how to load a text file,
        including csv and json. 
    """
    def load_header(self, initarg):
        
        self.filename = initarg
        
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
    dataframe = None
    #def load_header(self, initarg, **args):
    #    super(PandasData, self).load_header(self, initarg, **args)
    def load(self, df=None):
        if not df:
            df = self.filename
        
        import pandas as pd
        if df.endswith(".txt") or df.endswith(".csv"):
            self.dataframe = pd.read_csv(df)
        elif df.endswith(".xls") or df.endswith(".xlsx"):
            ef = pd.ExcelFile(df)
            sheet = ef.sheet_names[0]
            self.dataframe = ef.parse(sheet)
