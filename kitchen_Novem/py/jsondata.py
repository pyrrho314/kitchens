# Copyright (C) 2014 Novem LLC, created Craig Allen 2014
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.import generalclassification

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
import pandas as pd

from setref import SetrefData, ReferenceOnlyData

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
