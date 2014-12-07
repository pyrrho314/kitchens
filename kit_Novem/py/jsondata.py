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
from pandasdata import PandasData

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
        


