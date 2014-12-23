# %load_ext recipe
import os
from astrodata.mkro import *
from astrodata.generaldata import GeneralData
from astrodata.adutils import logutils as lu
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
import argparse
from astrodata.Lookups import get_lookup_table
from astrodata.adutils import ksutil as ks
from hbdata import HBGeoTIFF
from hbshapedata import HBShapeData
from astrodata.adutils import termcolor as tc

from hbmdbstorage import MDBStorage

parser = argparse.ArgumentParser()
parser.add_argument("--date", dest="date_range", default = "20140908")
parser.add_argument("--phrase", dest = "phrase", default = "ndvistarprime")
parser.add_argument("--context", dest = "context" , default = None)
parser.add_argument("--verbose", dest = "verbose", default = False, action="store_true")
parser.add_argument("--index", dest = "index", default = 0)
parser.add_argument("--subdir", dest = "subdir", default = None)


      
def load_ipython_extension(ipython):
    # The `ipython` argument is the currently active `InteractiveShell`
    # instance, which can be used in any way. This allows you to register
    # new magics or aliases, for example.
    lu.config("console", console_lvl = "info", file_name = None)
    #log = lu.get_logger("__notebook__")
    lu.change_level("INFO")
    #ipython.register_magic_function(recipe_magic, magic_kind = "cell", magic_name = "recipe")
    recmag = RecipeMagics(ipython)
    ipython.register_magics(recmag)

def unload_ipython_extension(ipython):
    # If you want your extension to be unloadable, put that logic here.
    pass

@magics_class
class RecipeMagics(Magics):
    ipython = None
    params = {}
    ROOTDIR = None
    def __init__(self, ip, parms = {}):
        super(RecipeMagics, self).__init__(ip)
        self.ipython = ip
        self.params = parms
    
    def get_config_paths(self):
        tdirs_table = get_lookup_table("HydroBio/directories")
        if not tdirs_table:
            print "NO SUCH CONTEXT:", context
            return line
        tdirs = tdirs_table[0]
        #print "tdirs",tdirs
        #print ks.dict2pretty("the_dirs",tdirs)

        the_dirs = {}
        for key in tdirs:
            the_dirs[key] = tdirs[key] % os.environ
        return the_dirs
        
    @line_magic
    def recipe_paths(self, line):
        context = None
        if len(line)>0:
            context = line
        try:
            self.ROOTDIR =  self.ipython.ev("ROOTDIR")
        except NameError:
            ROOTDIR = self.ROOTDIR = os.getcwd()
            self.ipython.push({"ROOTDIR":self.ROOTDIR})    
        
        print "ROOTDIR    = %s" % self.ROOTDIR
        print "CURRENTDIR = %s" % os.getcwd()

        
        tdirs_table = get_lookup_table("HydroBio/directories", context=context)
        if not tdirs_table:
            print "NO SUCH CONTEXT:", context
            return line
        tdirs = tdirs_table[0]
        #print "tdirs",tdirs
        #print ks.dict2pretty("the_dirs",tdirs)

        the_dirs = {}
        for key in tdirs:
            the_dirs[key] = tdirs[key] % os.environ
        self.ipython.push(the_dirs)
        return ks.dict2pretty("adding variables to namespace", the_dirs)
        
     
    @line_magic
    def sample_shape(self, line):
        args = line.split()
        options = parser.parse_args(args = args)
        phrase = options.phrase
        print "Looking for '%s' in fieldname" % (phrase)
        
        mdb = MDBStorage(collection_name = "shapes")
        reg = ".*?%s.*?" % phrase
        cursor = mdb.collection.find({"geojson.feature.properties.fieldname": {"$regex": reg} } )
        
        numfound = cursor.count()
        if numfound == 0:
            print tc.colored("NO SHAPES MATCH", "red")
            return None
        
        print "numfound = %d selecting 0'th" % numfound
        if options.verbose:
            outd = []
            for shape in cursor:
                outd.append(shape["_id"])
            print ks.dict2pretty("shapes found", outd)
        hbshape = HBShapeData({"setref":cursor[0]})
        addn = {"hbshape": hbshape, "shape_id": hbshape.meta("_id"), "loaded_from_mongo":True}
        addnmsg = {"hbshape":type(hbshape), "shape_id": addn["shape_id"],
                    "loaded_from_mongo":True}
        print ks.dict2pretty("adding to namespace", addnmsg)
        self.ipython.push(addn)
        
        return None
    
    @line_magic
    def sample_tiff(self, line):
        import glob
        args = line.split()
        options = parser.parse_args(args = args)
        date_r = options.date_range
        phrase = options.phrase
        index = int(options.index)
        print "Using date_range = %s and phrase = %s" % (date_r, phrase)
        
        dirs = self.get_config_paths()
        globpart = ("*%(datestr)s*%(phrase)s*.tif" %
                        { "datestr":date_r, # can't be range atm
                            "phrase":phrase
                        }
                    )
        datadir = dirs["processed_data"]% os.environ
        if options.subdir:
            datadir = os.path.join(datadir, options.subdir)
        globpath = os.path.join(datadir, globpart)
        fils = glob.glob(globpath)
        if len(fils) == 0:
            print tc.colored("NO FILES MATCH", "red")
            return None
        fil = fils[0]
        print "found %d files, choosing image index = %d" % (len(fils), index)
        
        if options.verbose:
            print ks.dict2pretty("found", [os.path.basename(fil) for fil in fils])
        hbgeo = HBGeoTIFF(fil)
        ipython = self.ipython
        
        addn = {"hbgeo": hbgeo, "tiffname": fil}
        addnmsg = {"hbgeo":type(hbgeo), "tiffname": os.path.basename(fil)}
        print ks.dict2pretty("adding to namespace", addnmsg)
        ipython.push(addn)
        
        return None
        
    @cell_magic
    def recipe(self, line, cell):
        """recipe cell magic"""
        rargs = line.split(" ")
        rname = rargs[0]
        if len(rargs)>1:
            rtype = rargs[1]
        else:
            rtype = "SETREF"
        if len(rargs)>2:
            contextname = rargs[2]
        else:
            contextname = "default"
        if len(rargs)>3:
            dbg = rargs[3]
        else:
            dbg = "INFO"
        rtype = "SETREF" if rtype=="-" else rtype
        contextname = "default" if contextname=="-" else contextname
        dbg = "INFO" if dbg == "-" else dbg
        lu.change_level(dbg)
        ro = mkRO(astrotype=rtype, copy_input=False, args=[], argv={"mode":"notebook"})
        ro.context.set_context(contextname, insert=True)
        ipython = self.ipython
        ro.recipeLib.load_and_bind_recipe(ro, rname, src = cell)
        ro.runstep(rname, ro.context)
        ipython.push({"rc":ro.context})
        return cell
