import os

from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
from astrodata.AstroDataType import globalClassificationLibrary as gCL
from primitives_SETREF import SetRefPrimitives
import pandas as pd

from astrodata.generaldata import GeneralData

log = logutils.get_logger(__name__)

class PandasPrimitives(SetRefPrimitives):
    astrotype = "JSON"
    
    def loadTables(self, rc):
        for inp in rc.get_inputs():
            inp.load()
        yield rc
        
    ## COLUMN_RELATE ##########    
    def columnRelate(self, rc):
        for inp in rc.get_inputs():
            log.stdinfo("rows=%s" % " | ".join( 
                                        inp.dataframe.columns.values.tolist()
                                              )
                       )
            log.stdinfo("cols=%s" % repr ( inp.dataframe.iloc[0:,0].values ) )
        yield rc
        
    def showTables(self, rc):
        start = 0
        end = 10
        for inp in rc.get_inputs():
            df = inp.dataframe
            log.stdinfo( str(inp.dataframe.columns.get_values()) )
            types = df.apply(lambda x: pd.lib.infer_dtype(x.values))
            log.stdinfo( str(inp.dataframe.columns.get_values()))
            log.stdinfo(inp.dataframe[start:end].to_string())
            
        yield rc
        
    def summarizeTables(self, rc):
        for inp in rc.get_inputs():
            log.stdinfo("showTables (pP21):\n %s " % repr(inp.dataframe.describe()))
            yield rc
    
    def setStorage(self, rc):
        storage = rc["storage"]
        if not storage:
            log.stdinfo("no storage type indicated")
            
        for inp in rc.get_inputs():
            if inp.supports_storage(storage):
                inp.load()
                fname = inp.filename
                inp.use_storage(storage)
                log.debug("Changed storage from %s to %s" % (os.path.basename(fname), inp.basename))
            else:
                fname = inp.filename
                log.stdinfo("%s does not support '%s' storage" % (os.path.basename(fname), storage))
            rc.report_output(inp)    
        yield rc          
