from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
from astrodata.AstroDataType import globalClassificationLibrary as gCL
from primitives_SETREF import SetRefPrimitives

from astrodata.generaldata import GeneralData

log = logutils.get_logger(__name__)

class PandasPrimitives(SetRefPrimitives):
    astrotype = "JSON"
    
    def loadTables(self, rc):
        for inp in rc.get_inputs():
            inp.load()
        yield rc
        
    def showTables(self, rc):
        start = 0
        end = 10
        for inp in rc.get_inputs():
            log.stdinfo(inp.dataframe[start:end].to_string())
        yield rc
        
    def summarizeTables(self, rc):
        for inp in rc.get_inputs():
            log.stdinfo("showTables (pP21):\n %s " % repr(inp.dataframe.describe()))
            yield rc
