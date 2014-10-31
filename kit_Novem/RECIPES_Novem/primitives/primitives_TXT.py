from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
from astrodata.AstroDataType import globalClassificationLibrary as gCL
from primitives_SETREF import SetRefPrimitives
import pandas as pd

from astrodata.generaldata import GeneralData

log = logutils.get_logger(__name__)

class TxtPrimitives(SetRefPrimitives):
    astrotype = "TXT"
    
    def parseAsDataDictionary(self, rc):
        from jsondata import PandasData
        
        failed = True
        for inp in rc.get_inputs():
            tfile = open(inp.filename)
            table = []
            for line in tfile:
                lineparts = line.split()
                mid = None
                if len(lineparts) >= 3:
                    mid = lineparts[1]
                if mid == "C" or mid == "N":
                    #log.info("GOT A GOOD LINE")
                    if False:
                        log.info("%s (%s) = %s" % ( lineparts[0].lower(),
                                                lineparts[1],
                                                " ".join(lineparts[2:])
                                               ) 
                                )
                    table.append([ lineparts[0].lower(), lineparts[1], " ".join(lineparts[2:]) ])
            if len(table) > 0:
                log.stdinfo("parseAsDataDictionary found %d items for table"%len(table));
                tabledata = PandasData(table)
                tabledata.filename = inp.filename
                tabledata.nativeStorage()
                tabledata.dataframe.columns = ["col_name", "col_type", "col_label"]
                #log.info("pT24:line %d:%s" % ( len(lineparts), line))
                rc.report_output(tabledata)
        yield rc
    parseAsSpecial = parseAsDataDictionary
    
