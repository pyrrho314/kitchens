import os

from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
from astrodata.AstroDataType import globalClassificationLibrary as gCL
from primitives_SETREF import SetRefPrimitives
import pandas as pd


from astrodata.generaldata import GeneralData

try:
    import termcolor
    COLORSTR = termcolor.line_color
except:
    COLORSTR = lambda arg: arg 
    

log = logutils.get_logger(__name__)

class PandasPrimitives(SetRefPrimitives):
    astrotype = "TABLE"
    
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
        
    def plot(self, rc):
        """general table plot just calls Pandas Plot with a convienience plot, not particularly useful for
            table data in general, and possibly time sensitive"""
        import matplotlib.pyplot as plt
        numrows = int(rc["num_rows"]) if rc["num_rows"] else 1000
        yaxis = rc["yaxis"] if rc["yaxis"] else None
        for inp in rc.get_inputs():
            inp.dataframe[:numrows].plot(y=yaxis)
            plt.show()
            yield rc
    
    def plotly(self, rc):
        import plotly.plotly as py
        from plotly.graph_objs import Layout,Figure
        def df_to_iplot(inp):
            
            '''
            Coverting a Pandas Data Frame to Plotly interface
            '''
            df = inp.dataframe
            del df["est"]
            lines={}
            x = df.columns.values[2:]
            for i in range(len(df)):
                row = df.iloc[i]
                key = row["industry"]
                lines[key]={}
                lines[key]["x"]=x
                lines[key]["y"]=row[2:].values
                lines[key]["name"]=key
                #Appending all lines
            lines_plotly=[lines[key] for key in lines]
            return lines_plotly

        py.sign_in("pyrrho", "04n3iw0mae")

        for inp in rc.get_inputs():
            df = inp.dataframe
            data = df_to_iplot(inp)
            layout = Layout(
                    title = inp.basename
                    )
            fig = Figure(data=data, layout=layout)
            unique_url = py.plot(data, filename = inp.basename, auto_open=False)
            log.status("plot for %s found at %s" % (inp.basename, unique_url))
        yield rc         
    def showTables(self, rc):
        start = 0
        end = 20
        
        display=None
        
        #try:
        #    from IPython.display import display
        #except:
        #    dipslay = None
        for inp in rc.get_inputs():
            log.stdinfo( "File: %s" % COLORSTR(inp.basename, attrs=["bold"]))
            df = inp.dataframe
            #log.stdinfo( str(inp.dataframe.columns.get_values()) )
            types = df.apply(lambda x: pd.lib.infer_dtype(x.values))
            #log.stdinfo( str(inp.dataframe.columns.get_values()))
            if display:
                display(inp.dataframe[start:end])
            else:
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
