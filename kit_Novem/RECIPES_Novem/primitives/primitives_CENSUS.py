import os

from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
from astrodata.AstroDataType import globalClassificationLibrary as gCL
from primitives_PANDAS import PandasPrimitives
import pandas as pd
import matplotlib.pyplot as plt

from astrodata.generaldata import GeneralData
from jsondata import PandasData
import re

try:
    import termcolor
    COLORSTR = termcolor.line_color
except:
    COLORSTR = lambda arg: arg 
    

log = logutils.get_logger(__name__)

def business_cols(df = None, columns = None):
    if columns == None:
        cols = df.columns.values
    cmap = {"_order" : []}
    
    # create a useful dictionary about the new columns
    # slightly better than hardcoding
    for i  in range(0,len(cols)):
        col = cols[i]
        print col, type(col)
        m = re.match("n(?P<fr>[0-9]+)(_(?P<to>[0-9]+))?", col)
        if m:
            fr = m.group("fr")
            to = m.group("to")
            log.debug("fr->to %s -> %s" % (fr,to))
            if to:
                newcol = "p%s_%s" % (fr,to)
            else:
                newcol = "p%s" % (fr)
            cmap["_order"].append(col)
            cmap[col] = newcol
            cmap[newcol] = col
    return cmap  

class MetroBusinessPrimitives(PandasPrimitives):
    astrotype = "METROBUSINESS"
    def convertToPercent(self, rc):
        for inp in rc.get_inputs():
            # zeroeth, change name
            inp.add_suffix("percent")
            # first change the column names
            df = inp.dataframe
            cols = df.columns.values
            cmap = {"_order" : []}
            
            # create a useful dictionary about the new columns
            # slightly better than hardcoding
            for i  in range(0,len(cols)):
                col = cols[i]
                print col, type(col)
                m = re.match("n(?P<fr>[0-9]+)(_(?P<to>[0-9]+))?", col)
                if m:
                    fr = m.group("fr")
                    to = m.group("to")
                    log.debug("fr->to %s -> %s" % (fr,to))
                    if to:
                        newcol = "p%s_%s" % (fr,to)
                    else:
                        newcol = "p%s" % (fr)
                    cmap["_order"].append(col)
                    cmap[col] = newcol
            print cmap

            #log.stdinfo("new columns: %s" % cmap)
            #df.rename(columns=cmap, inplace=True)       
            # add the fields

            for incol in cmap["_order"]:
                log.status("incol = %s -> %s" % (incol, cmap[incol] ) )
                #log.status("%s"% df[incol][6000:6010]* 2);
                df[cmap[incol]] = df[incol]/df["est"]
                        #log.stdinfo("new columns: %s" % cmap)
                        #df.rename(columns=cmap, inplace=True)       
                        # add the fields
            
            for incol in cmap["_order"]:
                log.status("incol = %s -> %s" % (incol, cmap[incol] ) )
                #log.status("%s"% df[incol][6000:6010]* 2);
                df[cmap[incol]] = df[incol].astype(float)/df["est"].astype(float)
            
            inp.needs_write()
            rc.report_output(inp)
        yield rc
    
    def collapse_naics(self, rc):
        for inp in rc.get_inputs():
            df = inp.dataframe
            bynaics = df.drop(["msa","emp", "qp1", "ap"], axis=1).groupby("naics").sum()
            # drop the summary rows
            index = bynaics.index
            for el in index:
                if ("-" in el) or ("/" in el):
                    # print "dropping el",el
                    bynaics.drop(el, inplace=True)
            
            #log.status("pC142: %s" % bynaics)
            inp.add_suffix("collapsed");
            
            inp.dataframe = bynaics
            rc.report_output(inp)
        yield rc
        
    def highlight(self, rc):
        import string
        pd.set_option("display.width",120)
        pd.set_option("display.max_colwidth",120)
        log.status("pC25:highlight")
        startcol = rc["start"] if rc["start"] else "p1_4"
        endcol = rc["end"] if rc["end"] else "p1000_4"
        counties = GeneralData.create_data_object("msa_county_reference12.h5")
        naicsinfo = GeneralData.create_data_object("6-digit_2012_Codes.h5")
        cdf = counties.dataframe
        ndf = naicsinfo.dataframe
        for inp in rc.get_inputs():
            df = inp.dataframe
            cmap = business_cols(df)
            busy = df[df["est"]>100]
            maxind = busy.loc[:, startcol:endcol].idxmax()
            log.status("numrows=%d" %len(inp.dataframe))
            #log.status("maxind=\n%s" % maxind)
            for (key,val) in maxind.iteritems():
                log.status("====\nmax %s companies of this size = %s %s%%" % (key,
                                                                            df[cmap[key]].iloc[val],
                                                                            df[key].iloc[val]*100));
                msa = df["msa"].iloc[val]
                log.status("msa   = %s %s " % (msa,
                                                COLORSTR(cdf[cdf["msa"]==msa].iloc[0]["name_msa"],
                                                        attrs=["bold"])
                                              ))
                naicsstr = df["naics"].iloc[val]
                cnt = naicsstr.count("/")
                order = pow(10,cnt)
                naics = naicsstr.replace("/","0")
                
                try:
                    naics = int(naics)
                    nline = ndf[ndf.iloc[:,0] >= naics][ndf.iloc[:,0] < naics+order]
                except:
                    nline = "couldn't find"
                    pass
                
                log.status("naics = %s\n%s" % (naicsstr, nline))
                
        yield rc

    def sort(self, rc):
        import string
        
        colname = rc.get("column", default = None)
        
        for inp in rc.get_inputs():
            df = inp.dataframe
        
            if colname == None:
                colname = df.columns[0]
        
            df = df.sort(colname, ascending=False)
            inp.dataframe = df
            rc.report_output(inp)
        yield rc
        
    def summarize_naics(self, rc):
        column = rc.get("column", None)
        start = rc.get("start", None)
        end = rc.get("end", None)
          
        log.status("column = %s" % column)      
        # get min and max
        for inp in rc.get_inputs():
            
            df = inp.dataframe
            allcolumns = []
            if not column and not start and not end:
                allcolumns = [str(df.columns[0])]
            elif start and not end:
                allcolumns = list(df[:,start:].columns)
            elif end and not start:
                allcolumns = list(df[:,:end].columns)
            elif end and start:
                allcolumns = list(df[:,start:end].columns)
            elif column:
                if column == "*":
                    allcolumns = list(df.columns)
                else:
                    allcolumns = [column]
            
            for column in allcolumns:
                #maxind = df[column].idxmax()
                #minind = df[column][df[column]>0].idxmin()
                
                
                maxrows = df[:6]
                minrows = df[-6:]
                median = df[column].median()
                medians = df[df[column]>=median][-6:]
                mean = df[column].mean()
                means = df[df[column]>=mean][-6:]
                #log.status("%s"%means)
                #newdf = newdf.append([df.loc[maxind], df.loc[minind]])
                #newdf = newdf.append(maxrows)
                #newdf = newdf.append(medians)
                #newdf = newdf.append(means)
                #newdf = newdf.append(minrows)
                #newdf = newdf.sort(column, ascending=False)
                
                #out1 = PandasData(maxrow)
                #out2 = PandasData(minrow)
                
                #df.est >= maxest]
                medians.append(means)
                for tdkey,tdf in [("maxrows",maxrows), 
                                   ("middle", medians),
                                   ("minrows",minrows)]:
                    ninp = PandasData(tdf)
                    ninp.dataframe = tdf
                    ninp.filename = inp.filename
                    ninp.add_suffix("%s-%s-summary" % (column, tdkey))
                    rc.report_output(ninp)
        yield rc

    def naics_interpret(self, rc):
        pd.set_option("display.width",120)
        pd.set_option("display.max_colwidth",120)
        counties = GeneralData.create_data_object("msa_county_reference12.h5")
        naicsinfo = GeneralData.create_data_object("6-digit_2012_Codes.h5")
        cdf = counties.dataframe
        ndf = naicsinfo.dataframe
    
        for inp in rc.get_inputs():
            df = inp.dataframe
            df = df.reset_index()
            cols = list(df.columns)
            cols[0] = "naics"
            df.columns = cols
            
            df["industry"] = df["naics"]
            
            ncols = list(df.columns.values)
            cols = [ncols[-1]]
            cols.extend(ncols[:-1])
            df = df[cols]
            
            for i in range(len(df)):
                naics = df.iloc[i]["naics"]
                naics = int(naics)
                industry = ndf[ndf.iloc[:,0] == naics].iloc[0,1]
                
                
                df["industry"].iloc[i] = industry 
                cols = list(df.columns)
                #log.status("naics = %s" % naics)
                #log.status("industry = %s" % industry)
                #log.status("%s" % df.iloc[i])
            inp.dataframe = df
            rc.report_output(inp)
            
        yield rc
