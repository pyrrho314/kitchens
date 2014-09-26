from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
from astrodata.AstroDataType import globalClassificationLibrary as gCL

from astrodata.generaldata import GeneralData

log = logutils.get_logger(__name__)

class SetRefPrimitives(PrimitiveSet):
    astrotype = "SETREF"
    
    # serious
    def stop(self, rc):
        import sys
        sys.exit()
    # regular     
    def adaptSetType(self, rc):
        for inp in rc.get_inputs():
            
            rec = inp.recommend_data_object()
            log.stdinfo("adaptSetType: recommended dataset object %s" % rec)
            mandc = None
                
            if rec:
                # we'll take the random first if there is more than one recommendation, atm
                    
                typ = None
                for typ in rec:
                    mandc = rec[typ]
                    if mandc:
                        break
                
            if mandc:
                mod,clas = mandc
                result = "module=%s class=%s" % (mod, clas)
                try:
                    newset = GeneralData.create_data_object(inp, hint=mandc)
                    newset.add("types", typ)
                    rc.report_output(newset)
                except:
                    rc.report_output(inp, stream="")
                    raise
            else:
                result = "..no recommendation.."
            log.info("(pSR18) %s-> %s" % (inp.basename, result))
        yield rc
    
    def filterOutNot(self, rc):
        for inp in rc.get_inputs():
            typs = inp.get_types()
            outtyp = rc["settype"]
            if outtyp in typs:
                log.info("%s stays" % inp.basename)
                rc.report_output(inp)
            else:
                notstream = "not_%s" % outtyp
                log.info("%s goes in stream '%s'" % (inp.basename, notstream))
                #rc.report_output(inp, stream= notstream)
        yield rc
    def goInteractive(self, rc):
        import code
        code.interact(local=locals())
        yield rc
    
    def markAsIngested(self, rc):
        inps = rc.get_inputs()
        
        for inp in inps:
            inp.prop_put("_data.ingested", True)
            inp.prop_put("_data.ingested_as", inp.get_types())
            rc.report_output(inp)
        log.stdinfo("marked %d dataset%s" % (len(inps),
                                             "" if len(inps)==1 else "s") )
        yield rc 
    
    ## NATIVESTORAGE ###########
    def nativeStorage(self, rc):
        
        retv = None
        for inp in rc.get_inputs():
            origname = inp.filename
            inp.load()
            retv = inp.nativeStorage()
            if retv:
                log.info("changed to nativeStorage for \n\t   %s \n\tto %s" % (origname, inp.filename))
            else:
                log.info("kept currentStorage for %s" % inp.filename)
        yield rc
           
    def reduceToHeader(self, rc):
        inps = rc.get_inputs()
        for inp in inps:
            log.stdinfo("Removing Raw")
            del(inp.json["table"]["rows"])
            rc.report_output(inp)
        
        yield rc
    
    def relateData(self, rc):
        yield rc
    
    def showInputs(self, rc):
#        log.fullinfo("helloWorld")
        import termcolor as tc
        inps = rc.get_inputs(); # print "primitives_NOVEM: JSONPrimitives.helloWorld(..)"
        log.stdinfo("%d inputs" % len(inps))
        i = 0
        for inp in inps:
            if rc["use_repr"]:
                tstr = repr(inp.json)
            else:
                tstr = inp.pretty_string()
            i += 1
            log.stdinfo(tc.colored("#%d"%i, "grey", "on_white"))
            log.info(tstr)
        yield rc
        
    def writeAndDrop(self, rc):
        settype = rc["settype"]
        if settype == None:
            log.stdinfo("writeAndDrop: nothing to do")
            yield rc.finish()
        for inp in rc.get_inputs():
            if inp.is_type(settype):
                inp.write()
        
    def writeOutputs(self, rc):
        for inp in rc.get_inputs():
            suffix = None
            if rc["suffix"]:
                suffix = "_%s"%suffix
            inp.write(suffix = suffix)
        yield rc
    writeOutput = writeOutputs    
    def seekRelationships(self, rc):
        inps = rc.get_inputs()
        yield rc
    
