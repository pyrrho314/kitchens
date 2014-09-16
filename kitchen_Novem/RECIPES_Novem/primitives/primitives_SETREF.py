from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
from astrodata.AstroDataType import globalClassificationLibrary as gCL

from astrodata.generaldata import GeneralData

log = logutils.get_logger(__name__)

class SetRefPrimitives(PrimitiveSet):
    astrotype = "JSON"
    
    def goInteractive(self, rc):
        import code
        code.interact(local=locals())
        yield rc
    
    def adaptSetType(self, rc):
        for inp in rc.get_inputs():
            hints = inp.get("type_hints")
            for hint in hints:
                mandc = gCL.recommend_data_object(hint)
                if mandc:
                    log.stdinfo("pSR20:", mandc)
                    mod,clas = mandc
                    result = "module=%s class=%s" % (mod, clas)
                    try:
                        newset = GeneralData.create_data_object(inp, hint=mandc)
                        newset.add("types", hint)
                        rc.report_output(newset)
                    except:
                        raise
                else:
                    result = "..no hint.."
                log.stdinfo("pSR18: %s -> %s" % (hint, result))
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
        for inp in inps:
            if rc["use_repr"]:
                tstr = repr(inp.json)
            else:
                tstr = inp.pretty_string()
            log.stdinfo(tc.colored("data types:", attrs=["bold"]) + " %s" % repr(inp.get_types()))
            log.stdinfo(tc.colored("data_obj:",attrs=["bold"])    + " %s" % repr(inp))
            log.stdinfo(tstr)
        yield rc
    
    def writeOutputs(self, rc):
        for inp in rc.get_inputs():
            pass # @@workpoint
    def seekRelationships(self, rc):
        inps = rc.get_inputs()
        yield rc
    
