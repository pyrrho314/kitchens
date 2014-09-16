from astrodata.ReductionObjects import PrimitiveSet
from astrodata.adutils import logutils, ksutil
log = logutils.get_logger(__name__)

class SetRefPrimitives(PrimitiveSet):
    astrotype = "JSON"
    
    def goInteractive(self, rc):
        import code
        code.interact(local=locals())
        yield rc
    
    def optimizeData(self, rc):
        
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
        inps = rc.get_inputs(); # print "primitives_NOVEM: JSONPrimitives.helloWorld(..)"
        log.stdinfo("%d inputs" % len(inps))
        for inp in inps:
            if rc["use_repr"]:
                tstr = repr(inp.json)
            else:
                tstr = inp.pretty_string()
            log.stdinfo("data types: %s" % repr(inp.get_types()))
            log.stdinfo(tstr)
        yield rc
        
    def seekRelationships(self, rc):
        inps = rc.get_inputs()
        yield rc
    
