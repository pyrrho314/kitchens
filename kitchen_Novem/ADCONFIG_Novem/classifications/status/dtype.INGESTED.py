class INGESTED(DataClassification):
    name = "INGESTED"
    usage = """
            Used as requirement for any more adapted type
            """
    requirement = PROPERTY("_data.ingested", True)
    
class UNINGESTED(DataClassification):
    name = "UNINGESTED"
    usage = """ 
            Generally exist to assign ingest recipe too, every type needing it's own recipe
            """
    requirement = NOT(PROPERTY("_data.ingested", True))
