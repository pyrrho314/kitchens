class JSON(DataClassification):
    name = "JSON"
    usage = """
        Applies to all JSON structures if they are stored with "<fname>.json" naming convention,
        OR if they have a .json member
        """
    parent = None
    requirement = FILENAME(".*?.json") | HASMEMBER("json")
    
class TXT(DataClassification):
    name = "TXT"
    usage = """
            Applies to filename recognition *.txt.
            """
    parent = None
    requirement = OR( FILENAME(".*?\.txt"),
                      FILENAME(".*?\.csv"))
                      
    
class TABLE(DataClassification):
    name = "TABLE"
    usage = """
            Applies to objects that have a .json[table] member
            """
    suggestedDataObj = [("jsondata", "PandasData")]

class SETREF(DataClassification):
    name = "SETREF"
    usage  = """
                refers to other data via a *.setref
             """
    requirement = FILENAME(".*?\.setref") 
