# commented out the setref lines b/c this is not handled correctly.
# SetRefData needs to play along, when a .setref is loaded, and
# actually forward itself to the correct object type if the data
# is present (in the file or associated files).
# It must also behave gracefully when the data is not present.


data_object_classes = {
    "fits": ("astrodata.AstroData", "AstroData"),
    "txt": ("jsondata", "TxtData"),
    "xls": ("pandasdata", "PandasData"),
    "csv": ("pandasdata", "PandasData"),
    "h5": ("pandasdata", "PandasData"),
    #"setref": ("jsondata", "ReferenceOnlyData"),
    "dmo": ("cubedata", "CubeDemoData"),
    "dmobson":("cubedata", "CubeDemoData")
    }
    
data_object_precedence = [ 
                            "fits", 
                            "txt",
                            "xls",
                            "csv",
                            "h5",
                            "dmobson",
                            "dmo",
                            #"setref"
                          ]