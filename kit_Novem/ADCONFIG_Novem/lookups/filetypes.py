data_object_classes = {
    "fits": ("astrodata.AstroData", "AstroData"),
    "txt": ("jsondata", "TxtData"),
    "xls": ("pandasdata", "PandasData"),
    "csv": ("pandasdata", "PandasData"),
    "h5": ("pandasdata", "PandasData"),
    "setref": ("jsondata", "ReferenceOnlyData"),
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
                            "setref"
                          ]