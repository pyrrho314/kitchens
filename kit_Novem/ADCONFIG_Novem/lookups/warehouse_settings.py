from fspackage import FSPackage

warehouse_package = [{"filesystem":FSPackage}]
warehouse_elements = {
    "root":            "/data_warehouse",
    }

shelf_addresses = {
    "setref_data":  
        {
            "path_templ" : "{root}/general_data/{user}/{type}",
            "requires"   : ["root", "user"]
        },
    "misc_data":
        {
            "path_templ" : "{root}/misc_data/{user}/{type}",
            "requires" : ["user", "root"]
        }
    }

type_shelf_names = {
    "SETREF": "setref_data",
    "TABLE": "setref_data"
}

type_store_precedence = [ "TABLE", "SETREF" ]


