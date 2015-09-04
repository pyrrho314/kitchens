
from astrodata.adutils import logutils, ksutil
from astrodata.Lookups import compose_multi_table, get_lookup_table
import base64

log = logutils.get_logger(__name__)
try:
    from pymongo import MongoClient

    class MongoPublisher(object):
        _connection = None
        host = None
        db = None
        user = None
        password = None
        collection_name = None
        collection = None
        
        def __init__(self):
            pass
        
        def configure(self, options):
            collection_name = None
            config_location = None
            config_key = None
            
            if "collection_name" in options:
                collection_name = options["collection_name"]
            if "db_info" in options:
                config_location = options["db_info"]["location"]    
                config_key      = options["db_info"]["key"]    
                config_location = options["db_info"]["location"]    

            mtret = compose_multi_table(config_location, config_key, just_one=True)
            mdbinfo = mtret["mdb_info"]
            
            print ksutil.dict2pretty("mdb_info", mdbinfo)
            self.host     = mdbinfo["host"]
            self.dbname   = mdbinfo["db"]
            self.user     = mdbinfo["user"]
            self.password = base64.b64decode(mdbinfo["password"])

            self.get_connection()
            if collection_name:
                self.collection_name = collection_name
                self.collection = self.db[collection_name]
        ##
        ####
        #####
        ## properties
        #####
        ####
        def get_connection(self):
            """Used to make Mongo connection. Can be called to establish
            connection or as property (i.e. cnx = self.connection). The 
            __init__ will presumably called get_connection initially so 
            generally it will be used as a get property.
            
            There is no set_connection(..) call. 
            """
            if not MongoPublisher._connection:
                connection = MongoClient(self.host)
                connected = connection[self.dbname].authenticate(self.user, self.password)
                MongoPublisher._connection = connection
            connection = MongoPublisher._connection
            self.connected = connection[self.dbname].authenticate(self.user, self.password)
            self.db = connection[self.dbname]
            return MongoPublisher._connection
        # connection property, no setter or delete
        connection = property(get_connection)
        #
        def publish_document(self, doc):
            doclist = None
            if isinstance(doc, list):
                doclist = doc
            else:
                doclist = [doc]
            for doci in doclist:
                self.collection.save(doci)
    
    
except:
    log.warning("cannot load mongo publisher module, is pymongo installed?")