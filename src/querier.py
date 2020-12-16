import pymongo as mongo
from resources.parameters import params

class QueryMongo:
    def __init__(self):
        self._params = params["mongoconfig"]
        self._initCollection()
    
    def _initCollection(self):
        self._conn = mongo.MongoClient(host = self._params["host"]
                                      , port = self._params["port"]
                                      , username = self._params["username"]
                                      , password = self._params["password"]
                                      )
        
        db = self._conn[self._params["db"]]
        self._collection = db[self._params["collection"]]
    
    def query(self, params):
        queryFilter = [{"$match": {"brand": params["brand"]
                                  , "name": params["itemName"]
                                  , "subname": params["itemSpec"]
                                }
                            }
                      , {"$unwind": "$info"}
                      , {"$match" : {"info.color": params["color"]
                                    , "info.size": params["size"]
                                    , "info.qtt": {"$gte":1}
                                }
                            }
                      , {"$project":{"brand": 1
                                    , "name": 1
                                    , "subname": 1
                                    , "price": 1
                                    , "info.color": 1
                                    , "info.size": 1
                                    , "info.locale": 1
                                    , "info.qtt": 1
                                    , "_id": 0
                                }
                        }]
        
        return list(self._collection.aggregate(queryFilter))