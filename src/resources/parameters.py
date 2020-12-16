params = {
            "producerconfig": {
                            "bootstrap.servers":"localhost:9092"
                            , "schema.registry.url":"http://localhost:8081"
            },
            "consumerconfig": {                
                            "bootstrap.servers":"localhost:9092"
                            , "schema.registry.url":"http://localhost:8081"
                            , "group.id":"store"
                            , "default.topic.config": {"auto.offset.reset":"earliest"}
                            , "max.poll.interval.ms":"900000"
            },
            "mongoconfig": {
                            "host":"192.168.0.15"
                            , "port":27017
                            , "username":"mongo"
                            , "password":"mongo"
                            , "db":"store"
                            , "collection":"products"
            }
            , "topic": "queryparams"
}