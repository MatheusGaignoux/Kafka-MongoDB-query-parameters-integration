from resources.parameters import params
from confluent_kafka.avro import AvroConsumer

class QueryConsumer:
    def __init__(self):
        self._params = params
        self._initConsumer()
    
    def _initConsumer(self):
        self._consumer = AvroConsumer(config = self._params["consumerconfig"])
        self._consumer.subscribe([self._params["topic"]])
        
    def _pollMsg(self):
        msg = self._consumer.poll(0.1)
        return msg

    def close(self):
        self._consumer.close()