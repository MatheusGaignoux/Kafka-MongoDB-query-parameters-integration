import random
import string
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from resources.parameters import params

class QueryProducer:
    def __init__(self):
        self._seed = random.randint(1, 3650)
        self._params = params
        self._schemas()
        self._initProducer()
    
    def _schemas(self):
        self._keySchema = avro.load("schemas/keyschema.avsc")
        self._valueSchema = avro.load("schemas/valueschema.avsc")
            
    
    def _initProducer(self):
        self._producer = AvroProducer(config = self._params["producerconfig"]
                                     , default_key_schema = self._keySchema
                                     , default_value_schema = self._valueSchema
                                     )
        
        self._producer.flush()
        
    def _produce_msg(self, value):
        self._seed += 1
        random.seed(self._seed)
        
        key = "".join(random\
                      .SystemRandom()\
                      .choice(string.ascii_uppercase + string.digits) for _ in range(10)
                      )
        
        self._producer.produce(topic = self._params["topic"]
                              , key = key
                              , value = value
                              )
    
    @property
    def msg(self):
        return self._msg
    
    @msg.setter
    def send(self, msg):
        self._msg = msg
        self._produce_msg(self._msg)