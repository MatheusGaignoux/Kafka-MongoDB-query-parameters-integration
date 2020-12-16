from time import sleep
from threading import Thread
from consumer import QueryConsumer
from querier import QueryMongo

class Pipe(QueryConsumer):
    def __init__(self):
        super().__init__()
        self._mongo = QueryMongo()
        self._switch = False
        self._response = None
        self._queue = []
    
    def _trigger(self):      
        while self._switch:         
            self._query = self._pollMsg()
            sleep(1)

    @property
    def response(self):
        return self._response

    @property
    def queue(self):
        return self._queue

    @property
    def on(self):
        self._switch = True
        self.thread = Thread(target = self._trigger)
        self.thread.start()

        return self
        
    @property
    def off(self):
        self._switch = False
        sleep(5)
        self.thread.join()
        
        return self
    
    @property
    def _query(self):
        return self._msg
        
    @_query.setter
    def _query(self, msg):
        self._msg = msg
        if self._msg is not None:
            self._response = self._mongo.query(self._msg.value())
            self._queue.append(self._response)