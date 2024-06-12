import nest_asyncio
from ib_insync import *

class IBConnectionSpawn:
    connections = []
    
    def __init__(self, clientId=5):
        self.clientId = clientId
        self.ib_connection = None
    
    def connect(self):
        ib = IB()
        util.startLoop()
        ib.connect(host='127.0.0.1', port=7496, clientId=self.clientId)
        self.ib_connection = ib
        IBConnectionSpawn.connections.append(self.ib_connection)
    
    @classmethod
    def disconnect_all(cls):
        for conn in cls.connections:
            conn.disconnect()
        cls.connections.clear()