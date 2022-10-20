from pymongo import MongoClient


class MongoDBClient:
    def __init__(self) -> None:
        self._mongo_client = MongoClient('mongodb://localhost:27017')
        self._db = self._mongo_client["maps"]
        self._col = self._db['SPB']

        print(self._col.find_one())
