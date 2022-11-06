from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


class MongoDBClient:
    def __init__(self, host, db_name) -> None:
        self._mongo_client : MongoClient = MongoClient(host)
        self._db : Database  = self._mongo_client[db_name]
        self._col : Collection = None
        self._db_name : str = db_name
        self._collections : list = self._db.list_collection_names()
        self._cur_collection : str = None

    def add_json(self, json_data, collection_name=None) -> None:
        col_name = self._cur_collection if collection_name is None \
            else collection_name

        assert col_name in self._collections

        col = self._get_collection(collection_name=col_name)
        col.insert_many(json_data)

    def create_collection(self, new_collection) -> None:
        self._collections.append(new_collection)

    def set_working_collection(self, collection_name) -> None:
        assert collection_name in self._collections

        self._cur_collection = collection_name

    def get_collection(self, collection_name) -> Collection:
        assert collection_name in self._collections
        
        return self._get_collection(collection_name=collection_name)

    def _get_collection(self, collection_name) -> Collection:
        return self._db[collection_name] if collection_name in self._collections \
            else None

