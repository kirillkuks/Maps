from database import MongoDBClient


class MapsDB(MongoDBClient):
    def __init__(self) -> None:
        super().__init__(
            host='mongodb://localhost:27017',
            db_name='maps',
        )
