from pymongo import MongoClient
from .abstractstorage import StorageAbstract


class MongoDatabase:
    instance = None

    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(*args, **kwargs)
        return cls.instance

    def __init__(self):
        URI = 'mongodb://docker:mongopw@localhost:49153'  # Host=localhost , Port=49153
        self.client = MongoClient(URI)
        self.database = self.client['crawler']


class MongoStorage(StorageAbstract):

    def __init__(self):
        self.mongo = MongoDatabase()

    async def store(self, data: list | dict, collection: str, *args) -> None:
        collection = self.mongo.database[f'{collection}']
        if isinstance(data, list) and len(data) > 1:
            collection.insert_many(data)
        else:
            collection.insert_one(data)
