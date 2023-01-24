from pymongo import MongoClient


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
