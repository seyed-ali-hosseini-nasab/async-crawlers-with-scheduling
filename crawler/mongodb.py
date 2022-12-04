from typing import Any

from pymongo import MongoClient
from pymongo.database import Database


def get_database() -> Database[Any]:
    host = 'mongodb://localhost'
    port = '27017'
    url = f'{host}:{port}'
    client = MongoClient(url)
    database = client['crawler']

    return database


async def store(data: list | dict, collection: str) -> None:
    database = get_database()
    collection = database[f'{collection}']

    if isinstance(data, list) and len(data) > 1:
        collection.insert_many(data)
    else:
        collection.insert_one(data)
