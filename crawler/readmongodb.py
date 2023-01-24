from databases import MongoDatabase
mongo = MongoDatabase()
# collection = mongo.database['schedule']
# collection.delete_many({'crawler_name': {'$eq': 'offch'}})

# print(mongo.database.list_collection_names())

collection = mongo.database['data_digikala']
collection.delete_many({'id': {'$ne': ''}})
collection = mongo.database['data_mopon']
collection.delete_many({'id': {'$ne': ''}})
collection = mongo.database['time_digikala']
collection.delete_many({'state': {'$ne': ''}})
collection = mongo.database['time_mopon']
collection.delete_many({'state': {'$ne': ''}})
