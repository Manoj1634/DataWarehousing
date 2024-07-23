import time
from pymongo import MongoClient


mongo_uri = "mongodb://localhost:27017/"
database_name = "config"
collection_name = "dd"

delete_start_time = time.time()
client = MongoClient(mongo_uri)
db = client[database_name]


result = db[collection_name].delete_many({})


if result.deleted_count > 0:
    print(f"{result.deleted_count} documents deleted successfully.")
else:
    print("No documents found to delete.")



delete_end_time = time.time()
delete_time_taken = delete_end_time - delete_start_time

print(f"Deletion time: {delete_time_taken} seconds")
client.close()