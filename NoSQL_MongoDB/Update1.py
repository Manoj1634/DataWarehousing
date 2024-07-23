from pymongo import MongoClient
import time


client = MongoClient("mongodb://localhost:27017/")


db = client.config  
collection = db.dd


update_operation = {
    "$set": {
        "Labeler Name": "Eli and Company"
    }
}


start_time = time.time()


result = collection.update_many({}, update_operation)


end_time = time.time()
execution_time = end_time - start_time


print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")
print(f"The execution time of the update operation is {execution_time} seconds.")