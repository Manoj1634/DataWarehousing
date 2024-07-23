import pymongo
import time


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.admin  
collection = db.dd  


pipeline = [
    {
        "$match": {
            "Drug Category": "S"
        }
    },
    {
        "$group": {
            "_id": "$Labeler Name",
            "Product_Count": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "Product_Count": -1
        }
    },
    {
        "$project": {
            "Labeler_Name": "$_id",
            "_id": 0,
            "Product_Count": 1
        }
    }
]


start_time = time.time()
results = list(collection.aggregate(pipeline))
end_time = time.time()

execution_time = end_time - start_time
print(f"The execution time of the pipeline is {execution_time} seconds.")

#Print results
#for result in results:
#    print(result)
