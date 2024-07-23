import pymongo
import time


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.admin  
collection = db.p2  


pipeline = [
    {
        "$group": {
            "_id": "$FDA Application Number",
            "Associated_Product_Count": { "$addToSet": "$Product Code" }
        }
    },
    {
        "$project": {
            "FDA_Application_Number": "$_id",
            "_id": 0,
            "Associated_Product_Count": { "$size": "$Associated_Product_Count" }
        }
    },
    {
        "$match": {
            "Associated_Product_Count": { "$gt": 1 }
        }
    },
    {
        "$sort": {
            "Associated_Product_Count": -1
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
 #   print(result)
