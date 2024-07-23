import pymongo
import time


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.admin  
collection = db.p2 


pipeline = [
    {
        "$group": {
            "_id": "$Labeler Name",
            "Product_Count": { "$sum": 1 },
            "FDA_Application_Numbers": { "$addToSet": "$FDA Application Number" }
        }
    },
    {
        "$project": {
            "Labeler_Name": "$_id",
            "_id": 0,
            "Product_Count": 1,
            "FDA_Count": { "$size": "$FDA_Application_Numbers" }
        }
    },
    {
        "$match": {
            "Product_Count": { "$gt": 5 },
            "FDA_Count": { "$gt": 3 }
        }
    },
    {
        "$sort": {
            "Product_Count": -1
        }
    }
]


start_time = time.time()
results = list(collection.aggregate(pipeline))
end_time = time.time()

execution_time = end_time - start_time
print(f"The execution time of the pipeline is {execution_time} seconds.")

# Print results
#for result in results:
    #print(result)
