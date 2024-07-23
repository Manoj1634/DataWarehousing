from pymongo import MongoClient
import time
import pymongo

client = MongoClient("mongodb://localhost:27017/")


db = client.admin  

db.Drugs.create_index([('ID', pymongo.ASCENDING)])
db.FDA.create_index([('ID', pymongo.ASCENDING)])

pipeline = [
    {
        "$lookup": {
            "from": "Drugs",
            "localField": "ID",
            "foreignField": "ID",
            "as": "DrugInfo"
        }
    },
    {
        "$unwind": "$DrugInfo"
    },
    {
        "$group": {
            "_id": "$FDA Application Number",
            "Associated_Product_Count": {"$addToSet": "$DrugInfo.Product Code"}
        }
    },
    {
        "$project": {
            "FDA_Application_Number": "$_id",
            "_id": 0,
            "Associated_Product_Count": {"$size": "$Associated_Product_Count"}
        }
    },
    {
        "$match": {
            "Associated_Product_Count": {"$gt": 1}
        }
    },
    {
        "$sort": {"Associated_Product_Count": -1}
    }
]


start_time = time.time()
results = list(db.FDA.aggregate(pipeline, allowDiskUse=True))
end_time = time.time()

execution_time = end_time - start_time
print(f"The execution time of the pipeline is {execution_time} seconds.")

# Print the results
#for result in results:
 #   print(result)

