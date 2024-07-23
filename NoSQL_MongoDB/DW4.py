from pymongo import MongoClient
import time
import pymongo

client = MongoClient("mongodb://localhost:27017/")


db = client.admin  

db.Manufacturers.create_index([('ID', pymongo.ASCENDING)])
db.Drugs.create_index([('ID', pymongo.ASCENDING)])


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
        "$match": {
            "DrugInfo.Drug Category": "S"
        }
    },
    {
        "$group": {
            "_id": "$Labeler Name",
            "Product_Count": {"$sum": 1}
        }
    },
    {
        "$sort": {"Product_Count": -1}
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
results = list(db.Manufacturers.aggregate(pipeline, allowDiskUse=True))
end_time = time.time()

execution_time = end_time - start_time
print(f"The execution time of the pipeline is {execution_time} seconds.")

# Print the results
#for result in results:
 #   print(result)
