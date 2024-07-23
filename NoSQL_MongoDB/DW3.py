from pymongo import MongoClient
import time
import pymongo

client = MongoClient("mongodb://localhost:27017/")


db = client.admin  

db.Manufacturers.create_index([('ID', pymongo.ASCENDING)])
db.FDA.create_index([('ID', pymongo.ASCENDING)])


pipeline = [
    {
        "$lookup": {
            "from": "Manufacturers",
            "localField": "ID",
            "foreignField": "ID",
            "as": "ManufacturerInfo"
        }
    },
    {
        "$unwind": "$ManufacturerInfo"
    },
    {
        "$group": {
            "_id": "$FDA Application Number",
            "Associated_Labelers": {"$addToSet": "$ManufacturerInfo.Labeler Code"}
        }
    },
    {
        "$project": {
            "FDA_Application_Number": "$_id",
            "_id": 0,
            "Associated_Labelers_Count": {"$size": "$Associated_Labelers"}
        }
    },
    {
        "$match": {
            "Associated_Labelers_Count": {"$gt": 1}
        }
    },
    {
        "$sort": {"Associated_Labelers_Count": -1}
    }
]


start_time = time.time()
results = list(db.FDA.aggregate(pipeline, allowDiskUse=True))
end_time = time.time()

#print(len(results))
execution_time = end_time - start_time
print(f"The execution time of the pipeline is {execution_time} seconds.")

# Print the results
#for result in results:
 #   print(result)
