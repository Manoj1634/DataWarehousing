from pymongo import MongoClient
import time
import pymongo

client = MongoClient("mongodb://localhost:27017/")


db = client.admin  

db.Manufacturers.create_index([('ID', pymongo.ASCENDING)])
db.Drugs.create_index([('ID', pymongo.ASCENDING)])
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
        "$lookup": {
            "from": "FDA",
            "localField": "ID",
            "foreignField": "ID",
            "as": "FDAInfo"
        }
    },
    {
        "$unwind": {
            "path": "$ManufacturerInfo",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$unwind": {
            "path": "$FDAInfo",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$match": {
            "ManufacturerInfo.Year": 2023,
            "ManufacturerInfo.Quarter": 3
        }
    },
    {
        "$group": {
            "_id": "$Drug Category",
            "Drug_Count": {"$sum": 1},
            "Manufacturer_Count": {"$addToSet": "$ManufacturerInfo.ID"},
            "FDA_Application_Count": {"$addToSet": "$FDAInfo.ID"}
        }
    },
    {
        "$project": {
            "Drug_Category": "$_id",
            "_id": 0,
            "Drug_Count": 1,
            "Manufacturer_Count": {"$size": "$Manufacturer_Count"},
            "FDA_Application_Count": {"$size": "$FDA_Application_Count"}
        }
    }
]


start_time = time.time()
results = list(db.Drugs.aggregate(pipeline, allowDiskUse=True))
end_time = time.time()

execution_time = end_time - start_time
print(f"The execution time of the pipeline is {execution_time} seconds.")

# Print the results
#for result in results:
 #   print(result)
