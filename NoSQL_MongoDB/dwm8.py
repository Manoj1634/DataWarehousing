import pymongo
import time


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.admin  
collection = db.p2  


pipeline = [
    {
        "$match": {
            "Year": 2023,
            "Quarter": 3
        }
    },
    {
        "$group": {
            "_id": "$Drug Category",
            "Drug_Count": { "$sum": 1 },
            "Manufacturer_Count": { "$addToSet": "$Labeler Code" },
            "FDA_Application_Count": { "$addToSet": "$FDA Application Number" }
        }
    },
    {
        "$project": {
            "Drug_Category": "$_id",
            "_id": 0,
            "Drug_Count": 1,
            "Manufacturer_Count": { "$size": "$Manufacturer_Count" },
            "FDA_Application_Count": { "$size": "$FDA_Application_Count" }
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
