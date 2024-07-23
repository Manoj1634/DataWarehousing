import pymongo
import time


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.admin  
collection = db.dd 


pipeline = [
    {
        "$group": {
            "_id": "$Labeler Name",
            "Total_Products": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "Total_Products": -1
        }
    },
    {
        "$project": {
            "Labeler_Name": "$_id",
            "Total_Products": 1,
            "_id": 0
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

