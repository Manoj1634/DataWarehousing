import pymongo
import time


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.admin  
collection = db.p2  


pipeline = [
    {
        "$match": {
            "Drug Category": { "$in": ["S", "I"] }
        }
    },
    {
        "$group": {
            "_id": {
                "Drug_Category": "$Drug Category",
                "Drug_Type_Indicator": "$Drug Type Indicator",
                "Year": "$Year",
                "Quarter": "$Quarter"
            },
            "Category_Count": { "$sum": 1 },
            "Manufacturers": { "$addToSet": "$Labeler Name" },
            "Product_Count": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "Drug_Category": "$_id.Drug_Category",
            "Drug_Type_Indicator": "$_id.Drug_Type_Indicator",
            "Year": "$_id.Year",
            "Quarter": "$_id.Quarter",
            "Category_Count": 1,
            "Manufacturer_Count": { "$size": "$Manufacturers" },
            "Product_Count": 1,
            "_id": 0
        }
    },
    {
        "$sort": {
            "Drug_Category": 1,
            "Drug_Type_Indicator": 1,
            "Year": 1,
            "Quarter": 1
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
