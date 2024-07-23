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
            "_id": {
                "Drug_Category": "$Drug Category",
                "Drug_Type_Indicator": "$Drug Type Indicator",
                "Year": "$ManufacturerInfo.Year",
                "Quarter": "$ManufacturerInfo.Quarter"
            },
            "Category_Count": {"$sum": 1},
            "Manufacturer_Count": {"$addToSet": "$ManufacturerInfo.Labeler Name"},
            "Product_Count": {"$sum": 1}
        }
    },
    {
        "$project": {
            "Drug_Category": "$_id.Drug_Category",
            "Drug_Type_Indicator": "$_id.Drug_Type_Indicator",
            "Year": "$_id.Year",
            "Quarter": "$_id.Quarter",
            "Category_Count": 1,
            "Manufacturer_Count": {"$size": "$Manufacturer_Count"},
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
results = list(db.Drugs.aggregate(pipeline, allowDiskUse=True))
end_time = time.time()

execution_time = end_time - start_time
print(f"The execution time of the pipeline is {execution_time} seconds.")

# Print the results
#for result in results:
 #   print(result)
