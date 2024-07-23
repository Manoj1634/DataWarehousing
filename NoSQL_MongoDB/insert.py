import time
import pandas as pd
from pymongo import MongoClient


mongo_uri = "mongodb://localhost:27017/"
database_name = "config"
collection_name = "dd"


csv_file_path = "/Users/manojpadala/Desktop/Group6_Section12_Project/final_dataset.csv"


start_time = time.time()


df = pd.read_csv(csv_file_path)


client = MongoClient(mongo_uri)
db = client[database_name]


db[collection_name].insert_many(df.to_dict(orient='records'))


end_time = time.time()
time_taken = end_time - start_time

print(f"Import time: {time_taken} seconds")


client.close()