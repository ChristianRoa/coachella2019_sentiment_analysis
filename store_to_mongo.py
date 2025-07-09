import pandas as pd
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["coachella_db"]

# Weekend 1 
csv_file_1 = 'data/Coachella_2019_Tweets_Weekend_1_2019-04-07_to_2019-04-16.csv'
df1 = pd.read_csv(csv_file_1, encoding='utf-8')
collection1 = db["tweets_week1"]
collection1.insert_many(df1.to_dict(orient="records"))
print(f"Inserted {len(df1)} tweets into 'tweets_week1' collection.")

# Weekend 2 
csv_file_2 = 'data/Coachella_2019_Tweets_Weekend_2_2019-04-14_to_2019-04-23.csv'
df2 = pd.read_csv(csv_file_2, encoding='utf-8')
collection2 = db["tweets_week2"]
collection2.insert_many(df2.to_dict(orient="records"))
print(f"Inserted {len(df2)} tweets into 'tweets_week2' collection.")
