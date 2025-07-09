import json
from datetime import datetime
from pymongo import MongoClient

# Load artist schedule JSON
with open("data/artist_schedule.json", "r") as f:
    schedule = json.load(f)

# Parse timestamps
for artist in schedule:
    artist["start"] = datetime.strptime(artist["start"], "%Y-%m-%d %H:%M:%S")
    artist["end"] = datetime.strptime(artist["end"], "%Y-%m-%d %H:%M:%S")

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["coachella_db"]
collections = ["tweets_week1", "tweets_week2"]

# Assign artists to each tweet
for collection_name in collections:
    collection = db[collection_name]
    print(f"Processing {collection_name}...")

    for doc in collection.find({}, {"_id": 1, "tweeted_at_pst": 1}):
        time_str = doc.get("tweeted_at_pst")
        if not time_str:
            continue
        try:
            tweet_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S%z")
            tweet_time = tweet_time.astimezone(tz=None).replace(tzinfo=None)
        except Exception as e:
            print(f"Skipping malformed date: {time_str}")
            continue

        # Find all matching artists
        artists_playing = [
            artist["artist"]
            for artist in schedule
            if artist["start"] <= tweet_time <= artist["end"]
        ]

        # Update MongoDB 
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"artists_playing": artists_playing}}
        )

print("All tweets updated with artist information.")
