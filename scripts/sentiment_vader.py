from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient

# Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["coachella_db"]
collections = ["tweets_week1", "tweets_week2"]

def get_label(score):
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"

for collection_name in collections:
    print(f"Analyzing sentiment for collection: {collection_name}")
    collection = db[collection_name]

    for doc in collection.find({}, {"_id": 1, "processed_text": 1}):
        text = doc.get("processed_text", "")
        if not text:
            continue

        score = analyzer.polarity_scores(text)["compound"]
        label = get_label(score)

        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "sentiment_score": score,
                "sentiment_label": label
            }}
        )

print("Sentiment analysis complete!")
