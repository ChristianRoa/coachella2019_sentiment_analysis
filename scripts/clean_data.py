import re
from pymongo import MongoClient

def clean_tweet(text):
    if not isinstance(text, str):
        return ""

    text = text.lower()
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)  # Remove URLs
    text = re.sub(r"@\w+", "", text)                     # Remove mentions
    text = re.sub(r"#\w+", "", text)                     # Remove hashtags
    text = re.sub(r"[^\w\s]", "", text)                  # Remove punctuation

    # Remove emojis and other non-text symbols
    emoji_pattern = re.compile(
        "["                     
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002500-\U00002BEF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE
    )
    text = emoji_pattern.sub(r'', text)

    return text.strip()  # Just return cleaned lowercase + punctuation-free string


def extract_hashtags(text):
    if not isinstance(text, str):
        return []
    return [tag.lower() for tag in re.findall(r"#(\w+)", text)]


# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["coachella_db"]

collections = ["tweets_week1", "tweets_week2"]

for collection_name in collections:
    print(f"Cleaning collection: {collection_name}")
    collection = db[collection_name]
    for doc in collection.find({}, {"_id": 1, "full_tweet_text": 1}):
        original_text = doc.get("full_tweet_text", "")
        cleaned = clean_tweet(original_text)
        hashtags = extract_hashtags(original_text)
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {
        "clean_text": cleaned,
        "hashtags_list": hashtags
    }}
        )

print("Cleaning complete!")
