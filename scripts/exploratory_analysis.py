import pandas as pd
from pymongo import MongoClient
from collections import Counter
import matplotlib.pyplot as plt
from collections import defaultdict

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["coachella_db"]
collections = ["tweets_week1", "tweets_week2"]

# Containers for analysis
all_words = []
all_hashtags = []
all_sentiments = []

# Extract data
for collection_name in collections:
    collection = db[collection_name]
    for doc in collection.find({}, {"processed_text": 1, "hashtags_list": 1, "sentiment_label": 1}):
        processed_text = doc.get("processed_text", "")
        if isinstance(processed_text, str):
            all_words.extend(processed_text.split())

        hashtags = doc.get("hashtags_list", [])
        if isinstance(hashtags, list):
            all_hashtags.extend(hashtags)

        sentiment = doc.get("sentiment_label", "")
        if sentiment:
            all_sentiments.append(sentiment)

# Count top elements
top_words = Counter(all_words).most_common(20)
top_hashtags = Counter(all_hashtags).most_common(20)
sentiment_dist = Counter(all_sentiments)

# Plotting
def plot_bar(data, title, xlabel, ylabel):
    labels, values = zip(*data)
    plt.figure(figsize=(10, 6))
    plt.bar(labels, values, color='skyblue')
    plt.xticks(rotation=45, ha='right')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.show()

plot_bar(top_words, "Top 20 Words", "Words", "Frequency")
plot_bar(top_hashtags, "Top 20 Hashtags", "Hashtags", "Frequency")
plot_bar(sentiment_dist.items(), "Sentiment Distribution", "Sentiment", "Count")

# Artist-level Analysis: Tweet Volume & Sentiment Score
artist_tweet_count = defaultdict(int)
artist_sentiment_scores = defaultdict(list)

for collection_name in collections:
    collection = db[collection_name]
    for doc in collection.find({}, {"artists_playing": 1, "sentiment_score": 1}):
        artists = doc.get("artists_playing", [])
        score = doc.get("sentiment_score", None)
        if not isinstance(artists, list) or score is None:
            continue
        for artist in artists:
            artist_tweet_count[artist] += 1
            artist_sentiment_scores[artist].append(score)

# Compute average sentiment per artist
artist_avg_sentiment = {
    artist: sum(scores) / len(scores) for artist, scores in artist_sentiment_scores.items()
    if scores
}

# DataFrames
df_volume = pd.DataFrame(artist_tweet_count.items(), columns=["Artist", "Tweet Volume"]) \
            .sort_values(by="Tweet Volume", ascending=False).head(20)

df_avg_sentiment = pd.DataFrame(artist_avg_sentiment.items(), columns=["Artist", "Average Sentiment"]) \
                   .sort_values(by="Average Sentiment", ascending=False).head(20)

# Plotting
plt.figure(figsize=(10, 6))
plt.barh(df_volume["Artist"], df_volume["Tweet Volume"])
plt.title("Top 20 Artists by Tweet Volume")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
plt.barh(df_avg_sentiment["Artist"], df_avg_sentiment["Average Sentiment"], color='green')
plt.title("Top 20 Artists by Average Sentiment Score")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()
