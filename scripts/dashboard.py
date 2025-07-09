import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
from pymongo import MongoClient
from collections import Counter, defaultdict

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["coachella_db"]
collections = ["tweets_week1", "tweets_week2"]

# Data Containers
all_words = []
all_hashtags = []
all_sentiments = []
artist_tweet_count = defaultdict(int)
artist_sentiment_scores = defaultdict(list)

# Extract & Aggregate Data
for collection_name in collections:
    collection = db[collection_name]
    for doc in collection.find({}, {"processed_text": 1, "hashtags_list": 1, "sentiment_label": 1, "sentiment_score": 1, "artists_playing": 1}):
        # Word frequency
        text = doc.get("processed_text", "")
        if isinstance(text, str):
            all_words.extend(text.split())

        # Hashtags
        hashtags = doc.get("hashtags_list", [])
        if isinstance(hashtags, list):
            all_hashtags.extend(hashtags)

        # Sentiment label
        sentiment = doc.get("sentiment_label", None)
        if sentiment:
            all_sentiments.append(sentiment)

        # Artist tweet volume and sentiment
        artists = doc.get("artists_playing", [])
        score = doc.get("sentiment_score", None)
        if isinstance(artists, list) and isinstance(score, (float, int)):
            for artist in artists:
                artist_tweet_count[artist] += 1
                artist_sentiment_scores[artist].append(score)

# Top 20 Calculations
top_words = Counter(all_words).most_common(20)
top_hashtags = Counter(all_hashtags).most_common(20)
sentiment_dist = Counter(all_sentiments)

artist_avg_sentiment = {
    artist: sum(scores) / len(scores) for artist, scores in artist_sentiment_scores.items()
    if scores
}

df_words = pd.DataFrame(top_words, columns=["Word", "Frequency"])
df_hashtags = pd.DataFrame(top_hashtags, columns=["Hashtag", "Frequency"])
df_sentiment = pd.DataFrame(sentiment_dist.items(), columns=["Sentiment", "Count"])
df_volume = pd.DataFrame(artist_tweet_count.items(), columns=["Artist", "Tweet Volume"]) \
            .sort_values(by="Tweet Volume", ascending=False).head(20)
df_avg_sentiment = pd.DataFrame(artist_avg_sentiment.items(), columns=["Artist", "Average Sentiment"]) \
                    .sort_values(by="Average Sentiment", ascending=False).head(20)

# Dash App
app = dash.Dash(__name__)
app.title = "Coachella Sentiment Dashboard"

app.layout = html.Div(children=[
    html.H1("Coachella 2019 Sentiment Analysis Dashboard"),

    dcc.Graph(
        figure=px.bar(df_words, x="Word", y="Frequency", title="Top 20 Words")
    ),

    dcc.Graph(
        figure=px.bar(df_hashtags, x="Hashtag", y="Frequency", title="Top 20 Hashtags")
    ),

    dcc.Graph(
        figure=px.bar(df_sentiment, x="Sentiment", y="Count", title="Sentiment Distribution")
    ),

    dcc.Graph(
        figure=px.bar(df_volume, x="Tweet Volume", y="Artist", orientation='h', title="Top 20 Artists by Tweet Volume")
    ),

    dcc.Graph(
        figure=px.bar(df_avg_sentiment, x="Average Sentiment", y="Artist", orientation='h', title="Top 20 Artists by Average Sentiment")
    )
])

if __name__ == '__main__':
    app.run(debug=True)
