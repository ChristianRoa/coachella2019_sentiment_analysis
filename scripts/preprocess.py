import nltk
from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
from nltk.stem import WordNetLemmatizer
from pymongo import MongoClient

# Download required NLTK data
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

# Load stopword sets
stop_words_en = set(stopwords.words('english'))
stop_words_es = set(stopwords.words('spanish'))
stop_words = stop_words_en.union(stop_words_es)

# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

def preprocess(text):
    if not isinstance(text, str):
        return ""

    tokens = wordpunct_tokenize(text)                          # Tokenize
    tokens = [t for t in tokens if t.lower() not in stop_words]  # Remove stopwords
    tokens = [lemmatizer.lemmatize(t.lower()) for t in tokens]   # Lemmatize and lowercase
    return " ".join(tokens)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["coachella_db"]
collections = ["tweets_week1", "tweets_week2"]

for collection_name in collections:
    print(f"Preprocessing collection: {collection_name}")
    collection = db[collection_name]

    for doc in collection.find({}, {"_id": 1, "clean_text": 1}):
        clean = doc.get("clean_text", "")
        processed = preprocess(clean)

        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "processed_text": processed
            }}
        )

print("Preprocessing complete!")
