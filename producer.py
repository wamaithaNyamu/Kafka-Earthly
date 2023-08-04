import praw
from fake_useragent import UserAgent
import asyncio
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Generate a random userAgent
ua = UserAgent()
userAgent = ua.random

# Load environment variables from .env file
load_dotenv()

# Get the Reddit API credentials from the environment variables
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

# Create the Reddit instance using praw
reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent=userAgent,
)

data = []

for submission in reddit.subreddit("jobs").hot(limit=10):
    data.append({
            "author": submission.author.name,
            "title": submission.title,
            "text": submission.selftext,
            "url": submission.url
        })

main_data = asyncio.run(main())
for x in main_data:
    print("x is ",x)
    producer.send('jobs', bytes(str(x), 'utf-8'))

producer.close()
