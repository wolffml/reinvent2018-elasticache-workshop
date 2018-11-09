import boto3
import json
import redis


comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
redis_endpoint = 'reinventworkshop.t1e6t9.ng.0001.use1.cache.amazonaws.com'
r = redis.StrictRedis(host=redis_endpoint, port=6379)


while r.llen('toProcess') > 0:
    str_item = r.lpop('toProcess')
    json_item = json.loads(str_item)
    review_text = json_item['review_body'] if 'review_body' in json_item else 'None'
    if review_text is None:
        review_text = ""
    review_truncated = (review_text[:3075] + '..') if len(review_text) > 3075 else review_text
    print(review_text)
    if review_truncated:
        get_sentiment = comprehend.detect_sentiment(Text=review_truncated, LanguageCode='en')
        positive = get_sentiment["SentimentScore"]["Positive"]
        negative = get_sentiment["SentimentScore"]["Negative"]
        neutral = get_sentiment["SentimentScore"]["Neutral"]
        sentiment = get_sentiment["Sentiment"]
    else:
        positive = 0
        negative = 0
        neutral = 0
        sentiment = "No Review"
    
    # Set Comprehend results to review set
    json_item['sentiment_positive'] = float(positive)
    json_item['sentiment_negative'] = float(negative)
    json_item['sentiment_neutral'] = float(neutral)
    json_item['sentiment_overall'] = sentiment
    processed_data = json.dumps(json_item)
    print(sentiment)
    r.rpush('Processed', processed_data)