import json
import boto3
import redis

try:
    #Connection to redis
    r = redis.StrictRedis(host="reinventworkshop.t1e6t9.ng.0001.use1.cache.amazonaws.com", port=6379)

    # The WHILE loop will iterate through items in the 'Processed' list until it's empty.
    while r.llen('Processed') > 0:
        str_item = r.lpop('Processed')
        json_item = json.loads(str_item)
        product_id = "pid:"+json_item["product_id"]
        review_id = json_item["review_id"]
        reviews = r.hget(product_id, "review_ids")
        
        print("Processing review for product: {}.").format(product_id)
        
        # This block groups together review IDs into a pipe delimited string for future use 
        if reviews is None:
            current_reviews = review_id
        elif len(reviews.decode('utf8')) > 0:
            current_reviews = reviews.decode('utf8') + "|" + review_id

        # This block is setting each review object as a key/value for each 'product_id'
        # Notice that for our reviews we're storing the data in a colon-separated key 
        # This will make it unique against other reviews from the same product
        r.hset(product_id, "product_parent",                json_item['product_parent'])
        r.hset(product_id, "product_title",                 json_item['product_title'])
        r.hset(product_id, "review_ids",                    current_reviews)
        r.hset(product_id, review_id+":sentiment_positive", json_item['sentiment_positive'])
        r.hset(product_id, review_id+":sentiment_negative", json_item['sentiment_negative'])
        r.hset(product_id, review_id+":sentiment_neutral",  json_item['sentiment_neutral'])
        r.hset(product_id, review_id+":sentiment_overall",  json_item['sentiment_overall'])
        r.hset(product_id, review_id+":customer_id",        json_item['customer_id'])
        r.hset(product_id, review_id+":star_rating",        json_item['star_rating'])
        r.hset(product_id, review_id+":helpful_votes",      json_item['helpful_votes'])
        r.hset(product_id, review_id+":total_votes",        json_item['total_votes'])
        r.hset(product_id, review_id+":vine",               json_item['vine'])
        r.hset(product_id, review_id+":verified_purchase",  json_item['verified_purchase'])
        r.hset(product_id, review_id+":review_headline",    json_item['review_headline'])
        r.hset(product_id, review_id+":review_body",        json_item['review_body'])
        r.hset(product_id, review_id+":review_date",        json_item['review_date'])
        r.hset(product_id, review_id+":year",               json_item['year'])

except Exception as e:
    print(e)
    raise e