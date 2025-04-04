
# A very simple Flask Hello World app for you to get started with...
from elasticsearch import Elasticsearch, helpers
from flask import Flask, request, jsonify
from mock_data import mock_tweets
from flask_cors import CORS
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})


# Create the Elasticsearch client with HTTPS and authentication
client = Elasticsearch([f'https://{os.getenv("ES_HOST")}:{os.getenv("ES_PORT")}'], 
                   basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD")),
                   verify_certs=False)



@app.route('/')
def hello_world():
    return 'Welcome to IOA!'


# Mock API for returning some tweets
@app.route('/mock_search', methods=["GET"])
def mock_search():
  results = mock_tweets

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
    "total": results['hits']['total']['value'],
    "tweets": tweets,
  })

# Mock API for insights/stats
@app.route('/mock_insights', methods=["GET"])
def mock_insights():
  results = mock_tweets

  return jsonify({
      "top_users": results['aggregations']['top_users']['buckets'],
      "top_hashtags": results['aggregations']['top_hashtags']['buckets'],
      "top_urls": results['aggregations']['top_urls']['buckets']
  })





@app.route('/search', methods=["GET"])
def search_query():
  '''
  Basic ES query given search query
  '''
  query = request.args.get('query', '')

  page = int(request.args.get('page', 1))  # Default to page 1
  size = int(request.args.get('size', 10))  # Default page size is 10
  # Calculate 'from' for pagination
  from_index = (page - 1) * size 

  from_date = request.args.get('from')
  to_date = request.args.get('to')
  tweet_language = request.args.get('language')
  hashtags = request.args.getlist('hashtags')
  user = request.args.get('user')
  sort_param = request.args.get('sort_by')


  body = {
    "query": {
      "bool": {
        "must": [],
        "filter": []
      }
    },
    "from": from_index,  # Start position
    "size": size  # Number of results per page
  }
  
  if query:
        body['query']['bool']['must'].append({
            "multi_match": {
                "query": query,
                "fields": ["text", "user", "hashtags"],
                "fuzziness": "AUTO"
            }
        })
  
  # match userid
  if user:
    body['query']['bool']['filter'].append({
      "term": {"userid.keyword": user}
    })

  # filter language
  if tweet_language:
    body["query"]["bool"]["filter"].append({
       "term": {"tweet_language.keyword": tweet_language}
    })

  # add date range if requested
  if from_date or to_date:
    date_range = {}
    if from_date:
      date_range["gte"] = from_date
    if to_date:
      date_range["lte"] = to_date
    
    body['query']['bool']['filter'].append({
      "range": {
        "tweet_time": date_range
      }
    })

  # filter hashtags using AND
  for hashtag in hashtags:
    body["query"]["bool"]["filter"].append({
       "term": {"hashtags": hashtag}
    })

  # sorting mapping
  if sort_param:
    print(sort_param)
    sort_mapping = {
      'accuracy': '_score',
      'time': 'tweet_time',
      'retweets': 'retweet_count',
      'likes': 'like_count'
    }

    body['sort'] = [{str(sort_mapping[sort_param]): {'order': 'desc'}}]
    print(body['sort'])



  results = client.search(index='tweets', body=body)

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
      "total": results['hits']['total']['value'],
      "page": page,
      "size": size,
      "tweets": tweets,
  })
  

@app.route('/insights', methods=["GET"])
def get_insights():
  '''
  ES aggregations based on search query
  '''
  query = request.args.get('query', '')

  from_date = request.args.get('from')
  to_date = request.args.get('to')
  tweet_language = request.args.get('language')
  hashtags = request.args.getlist('hashtags')
  user = request.args.get('user')


  body = {
    "query": {
      "bool": {
        "must": [],
        "filter": []
      }
    },
    "size": 0,
    "aggs": {
        "top_users": {
            "terms": {
                "field": "user_screen_name"
            }
        },
        "top_hashtags": {
            "terms": {
                "field": "hashtags"
            }
        },
        "top_urls": {
            "terms": {
                "field": "urls"
            }
        }
    }
  }
  
  if query:
        body['query']['bool']['must'].append({
            "multi_match": {
                "query": query,
                "fields": ["text", "user", "hashtags"],
                "fuzziness": "AUTO"
            }
        })
  
  if user:
    body['query']['bool']['filter'].append({
      "term": {"userid.keyword": user}
    })

  # filter language
  if tweet_language:
    body["query"]["bool"]["filter"].append({
       "term": {"tweet_language.keyword": tweet_language}
    })

  # add date range if requested
  if from_date or to_date:
    date_range = {}
    if from_date:
      date_range["gte"] = from_date
    if to_date:
      date_range["lte"] = to_date
    
    body['query']['bool']['filter'].append({
      "range": {
        "tweet_time": date_range
      }
    })

  # filter hashtags using AND
  for hashtag in hashtags:
    body["query"]["bool"]["filter"].append({
       "term": {"hashtags": hashtag}
    })
  
  
  results = client.search(index='tweets', body=body)

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
    "top_users": results['aggregations']['top_users']['buckets'],
    "top_hashtags": results['aggregations']['top_hashtags']['buckets'],
    "top_urls": results['aggregations']['top_urls']['buckets']
  })