
# A very simple Flask Hello World app for you to get started with...
from elasticsearch import Elasticsearch, helpers
from flask import Flask, request, jsonify
from mock_data import mock_tweets
from flask_cors import CORS
import os
from dotenv import load_dotenv
from query_builder import ESQueryBuilder

load_dotenv()

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})


# Create the Elasticsearch client with HTTPS and authentication
client = Elasticsearch([f'https://{os.getenv("ES_HOST")}:{os.getenv("ES_PORT")}'], 
                   basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASSWORD")),
                   verify_certs=False)

index_name = 'ioa-tweets'

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
  from_index = (page - 1) * size # Calculate 'from' for pagination

  from_date = request.args.get('from')
  to_date = request.args.get('to')
  tweet_language = request.args.get('language')
  hashtags = request.args.getlist('hashtags')
  user = request.args.get('user')
  sort_param = request.args.get('sort_by')

  query_body = ESQueryBuilder()

  query_body = (
    query_body
    .paginate(from_index, size)
    .add_query(query)
    .filter_user(user)
    .filter_language(tweet_language)
    .filter_date(to_date, from_date)
    .filter_hashtags(hashtags)
    .sort_by(sort_param)
  )


  results = client.search(index=index_name, body=query_body.get_query())

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
  interval = request.args.get('interval')

  query_body = ESQueryBuilder()

  query_body = (
    query_body
    .add_query(query)
    .filter_user(user)
    .filter_language(tweet_language)
    .filter_date(to_date, from_date)
    .filter_hashtags(hashtags)
    .agg_users()
    .agg_hashtags()
    .agg_urls()
    .agg_histogram(interval=interval)
  )
  
  results = client.search(index=index_name, body=query_body.get_query())

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
    "tweets_over_time": results['aggregations']['tweets_over_time']['buckets'],
    "top_users": results['aggregations']['top_users']['buckets'],
    "top_hashtags": results['aggregations']['top_hashtags']['buckets'],
    "top_urls": results['aggregations']['top_urls']['buckets']
  })