
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
  # query = "follow"
  body = {
    "query": {
      "query_string": {
        "query": query
      }
    }, 
  }
  
  results = client.search(index='tweets', body=body)

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
      "total": results['hits']['total']['value'],
      "tweets": tweets,
  })
  

@app.route('/insights', methods=["GET"])
def get_insights():
  '''
  ES aggregations based on search query
  '''
  query = request.args.get('query', '')
  # TODO: Time data histogram
  body = {
    "query": {
      "query_string": {
        "query": query
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
  
  results = client.search(index='tweets', body=body)

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
    "top_users": results['aggregations']['top_users']['buckets'],
    "top_hashtags": results['aggregations']['top_hashtags']['buckets'],
    "top_urls": results['aggregations']['top_urls']['buckets']
  })