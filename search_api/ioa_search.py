
# A very simple Flask Hello World app for you to get started with...
from elasticsearch import Elasticsearch, helpers
from flask import Flask, request, jsonify
from mock_data import mock_tweets
from flask_cors import CORS


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})


es_host = '127.0.0.1'
es_port = 9200
es_username = 'elastic'
es_password = ''

# Create the Elasticsearch client with HTTPS and authentication
client = Elasticsearch([f'http://{es_host}:{es_port}'], 
                   #basic_auth=(es_username, es_password), 
                   api_key="",
                   verify_certs=False)



@app.route('/')
def hello_world():
    return 'Hello from Flask!'


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
  
  results = client.search(index='tweets_test', body=body)

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
  
  results = client.search(index='tweet_id', body=body)

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
    "top_users": results['aggregations']['top_users']['buckets'],
    "top_hashtags": results['aggregations']['top_hashtags']['buckets'],
    "top_urls": results['aggregations']['top_urls']['buckets']
  })