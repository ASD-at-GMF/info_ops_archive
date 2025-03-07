
# A very simple Flask Hello World app for you to get started with...
from elasticsearch import Elasticsearch, helpers
from flask import Flask, request, jsonify
from mock_data import mock_tweets
from flask_cors import CORS


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})
client = Elasticsearch(
  "http://localhost:9200",
  api_key="TFY2ZzNwUUJ5cDNTSi1kZVRZVkk6VVluaFpoTjRSRXUyTnpOUm4ycUMzdw=="
)



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
    "aggs" : {
       "top_users" : {
          "terms": {
             "field": "user_screen_name"
          }
       }
    }
  }
  
  results = client.search(index='tweet_id', body=body)

  tweets = [hit['_source'] for hit in results['hits']['hits']]
  return jsonify({
      "total": results['hits']['total']['value'],
      "tweets": tweets,
  })
  

  @app.route('/insights', methods=["GET"])
  def get_insights():
     None