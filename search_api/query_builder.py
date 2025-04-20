from elasticsearch import Elasticsearch

class ESQueryBuilder:

  def __init__(self, index_name="tweets"):
    self.index_name = index_name
    self.body = {
      "query": {
        "bool": {
          "must": [],
          "filter": []
        }
      },
      "size": 0,
      "aggs": {
      }
    }

  def paginate(self, from_index, size):
    self.body["from"] = from_index
    self.body["size"] = size
    return self

  def agg_users(self, field: str = "user_screen_name", size: int = 10, agg_name: str = "top_users"):
    self.body["aggs"][agg_name] = {
      "terms": {
        "field": field,
        "size": size,
        "order": {"_count": "desc"}
      }
    }
    return self
  
  def agg_urls(self, field: str = "urls.keyword", size: int = 10, agg_name: str = "top_urls"):
    self.body["aggs"][agg_name] = {
      "terms": {
        "field": field,
        "size": size,
        "order": {"_count": "desc"}
      }
    }
    return self
  
  def agg_hashtags(self, field: str = "hashtags", size: int = 10, agg_name: str = "top_hashtags"):
    self.body["aggs"][agg_name] = {
      "terms": {
        "field": field,
        "size": size,
        "order": {"_count": "desc"}
      }
    }
    return self
  
  def agg_histogram(self, field: str = "tweet_time", interval = "year", agg_name: str = "tweets_over_time"):
    self.body["aggs"][agg_name] = {
      "date_histogram": {
        "field": field,
        "calendar_interval": interval
      }
    }
    return self

  def add_query(self, query):
    if query:
      self.body['query']['bool']['must'].append({
              "multi_match": {
                  "query": query,
                  "fields": ["text", "user", "hashtags"],
                  "fuzziness": "AUTO"
              }
          })
    return self

  def filter_user(self, userid):
    if userid:
      self.body['query']['bool']['filter'].append({
        "term": {"userid.keyword": userid}
      })
    return self

  def filter_language(self, language):
    if language:
      self.body["query"]["bool"]["filter"].append({
        "term": {"tweet_language.keyword": language}
      })
    return self

  def filter_date(self, to_date, from_date):
    if to_date or from_date:
      date_range = {}
      if from_date:
        date_range["gte"] = from_date
      if to_date:
        date_range["lte"] = to_date
      
      self.body['query']['bool']['filter'].append({
        "range": {
          "tweet_time": date_range
        }
      })
    return self

  def filter_hashtags(self, hashtags):
    for hashtag in hashtags:
      self.body["query"]["bool"]["filter"].append({
        "term": {"hashtags": hashtag}
      })
    return self

  def sort_by(self, sort_param):
    if sort_param:
      print(sort_param)
      sort_mapping = {
        'accuracy': '_score',
        'time': 'tweet_time',
        'retweets': 'retweet_count',
        'likes': 'like_count'
      }

      self.body['sort'] = [{str(sort_mapping[sort_param]): {'order': 'desc'}}]
      print(self.body['sort'])
    return self
  
  def get_query(self):
    return self.body

  
  