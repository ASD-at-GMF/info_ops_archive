from elasticsearch import Elasticsearch

class ESQueryBuilder:
  """
    A class representing an ES query body for IOA

    Attributes:
      index_name: relevant index name for query
      body: main body for the ES query  

    Each function returns the class so that it can be method-chained.
    """

  def __init__(self, index_name="ioa-tweets"):
    """
    Initialize the main query body to be built upon

    Attributes:
      index_name: relevant index name for query
    """
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
    """
    Set the size and starting index of query results, used for pagination

    Attributes:
      from_index: Starting position for the results
      size: number of results needed
    """
    self.body["from"] = from_index
    self.body["size"] = size
    return self

  def agg_users(self, field: str = "user_screen_name.keyword", size: int = 10, agg_name: str = "top_users"):
    """
    Add aggregate of the top users to the query

    Attributes:
      field: what to aggregate on, default to user_screen_name keyword
      size: number of results, default to top 10 user names
      agg_name: name for aggregation result
    """
    self.body["aggs"][agg_name] = {
      "terms": {
        "field": field,
        "size": size,
        "order": {"_count": "desc"}
      }
    }
    return self
  
  def agg_urls(self, field: str = "urls", size: int = 10, agg_name: str = "top_urls"):
    """
    Add aggregate of the top urls to the query

    Attributes:
      field: what to aggregate on, default to urls (keyword type)
      size: number of results, default to top 10 urls
      agg_name: name for aggregation result
    """
    self.body["aggs"][agg_name] = {
      "terms": {
        "field": field,
        "size": size,
        "order": {"_count": "desc"}
      }
    }
    return self
  
  def agg_hashtags(self, field: str = "hashtags", size: int = 10, agg_name: str = "top_hashtags"):
    """
    Add aggregate of the top hashtags to the query

    Attributes:
      field: what to aggregate on, default to hashtags keyword
      size: number of results, default to top 10 hashtags
      agg_name: name for aggregation results
    """
    self.body["aggs"][agg_name] = {
      "terms": {
        "field": field,
        "size": size,
        "order": {"_count": "desc"}
      }
    }
    return self
  
  def agg_histogram(self, field: str = "tweet_time", interval = "year", agg_name: str = "tweets_over_time"):
    """
    Add aggregate of the buckets for when tweets appeared to build a histogram

    Attributes:
      field: what to aggregate on, default to tweet_time date
      interval: how wide the histogram buckets should be, defaulted to year
      agg_name: name for aggregation results
    """
    if interval is None:
      interval = "year"
    self.body["aggs"][agg_name] = {
      "date_histogram": {
        "field": field,
        "calendar_interval": interval
      }
    }
    return self

  def add_query(self, query):
    """
    Add search query to the body to search for tweet text, user, or hashtag fields

    Attributes:
      query: adds search query to the body
    """
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
    """
    Filter the results by a userid only

    Attributes:
      userid: userid to filter tweets by
    """
    if userid:
      self.body['query']['bool']['filter'].append({
        "term": {"userid.keyword": userid}
      })
    return self

  def filter_language(self, language):
    """
    Filter the results by a specific language only

    Attributes:
      language: language to filter by
    """
    if language:
      self.body["query"]["bool"]["filter"].append({
        "term": {"tweet_language.keyword": language}
      })
    return self

  def filter_date(self, to_date, from_date):
    """
    Filter the results by a date, starting from a date and ending at a date

    Attributes:
      to_date: starting date
      from_date: ending date
    """
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
    """
    Filter the results by hashtags

    Attributes:
      hashtags: list of hashtags to filter by (AND, results will include all hashtags provided)
    """
    for hashtag in hashtags:
      self.body["query"]["bool"]["filter"].append({
        "term": {"hashtags": hashtag}
      })
    return self

  def sort_by(self, sort_param):
    """
    Set the sorting setting. Set by accuracy, time, retweets, and likes

    Attributes:
      sort_param: parameter for search to be sorted by
    """
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
    """
    Return the current query body, to be used to call ES
    """
    return self.body

  
  