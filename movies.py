from elasticsearch import Elasticsearch, helpers
from pprint import pprint
import configparser
import csv
from collections import deque


config = configparser.ConfigParser()
config.read('settings.ini')

def readMovies():
    csvfile = open('data/ml-latest-small/movies.csv', 'r')
    reader = csv.DictReader(csvfile)
    titleLookup = {}
    for movie in reader:
        titleLookup[movie['movieId']] = movie['title']
    return titleLookup

def readRatings():
    csvfile = open('data/ml-latest-small/ratings.csv', 'r')
    reader = csv.DictReader(csvfile)
    titles = readMovies()
    for line in reader:
        rating = {}
        rating['user_id'] = int(line['userId'])
        rating['movie_id'] = int(line['movieId'])
        rating['title'] = titles[line['movieId']] 
        rating['rating'] = float(line['rating'])
        rating['timestamp'] = int(line['timestamp'])
        yield rating


es = Elasticsearch(
    "http://localhost:9200",
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

pprint(es.info())

es.indices.delete(index='ratings', ignore=404)

deque(helpers.parallel_bulk(es, readRatings(), index='ratings'), maxlen=0)

pprint(es.indices.refresh(index='ratings'))
