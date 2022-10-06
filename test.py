from elasticsearch import Elasticsearch, helpers
from pprint import pprint
import configparser

config = configparser.ConfigParser()
config.read('settings.ini')

es = Elasticsearch(
    "http://localhost:9200",
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

pprint(es.info())

es.index(
 index='lord-of-the-rings',
 id=1,
 body={
  'character': 'Aragon',
  'quote': 'It is not this day.'
 })

es.index(
 index='lord-of-the-rings',
 id=2,
 body={
  'character': 'Aragon2',
  'quote': 'It is not this day2.'
 })

pprint(es.indices.refresh(index='lord-of-the-rings'))

