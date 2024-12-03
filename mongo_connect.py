from pymongo import MongoClient


host = 'localhost'
port = 27017

client = MongoClient(host=host, port=port)

db = client.nhldb


