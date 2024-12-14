from typing import Any, List

from dotenv import dotenv_values
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection, InsertOneResult


class MongoConnect:

    def __init__(self, config: dict) -> None:
        """
        Constructor

        Args:
            config (dict): _description_
        """
        self.config = config
        self._client = MongoClient(host=self.config['mongodbhost'], port=int(self.config['mongodbport']))

    def list_data_bases(self) -> List[str]:
        """
        Return the databases in the mongodb instance

        Returns:
            List[str]: A list of the instance databases
        """
        return self._client.list_database_names()
    
    def list_collections(self, db: Database) -> List[str]:
        return db.list_collection_names()
    
    def database(self, db: str) -> Database:
        return self._client[db]
    
    def insert_document(self, collection: Collection, document: Any) -> InsertOneResult:
        return collection.insert_one(document).inserted_id


client = MongoClient(host=self.config['mongodbhost'], port=int(self.config['mongodbport']))

db = client.get_database()

collection: Collection = db.get_collection()

collection.insert_one().inserted_id

config = dotenv_values()

client = MongoConnect(config=config)

print(client.list_collections('admin'))
