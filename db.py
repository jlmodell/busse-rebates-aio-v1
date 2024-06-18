import os
from functools import lru_cache

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


class DB:
    def __init__(self, db_name, collection_name):
        self.client = MongoClient(os.getenv("MONGODB"))
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert(self, data):
        self.collection.insert_one(data)

    def insert_many(self, data):
        result = self.collection.insert_many(data)
        print(f"Inserted {len(result.inserted_ids)} documents")

    def find(self, query, projection=None):
        return self.collection.find(query, projection)

    def find_one(self, query):
        return self.collection.find_one(query)

    def update(self, query, data, upsert=False):
        self.collection.update_one(query, data, upsert=upsert)

    def delete(self, query):
        self.collection.delete_one(query)

    def delete_many(self, query):
        result = self.collection.delete_many(query)
        print(f"Deleted {result.deleted_count} documents")

    def aggregate(self, pipeline):
        return self.collection.aggregate(pipeline)

    def count_documents(self, query):
        return self.collection.count_documents(query)

    def close(self):
        self.client.close()


Tracings = DB("busserebatetraces", "tracings")
Data_warehouse = DB("busserebatetraces", "data_warehouse")
Roster = DB("busserebatetraces", "roster")
Sched_data = DB("busserebatetraces", "sched_data")
GPO_contracts = DB("busserebatetraces", "contracts")

Contracts = DB("bussepricing", "contract_prices")


@lru_cache(maxsize=None)
def find_part_details_by_part(part: str) -> dict:
    from db import Sched_data

    doc = Sched_data.find_one({"part": part})
    if doc:
        return doc

    return None


@lru_cache(maxsize=None)
def find_gpo_by_contract(contract: str) -> str:
    doc = GPO_contracts.find_one({"contract": contract})
    if not doc:
        truncated_contract = contract[:5] + ".*"
        doc = GPO_contracts.find_one(
            {"contract": {"$regex": truncated_contract, "$options": "i"}}
        )

    if doc:
        medassets = ["MEDASSETS", "VIZIENT", "VHA"]
        gpo = doc["gpo"].upper()
        return "MEDASSETS" if gpo in medassets else gpo

    return "Contract Not Found."


if __name__ == "__main__":

    def delete_many(table: str, q: str, month: str = None, year: str = None):
        if table == "tracings":
            Tracings.delete_many(
                {
                    "period": q,
                }
            )
        elif table == "data_warehouse":
            Data_warehouse.delete_many(
                {
                    "__file__": q,
                    "__month__": month,
                    "__year__": year,
                }
            )

    import fire

    fire.Fire(delete_many)
