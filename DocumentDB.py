from pymongo import MongoClient
import pymongo
import json
import time
import threading
import random

DEFAULT_MONGO_CONNECTION = "192.168.230.130"
MONGO_DBNAME = "testDatabase"
MONGO_COLLECTION_NAME = "testCollection"
COLLECTION_SIZE = 1000000
COLLECTION_FILE_NAME = "testData.json"
NUM_OF_QUERIES = 50000

FIRST_NAME_LIST = [
    "James",
    "John",
    "Robert",
    "Michael",
    "William",
    "David",
    "Richard",
    "Charles",
    "Joseph",
    "Thomas",
    "Olivia",
    "Emma",
    "Charlotte",
    "Amelia",
    "Sophia",
    "Isabella",
    "Ava",
    "Mia",
    "Evelyn",
    "Luna",
    "Santiago",
    "Mateo",
    "Sebastián",
    "Leonardo",
    "Diego",
]


def main():
    with open(COLLECTION_FILE_NAME, "r") as f:
        docs = json.load(f)
        print("Connecting to MongoDB: '{DEFAULT_MONGO_CONNECTION}'")
        client = MongoClient(DEFAULT_MONGO_CONNECTION)

        if MONGO_DBNAME in client.list_database_names():
            client.drop_database(MONGO_DBNAME)
            print(f"Database '{MONGO_DBNAME}' dropped.")
        else:
            print(f"Database '{MONGO_DBNAME}' Not found. Creating new")

        # Create the database and collection
        db = client[MONGO_DBNAME]
        collection = db[MONGO_COLLECTION_NAME]
        collection.create_index([("children.firstName", pymongo.ASCENDING)])

        start_time = time.time()
        for i in range(0, COLLECTION_SIZE):
            collection.insert_one(docs[i])
        print(
            f"Database '{MONGO_DBNAME}' and Collection '{MONGO_COLLECTION_NAME}' created."
        )
        end_time = time.time()
        # Close MongoDB connection

        total_time = end_time - start_time
        print(f"Inserted {COLLECTION_SIZE} documents in {total_time} seconds")

        mongo_query = {"children.firstName": random.choice(FIRST_NAME_LIST)}
        start_time = time.time()
        for i in range(0, NUM_OF_QUERIES):
            found = list(collection.find(mongo_query).batch_size(100))
            if i % 1000 == 0:
                end_time = time.time()
                total_time = end_time - start_time
                print(f"fetch number {i} in {total_time} seconds")

        client.close()

        print(f"Sent {NUM_OF_QUERIES} Queries in {total_time} seconds")


if __name__ == "__main__":
    main()
