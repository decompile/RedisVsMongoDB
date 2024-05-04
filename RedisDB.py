import redis

# from redis import Redis
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TextField
from redis.commands.search.query import Query

import json
import time
import random

DEFAULT_REDIS_CONNECTION = "192.168.230.135"
DEFAULT_REDIS_PORT = "6379"
KEY_PREFIX = "person:"
IDX_NAME = "personIDX"
COLLECTION_SIZE = 1000000
COLLECTION_FILE_NAME = "testData.json"
NUM_OF_QUERIES = 50000
RESULTS_PER_QUERY = 100

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
        print(f"Connecting to Redis: '{DEFAULT_REDIS_CONNECTION}'")

        client = redis.Redis(
            host=DEFAULT_REDIS_CONNECTION,
            port=DEFAULT_REDIS_PORT,
            decode_responses=True,
        )

        keys = client.keys(f"{KEY_PREFIX}*")

        if keys:
            client.delete(*keys)
            print(f"deleted {len(keys)} from '{DEFAULT_REDIS_CONNECTION}'")
        else:
            print(f"Database '{DEFAULT_REDIS_CONNECTION}' Not found. Creating new")

        try:
            schema = TextField("$.children[0:].firstName", as_name="firstName")
            client.ft(f"{IDX_NAME}").create_index(
                schema, definition=IndexDefinition(index_type=IndexType.JSON)
            )
        except:
            print("Index already exists")

        start_time = time.time()
        for i in range(0, COLLECTION_SIZE):
            client.json().set(f"{KEY_PREFIX}{i}", "$", docs[i])

        print(f"Database created.")
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {COLLECTION_SIZE} documents in {total_time} seconds")

        redis_index = client.ft(IDX_NAME)
        start_time = time.time()
        q = Query(f"{random.choice(FIRST_NAME_LIST)}").paging(0, 100)
        for i in range(0, NUM_OF_QUERIES):
            found = redis_index.search(q)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Sent {NUM_OF_QUERIES} Queries in {total_time} seconds")


if __name__ == "__main__":
    main()
