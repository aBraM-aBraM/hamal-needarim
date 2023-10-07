import datetime
import json
from typing import Optional

from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

with open("credentials.json") as f:
    values = json.load(f)

username = values["username"]
password = values["password"]

uri = f"mongodb+srv://{username}:{password}@hamalneedarim.tuv6lf6.mongodb.net/?retryWrites=true&w=majority"


def add_user(collection: Collection, fullname: str, last_seen_place: str, age: Optional[int] = None,
             phone_number: Optional[str] = None,
             current_status: Optional[str] = "Unknown", last_updated: datetime.datetime = datetime.datetime.now()):
    result = collection.insert_one(
        {
            "fullname": fullname,
            "age": age,
            "phone_number": phone_number,
            "last_seen_place": last_seen_place,
            "current_status": current_status,
            "last_updated": last_updated
        }
    )
    if result.acknowledged:
        print("Data inserted successfully.")
        # Print the last time the entry was updated
        last_updated = collection.find_one({"_id": result.inserted_id})["last_updated"]
        print(f"Last updated at: {last_updated}")
    else:
        print("Failed to insert data.")


def main():
    client = MongoClient(uri, server_api=ServerApi('1'))

    db = client["hamal-needarim"]
    collection = db["needarim"]
    print(collection.find_one({"fullname": "John Doe"}))
    client.close()


if __name__ == '__main__':
    main()
