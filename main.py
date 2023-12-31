import datetime
import json
from typing import Optional, List

from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import csv

with open("credentials.json") as f:
    values = json.load(f)

username = values["username"]
password = values["password"]

uri = f"mongodb+srv://{username}:{password}@hamalneedarim.tuv6lf6.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri, server_api=ServerApi('1'))

db = client["hamal-needarim"]
collection = db["needarim"]


def add_user(fullname: str,
             last_seen_place: str,
             age: Optional[int] = None,
             phone_number: Optional[str] = None,
             current_status: Optional[str] = "Unknown",
             last_updated: datetime.datetime = datetime.datetime.now(),
             notes: Optional[str] = None):
    result = collection.insert_one(
        {
            "fullname": fullname,
            "age": age,
            "phone_number": phone_number,
            "last_seen_place": last_seen_place,
            "current_status": current_status,
            "last_updated": last_updated,
            "notes": notes
        }
    )
    if result.acknowledged:
        print("Data inserted successfully.")
        # Print the last time the entry was updated
        last_updated = collection.find_one({"_id": result.inserted_id})["last_updated"]
        print(f"Last updated at: {last_updated}")
    else:
        print("Failed to insert data.")


def parse_csv1(filename: str):
    # parses this csv
    # https://docs.google.com/spreadsheets/d/19-g2ybI4xFSAoRlyZffJGLVfHNKgE04rm9GyTWr97fU/edit#gid=0
    data = []
    with open("csv1.json", encoding="utf-8") as csv1_key_mapping:
        key_mapping = json.load(csv1_key_mapping)

    with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            mapped_row = {}
            for key, value in row.items():
                if key in key_mapping:
                    mapped_row[key_mapping[key]] = value

            if mapped_row and mapped_row["fullname"]:
                data.append(mapped_row)
    return data


def safe_upload_entries(new_entries: List[dict]):
    for entry in new_entries:
        # Check if there's an existing entry with the same full_name and phone_number
        existing_entry = collection.find_one({
            "fullname": entry["fullname"],
            "phone_number": entry["phone_number"]
        })

        if existing_entry:
            # Update existing entry
            for key, value in entry.items():
                # Skip empty keys
                if key == "notes" and existing_entry.get(key):
                    # Append notes with a newline if notes exist in the existing entry
                    existing_entry[key] += "\n" + value
                elif key in ["current_status", "last_updated", "last_seen_place"]:
                    existing_entry[key] = value

            # Save the updated entry
            collection.save(existing_entry)
        else:
            collection.insert_one(entry)


def main():
    for document in collection.find({}):
        print(document)

    client.close()


if __name__ == '__main__':
    main()
