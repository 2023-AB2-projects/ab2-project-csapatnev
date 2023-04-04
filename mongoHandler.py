from pymongo import MongoClient
#import commands as cmd
from lxml import etree

def connect_mongo(db_name):
    if db_name == "MASTER":
        client = MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        return db, client
    else:
        client = MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        return db, client

# When these functions are called, parameters have already been checked for correctness
def drop_database(db_name, mongoclient):
    mongoclient.drop_database(db_name)

def drop_collection(db_name, table_name, mongoclient):
    db = mongoclient[db_name]
    collection = db[table_name]
    collection.drop()

def insert_into(mongoclient, db_name, table_name, primary_key_column, columns, values):
    db = mongoclient[db_name]
    collection = db[table_name]
    primary_key = values[columns.index(primary_key_column)]
    
    # Create new lists without the primary_key_column
    non_primary_columns = [col for col in columns if col != primary_key_column]
    non_primary_values = [val for col, val in zip(columns, values) if col != primary_key_column]

    # Create a dictionary of column name to value
    data = {"_id": primary_key, "value": "#".join(str(value) for value in non_primary_values)}
    
    # Insert the data into the collection
    result = collection.insert_one(data)
    
    # Check if the operation was successful
    if result.inserted_id:
        return 0, f"Document inserted with ID: {result.inserted_id}"
    else:
        return -1, "Error inserting document"



# ###
# SQL: DELETE FROM students WHERE StudID > 10000;
# # Example usage:
# db_name = "my_database"
# table_name = "students"
# filter_conditions = {"StudID": {"$gt": 10000}}

# mongodb, client = connect_mongo(db_name)
# success, message = delete(mongodb, table_name, filter_conditions)
# print(message)
###

def delete_from(mongoclient, table_name, db_name, filter_conditions, column_index):
    db = mongoclient[db_name]
    collection = db[table_name]

    # Create a regex pattern to match the value after (column_index - 1) '#' characters
    for key, value in filter_conditions.items():
        regex = f"^(?:[^#]*#){{{column_index - 2}}}[^#]*{value}[^#]*"

    updated_filter_conditions = {"value": {"$regex": regex}}

    result = collection.delete_many(updated_filter_conditions)
    if result.deleted_count > 0:
        return 0, f"Deleted {result.deleted_count} document(s) matching the filter conditions."
    else:
        return -1, "No document found matching the filter conditions."


# testing function
def fetch_all_documents(mongodb, table_name):
    collection = mongodb[table_name]
    documents = collection.find()
    return list(documents)

def select_all(mongodb, table_name):
    collection = mongodb[table_name]
    documents = collection.find()
    return list(documents)