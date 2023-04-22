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

def insert_into(mongoclient, db_name, table_name, primary_key_column, foreign_keys, unique_keys, columns, values):
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

    # Handle unique keys
    for uk in unique_keys:
        uk_index = columns.index(uk)
        uk_value = values[uk_index]

        uk_collection = db[f"{table_name}_{uk}_UNIQUE_INDEX"]
        if uk_collection.count_documents({"_id": uk_value}) > 0:
            return -1, f"Error inserting document: Unique key constraint violation for {uk} with value {uk_value}"

        uk_data = {"_id": uk_value, "value": primary_key}
        uk_collection.insert_one(uk_data)

    # Handle foreign keys
    for fk in foreign_keys:
        fk_index = columns.index(fk)
        fk_value = values[fk_index]

        fk_collection = db[f"{table_name}_{fk}_FOREIGN_INDEX"]
        existing_document = fk_collection.find_one({"_id": fk_value})

        if existing_document:
            existing_document["value"] = existing_document["value"] + "#" + primary_key
            fk_collection.replace_one({"_id": fk_value}, existing_document)
        else:
            fk_data = {"_id": fk_value, "value": primary_key}
            fk_collection.insert_one(fk_data)

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

def delete_from(mongoclient, table_name, db_name, filter_conditions, columns, unique_keys, foreign_keys):
    db = mongoclient[db_name]
    collection = db[table_name]

    # Extract the primary_key_column and its conditions from filter_conditions
    primary_key_column = list(filter_conditions.keys())[0]
    primary_key_conditions = filter_conditions[primary_key_column]

    # Create a new filter_conditions with the "_id" field
    new_filter_conditions = {"_id": {}}

    for condition, value in primary_key_conditions.items():
        # Convert the primary_key_value to the appropriate data type
        if isinstance(value, str) and value.isdigit():
            value = int(value)
        new_filter_conditions["_id"][condition] = value

    # Fetch the documents to be deleted
    docs_to_delete = list(collection.find(new_filter_conditions))

    # Iterate through the documents to be deleted and update unique and foreign key index collections
    for doc in docs_to_delete:
        non_primary_values = doc["value"].split("#")

        # Handle unique keys
        for unique_key in unique_keys:
            unique_key_col_index = columns.index(unique_key) - 1
            unique_key_value = non_primary_values[unique_key_col_index]

            index_collection = db[f"{table_name}_{unique_key}_UNIQUE_INDEX"]
            index_collection.delete_one({"_id": unique_key_value})

        # Handle foreign keys
        for fk in foreign_keys:
            fk_index = columns.index(fk) - 1

            # Extract the foreign key value from the document and try to convert it to the appropriate data type
            fk_value = non_primary_values[fk_index]
            if fk_value.isdigit():
                fk_value = int(fk_value)

            index_collection = db[f"{table_name}_{fk}_FOREIGN_INDEX"]
            index_doc = index_collection.find_one({"_id": fk_value})
            if index_doc:
                updated_value_list = [v for v in index_doc["value"].split(",") if v != str(doc["_id"])]
                if updated_value_list:
                    index_collection.update_one({"_id": fk_value}, {"$set": {"value": ",".join(updated_value_list)}})
                else:
                    index_collection.delete_one({"_id": fk_value})

    # Delete the documents from the main collection
    result = collection.delete_many(new_filter_conditions)
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