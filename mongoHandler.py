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

def update_index_on_insert(mongoclient, db_name, table_name, index_name, columns, column_indices, primary_key, non_primary_values):
    db = mongoclient[db_name]

    # Get the index collection
    index_collection_name = f"{table_name}_{'_'.join(columns)}_{index_name}_INDEX"
    index_collection = db[index_collection_name]

    # Get the value string and split it into a list of values
    value_list = non_primary_values

    # Get the indexed column values from the list of values using the provided column_indices
    indexed_column_values = [value_list[i-1] if i != -1 else primary_key for i in column_indices]

    # Try to cast values to integers if possible
    indexed_column_values = [int(value) if str(value).isdigit() else value for value in indexed_column_values]

    # Convert the tuple to a string by joining with a separator
    indexed_column_values_str = '#'.join(str(value) for value in indexed_column_values)

    # Check if the indexed column values already exist in the index collection
    existing_document = index_collection.find_one({"_id": indexed_column_values_str})

    if existing_document:
        # Append the primary key to the existing document
        existing_document["value"] = existing_document["value"] + "#" + primary_key
        index_collection.replace_one({"_id": indexed_column_values_str}, existing_document)
    else:
        # Insert a new document with the indexed column values and primary key
        index_data = {"_id": indexed_column_values_str, "value": primary_key}
        index_collection.insert_one(index_data)



def insert_into(mongoclient, db_name, table_name, primary_key_column, foreign_keys, unique_keys, columns, values, index_configs, foreign_key_references):
    db = mongoclient[db_name]
    collection = db[table_name]
    primary_key = values[columns.index(primary_key_column)]

    # Create new lists without the primary_key_column
    non_primary_columns = [col for col in columns if col != primary_key_column]
    non_primary_values = [val for col, val in zip(columns, values) if col != primary_key_column]

    # Check if primary key data already exists
    if collection.count_documents({"_id": primary_key}) > 0:
        return -1, f"Error inserting document: Primary key constraint violation for {primary_key_column} with value {primary_key}"

    # Validate unique keys
    for uk in unique_keys:
        uk_index = columns.index(uk)
        uk_value = values[uk_index]

        uk_collection = db[f"{table_name}_{uk}_UNIQUE_INDEX"]
        if uk_collection.count_documents({"_id": uk_value}) > 0:
            return -1, f"Error inserting document: Unique key constraint violation for {uk} with value {uk_value}"

    # Validate foreign keys
    for fk in foreign_keys:
        fk_index = columns.index(fk)
        fk_value = values[fk_index]

        referenced_collection_name = foreign_key_references[fk]['collection']
        referenced_key = foreign_key_references[fk]['key']

        referenced_collection = db[referenced_collection_name]

        # Check for primary key reference
        if referenced_collection.count_documents({"_id": fk_value}) > 0:
            continue

        # Check for unique key reference
        unique_key_collection = db[f"{referenced_collection_name}_{referenced_key}_UNIQUE_INDEX"]
        if unique_key_collection.count_documents({"_id": fk_value}) > 0:
            continue

        return -1, f"Error inserting document: Foreign key constraint violation for {fk} with value {fk_value} - Referenced value not found in {referenced_collection_name}"

    # Create a dictionary of column name to value
    data = {"_id": primary_key, "value": "#".join(str(value) for value in non_primary_values)}

    # Insert the data into the collection
    result = collection.insert_one(data)

    # Update unique keys index collections
    for uk in unique_keys:
        uk_index = columns.index(uk)
        uk_value = values[uk_index]

        uk_data = {"_id": uk_value, "value": primary_key}
        uk_collection = db[f"{table_name}_{uk}_UNIQUE_INDEX"]
        uk_collection.insert_one(uk_data)

    # Update foreign keys index collections
    for fk in foreign_keys:
        fk_index = columns.index(fk)
        fk_value = values[fk_index]

        fk_data = {"_id": fk_value, "value": primary_key}
        fk_collection = db[f"{table_name}_{fk}_FOREIGN_INDEX"]
        fk_collection.insert_one(fk_data)

    # Handle individual index files
    if index_configs:
        for index_config in index_configs:
            index_name = index_config['index_name']
            index_columns = index_config['index_columns']
            index_column_indices = index_config['index_column_indices']
            update_index_on_insert(mongoclient, db_name, table_name, index_name, index_columns, index_column_indices, primary_key, non_primary_values)

    # Check if the operation was successful
    if result.inserted_id:
        return 0, f"Document inserted with ID: {result.inserted_id}"
    else:
        return -1, "Error inserting document"


def create_index(mongoclient, db_name, table_name, index_name, columns, column_indices):
    db = mongoclient[db_name]

    # Create the index file collection
    index_collection_name = f"{table_name}_{'_'.join(columns)}_{index_name}_INDEX"
    index_collection = db[index_collection_name]

    # Find the collection for the table
    table_collection = db[table_name]

    if table_collection.estimated_document_count() == 0:
        # If there's no data in the main collection, create an empty index
        return 0, f"Created an empty compound index {index_name} on columns {', '.join(columns)} for table {table_name}."

    try:
        for document in table_collection.find():
            # Get the primary key
            primary_key = document["_id"]

            # Get the value string and split it into a list of values
            value_list = document["value"].split("#")

            # Get the indexed column values from the list of values using the provided column_indices
            indexed_column_values = tuple(value_list[i-1] if i != -1 else primary_key for i in column_indices)

            # Convert the tuple to a string by joining with a separator
            indexed_column_values_str = '#'.join(indexed_column_values)

            # Check if the indexed column values already exist in the index collection
            existing_document = index_collection.find_one({"_id": indexed_column_values_str})

            if existing_document:
                # Append the primary key to the existing document
                existing_document["value"] = existing_document["value"] + "#" + primary_key
                index_collection.replace_one({"_id": indexed_column_values_str}, existing_document)
            else:
                # Insert a new document with the indexed column values and primary key
                index_data = {"_id": indexed_column_values_str, "value": primary_key}
                index_collection.insert_one(index_data)

        return 0, f"Compound index {index_name} on columns {', '.join(columns)} for table {table_name} created successfully."
    
    except Exception as e:
        # Step 6: Return an error message
        return -1, f"Error creating compound index {index_name} on columns {', '.join(columns)} for table {table_name}. Error: {e}"

def delete_from(mongoclient, table_name, db_name, filter_conditions, columns, unique_keys, foreign_keys, foreign_key_references, index_collections):
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

    # Validate foreign key constraints
    for doc in docs_to_delete:
        non_primary_values = doc["value"].split("#")
        for fk, fk_info in foreign_key_references.items():
            fk_index = columns.index(fk) - 1
            fk_value = non_primary_values[fk_index]

            if fk_value.isdigit():
                fk_value = int(fk_value)

            referenced_collection_name = fk_info['collection']
            referenced_key = fk_info['key']
            referenced_collection = db[referenced_collection_name]

            if referenced_collection.count_documents({referenced_key: fk_value}) > 0:
                return -1, f"Error deleting document: Foreign key constraint violation for {fk} with value {fk_value} - Referenced value exists in {referenced_collection_name}"

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
        
        # Handle individual index files (created with "create index")
        for index_collection_name in index_collections:
            index_collection = db[index_collection_name]
            index_doc = index_collection.find_one({"value": {"$regex": f"(^|.*#){doc['_id']}($|#.*)"}})

            if index_doc:
                # Update the index entry in the index collection
                updated_value_list = [v for v in index_doc["value"].split("#") if v != str(doc["_id"])]
                if updated_value_list:
                    index_collection.update_one({"_id": index_doc["_id"]}, {"$set": {"value": "#".join(updated_value_list)}})
                else:
                    index_collection.delete_one({"_id": index_doc["_id"]})

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