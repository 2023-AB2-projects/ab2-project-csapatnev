from pymongo import MongoClient
import commands as cmd
import xml
from xml import ElementTree

def find_table_structure(xml_root ,db_name, table_name):
    # find the table we want
    table_to_find = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']"
                                    .format(db_name, table_name))

    return table_to_find
    
def handle_my_mongo(current_db_name):
    current_db_name = current_db_name.upper()
    
    # Fetch the xml file
    xml_root = cmd.parse_xml_file(cmd.XML_FILE_LOCATION)

    if not cmd.database_exists(xml_root, current_db_name):
        return (-1, f"Error: Database {current_db_name} does not exist!")
    else:
        # connect to mongo
        client = MongoClient("mongodb://localhost:6970/")
        db = client[current_db_name]
        return db, xml_root


def validate_data_types(table_structure, columns, values):
    # Get the list of attributes from the table structure
    attributes = table_structure.findall('Structure/Attribute')

    # Create a dictionary of column name to attribute type
    column_types = {attr.get('attributeName'): attr.get('type') for attr in attributes}

    # Iterate through the provided columns and values
    for column, value in zip(columns, values):
        expected_type = column_types[column]

        # Validate the value based on the expected data type
        if expected_type == 'VARCHAR':
            if not isinstance(value, str):
                return False
        elif expected_type == 'INT':
            if not (isinstance(value, int) or value.isdigit()):
                return False
        elif expected_type == 'FLOAT':
            try:
                float(value)
            except ValueError:
                return False
        elif expected_type == 'BIT':
            if not (isinstance(value, bool) or value == '0' or value == '1'):
                return False
        elif expected_type == 'DATE':
            if not (isinstance(value, str) and value.count('-') == 2):
                return False
        elif expected_type == 'DATETIME':
            if not (isinstance(value, str) and value.count('-') == 2 and value.count(':') == 2):
                return False
        else:
            # For any other data types, return False
            return False

    # If all values pass validation, return True
    return True


def insert_into(db_name, table_name, columns, values):
    db, xml_root = handle_my_mongo(db_name)
    table_name = table_name.upper()
    table_structure = find_table_structure(xml_root, db_name, table_name)


    # Validate the data types
    if not validate_data_types(table_structure, columns, values):
        return (-1, "Error: Invalid data type!")

    pk_index = 0    # every table has a primary key that is the first column

    key = int(values[pk_index])
    value_list = [values[i] for i in range(len(columns)) if i != pk_index]
    value = '#'.join(value_list)
    document = {"Key": key, "Value": value}

    # insert into mongo
    collection = db[table_name]
    collection.insert_one(document)
