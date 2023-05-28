import xml.etree.ElementTree as ET

def get_column_names_from_xml(database_name, table_name, xml_root):
    # Parse the XML file
    root = xml_root

    # Iterate over all Databases in the XML
    for db in root.findall('Database'):
        if db.attrib['name'] == database_name:
            # Found the correct Database, now look for the Table
            for table in db.findall('.//Table'):
                if table.attrib['name'] == table_name:
                    # Found the correct Table, now get the column names
                    columns = [attr.attrib['attributeName'] for attr in table.findall('.//Attribute')]
                    
                    # Find the primary key columns
                    primary_keys = [pk.text for pk in table.findall('.//primaryKey/pkAttribute')]
                    
                    return columns, primary_keys
    
    # If we get here, we didn't find the Database or Table
    return -1, None


def load_table_data(database_name, table_name, mongo_client, xml_root):
    db = mongo_client[database_name]
    collection = db[table_name]

    data = {}

    columns, pk_columns = get_column_names_from_xml(database_name, table_name, xml_root)
    if columns == -1:
        return -1, "Error: Database or Table not found! [xml column getter]"
    
    pk_column_indexes = [columns.index(pk_col) for pk_col in pk_columns]

    for row in collection.find():
        keys = row['_id'].split('#')
        values = row['value'].split('#')

        # Create dictionaries for primary keys and non-primary keys
        pk_dict = {columns[i]: keys[i] for i in pk_column_indexes}

        columns_ = [col for col in columns if col not in pk_columns]
        value_dict = {columns_[i]: values[i] for i in range(len(columns_))}

        # Add to the main dictionary
        data[row['_id']] = {'pk': pk_dict, 'value': value_dict}

    return 0, data


def load_table_data_from_index(db_name, table_name, index_data, mongo_client, xml_root):
    db = mongo_client[db_name]
    collection = db[table_name]

    data = {}

    columns, pk_columns = get_column_names_from_xml(db_name, table_name, xml_root)
    if columns == -1:
        return -1, "Error: Database or Table not found! [xml column getter]"
    
    pk_column_indexes = [columns.index(pk_col) for pk_col in pk_columns]

    # Load only the rows whose primary key is in the index data
    for row in collection.find({'_id': {'$in': list(index_data.values())}}):
        keys = row['_id'].split('#')
        values = row['value'].split('#')

        # Create dictionaries for primary keys and non-primary keys
        pk_dict = {columns[i]: keys[i] for i in pk_column_indexes}

        columns_ = [col for col in columns if col not in pk_columns]
        value_dict = {columns_[i]: values[i] for i in range(len(columns_))}

        # Add to the main dictionary
        data[row['_id']] = {'pk': pk_dict, 'value': value_dict}

    return 0, data
