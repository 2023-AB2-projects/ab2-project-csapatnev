import xml.etree.ElementTree as ET

def get_column_names_from_xml(database_name, table_name):
    # Parse the XML file
    tree = ET.parse('./databases.xml')
    root = tree.getroot()

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
    return -1, "Error: Database or Table not found! [xml column getter]"


def load_table_data(database_name, table_name, mongo_client):
    db = mongo_client[database_name]
    collection = db[table_name]

    data = {}

    columns, pk_columns = get_column_names_from_xml(database_name, table_name)
    pk_column_indexes = [columns.index(pk_col) for pk_col in pk_columns]

    for row in collection.find():
        keys = row['_id'].split('#')
        values = row['value'].split('#')

        # Create dictionaries for primary keys and non-primary keys
        pk_dict = {columns[i]: keys[i] for i in pk_column_indexes}
        value_dict = {column: value for column, value in zip(columns, values) if column not in pk_columns}

        # Add to the main dictionary
        data[row['_id']] = {'pk': pk_dict, 'value': value_dict}

    return data


        

