from lxml import *
from lxml import etree
import mongoHandler

XML_FILE_LOCATION = './databases.xml'


# All server sided functions return a tuple (<error_code>, <error_message>)
# Error_code = 0 -> no error
# Error_code < 0 -> error, message returned needs to be printed on client side

def parse_xml_file(file_name):
    file_name = file_name.upper()
    with open(file_name, 'rb') as file:
        content = file.read()
    return etree.fromstring(content)

def database_exists(xml_root, database_name):
    database_name = database_name.upper()
    databases = xml_root.findall(".//Database[@name='{}']".format(database_name))
    return len(databases) > 0

def create_database(db_name):
    db_name = db_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if database_exists(xml_root, db_name):
        return (-1, "Error: Database already exists!")
    else:
        database = etree.Element('Database', name=db_name)
        database.text = "\n    "  # Set the text content of the <Database> element to an empty string with proper indentation
        database.tail = "\n\n"

        # Add a new <Tables> element under the <Database> element
        tables = etree.Element('Tables')
        tables.text = "\n        "  # Set the text content of the <Tables> element to an empty string with proper indentation
        database.append(tables)

        
        xml_root.append(database)

        # Save the modified XML File and close it
        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

        return (0, f"Database {db_name} Created!")

def drop_database(db_name, mongoclient):
    db_name = db_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    else:
        # Remove the database with the matching name
        database_to_remove = xml_root.find(".//Database[@name='{}']".format(db_name))
        xml_root.remove(database_to_remove)

        # Save the modified xml file and close it
        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print = True, xml_declaration = True, encoding = 'UTF-8'))

        # drop the mongo database
        mongoHandler.drop_database(db_name, mongoclient)

        return (0, f"Database {db_name} succesfully dropped!")
        

def table_exists(xml_root, db_name, table_name):
    db_name = db_name.upper()
    table_name = table_name.upper()
    tables = xml_root.findall(".//Database[@name='{}']/Tables/Table[@name='{}']".format(db_name, table_name))
    return len(tables) == 1


# Create table -> db_name, table_name, list_of_columns [(column_name, data_type)]
def create_table(db_name, table_name, list_of_columns, primary_keys, foreign_keys):
    db_name = db_name.upper()
    table_name = table_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} already exists!")
    else:
        # Create the new table element
        table = etree.Element('Table', name=table_name)
        table.text = "\n        "  # Add newline before the <Table> tag
        table.tail = "\n    "  # Add newline after the </Table> tag
        
        # Create the structure element with the specified columns
        structure = etree.SubElement(table, 'Structure')
        structure.text = "\n            "  # Add newline before the <Structure> tag
        structure.tail = "\n        "  # Add newline after the </Structure> tag
        for column in list_of_columns:
            attribute = etree.SubElement(structure, 'Attribute', attributeName=column[0].upper(), type=column[1].upper())
            if column == list_of_columns[-1]:
                attribute.tail = "\n        "  # Add newline after each <Attribute> tag
            else:
                attribute.tail = "\n            "  # Add newline after each <Attribute> tag
        # Create the primary key element
        primary_key = etree.SubElement(table, 'primaryKey')
        primary_key.text = "\n            "  # Add newline before the <primaryKey> tag
        # insert primary key(s): <pkAttribute>column_name</pkAttribute>
        for pk in primary_keys:
            attribute = etree.SubElement(primary_key, 'pkAttribute')
            attribute.text = pk[0].upper()
            # if pk is the last element in the list, add a newline after the </pkAttribute> tag
            if pk == primary_keys[-1]:
                attribute.tail = "\n        "  # Add newline after each <pkAttribute> tag    
            else:
                attribute.tail = "\n            "
        primary_key.tail = "\n        "  # Add newline after the </primaryKey> tag
        
        # Create foreign key element
        foreign_key = etree.SubElement(table, 'foreignKeys')
        foreign_key.text = "\n            "  # Add newline and indentation before the <foreignKeys> tag

        # insert foreign keys:
        for fk in foreign_keys:
            fk_attr = etree.SubElement(foreign_key, 'foreignKey')
            fk_attr.text = fk[0].upper()

            references = etree.SubElement(foreign_key, 'references')
            references.text = "\n                "  # Add newline and indentation before <references> tag
            
            refTable = etree.SubElement(references, 'refTable')
            refTable.text = fk[1].upper()
            refTable.tail = "\n                "  # Add newline and indentation after <refTable> tag

            refAttribute = etree.SubElement(references, 'refAttribute')
            refAttribute.text = fk[2].upper()
            refAttribute.tail = "\n            "  # Add newline and indentation after <refAttribute> tag

            references.tail = "\n        "  # Add newline and indentation before </references> tag
            fk_attr.tail = "\n\n            "  # Add two newlines and indentation before the next <foreignKey> tag

        foreign_key.tail="\n        "  # Add newline and indentation before </foreignKeys> tag


        IndexFiles = etree.SubElement(table, 'IndexFiles')
        IndexFiles.text = "\n\n        "
        IndexFiles.tail = "\n        "

        # Add the new table element to the database
        tables = xml_root.find(".//Database[@name='{}']/Tables".format(db_name))
        tables.append(table)
        tables.tail = "\n"  # Add newline after the </Tables> tag

        # Save the modified xml file and close it
        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

        return (0, f"Table {table_name} created in database {db_name}!")


def drop_table(db_name, table_name, mongodb):
    db_name = db_name.upper()
    table_name = table_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    else:
        # Remove the table with the matching name
        table_to_remove = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']"
                                         .format(db_name, table_name))
        xml_root.find(".//Database[@name='{}']/Tables".format(db_name)).remove(table_to_remove)

        # Save the modified xml file and close it
        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

        # drop the mongo collection
        mongoHandler.drop_collection(table_name, mongodb)

        return (0, f"Table {table_name} in database {db_name} successfully dropped!")


def create_index(db_name, table_name, index_name, columns):
    db_name = db_name.upper()
    table_name = table_name.upper()
    index_name = index_name.upper()
    for column in columns:
        column = column.upper()

    xml_root = parse_xml_file(XML_FILE_LOCATION)

    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    else:
        # create IAttribute element with the column_name inside: i.e. <IAttribute>column_name</IAttribute>
        # have it be inside the indexAttributes element of the table table_name
        indexFiles = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles"
                                                .format(db_name, table_name))
        
        # create container tag
        indexFile = etree.SubElement(indexFiles, 'IndexFile', indexName="idk.ind", indexType="BTree")
        indexFile.text = "\n            "
        indexFile.tail = "\n        "
        for column_name in columns:
            IAttribute = etree.SubElement(indexFile, 'IAttribute')
            IAttribute.text = column_name.upper()
            if column_name == columns[-1]:
                IAttribute.tail = "\n        "  # Add newline after each <IAttribute> tag
            else:
                IAttribute.tail = "\n            "


        # Save the modified xml file and close it
        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

        return (0, f"Index created on column {column_name} in table {table_name} in database {db_name}!")

def validate_data_types(attributes, columns, values):
    # Create a dictionary of column name to attribute type
    column_types = {attr.get('attributeName'): attr.get('type') for attr in attributes}

    # Iterate through the provided columns and values
    for column, value in zip(columns, values):
        expected_type = column_types[column].upper()

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

def find_pk_column(structure):
    # simply return the name of the first column in the xml file
    return structure.find('Attribute').get('attributeName')

def insert_into(db_name, table_name, columns, values, mongodb):
    # check for valid database and table
    db_name = db_name.upper()
    table_name = table_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if db_name != "MASTER" and not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    else:
        # check for valid columns and types of values
        # get the structure of the table
        structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/Structure"
                                    .format(db_name, table_name))
        # get the list of attributes
        attributes = structure.findall('Attribute')
        # check if the number of columns and values match
        if len(columns) != len(values):
            return (-3, f"Error: Number of columns and values do not match!")
        # check if the types of the values match the types of the columns
        for i in range(len(columns)):
            # get the type of the column
            column_type = structure.find(f"Attribute[@attributeName='{columns[i]}']").get('type')
            # check if the type of the value matches the type of the column using the validate_data_types function
            if not validate_data_types(attributes, [columns[i]], [values[i]]):
                return (-4, f"Error: Type of value {values[i]} does not match type of column {columns[i]}!")
        
        # insert the values into the table
        primary_key_column = find_pk_column(structure)
        return mongoHandler.insert_into(mongodb, table_name, primary_key_column, columns, values)

def delete_from(db_name, table_name, filter_conditions, mongodb):
    # check for valid database and table
    db_name = db_name.upper()
    table_name = table_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    else:
        # delete the values from the table
        mongoHandler.delete_from(mongodb, table_name, filter_conditions)

def select_all(db_name, table_name, mongodb):
    # check for valid database and table
    db_name = db_name.upper()
    table_name = table_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    else:
        # select all the values from the table
        return mongoHandler.select_all(mongodb, table_name)