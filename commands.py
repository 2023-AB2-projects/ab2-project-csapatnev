from lxml import *
from lxml import etree
import mongoHandler
import datetime

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
def create_table(db_name, table_name, list_of_columns, primary_keys, foreign_keys, unique_keys):
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
            attribute.text = pk.upper()
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

        # Create unique key element
        unique_key = etree.SubElement(table, 'uniqueKeys')
        unique_key.text = "\n            "  # Add newline and indentation before the <uniqueKeys> tag

        for uk in unique_keys:
            uk_attr = etree.SubElement(unique_key, 'uniqueKey')
            uk_attr.text = uk.upper()

            if uk == unique_keys[-1]:
                uk_attr.tail = "\n        "
            else:
                uk_attr.tail = "\n            "

        unique_key.tail = "\n        "  # Add newline and indentation before </uniqueKeys> tag

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


def drop_table(db_name, table_name, mongoclient):
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
        mongoHandler.drop_collection(db_name, table_name, mongoclient)

        return (0, f"Table {table_name} in database {db_name} successfully dropped!")


def index_exists(xml_root, db_name, table_name, index_name):
    # index in table in db exists
    index = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles/IndexFile[@indexName='{}']"
                            .format(db_name, table_name, index_name))
    if index is None:
        return False
    else:
        return True

def create_index(mongoclient, db_name, table_name, index_name, columns):
    db_name = db_name.upper()
    table_name = table_name.upper()
    index_name = index_name.upper()
    index_name = index_name[:-1]
    columns = [column.upper() for column in columns]

    xml_root = parse_xml_file(XML_FILE_LOCATION)

    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    elif index_exists(xml_root, db_name, table_name, index_name):
        return (-3, f"Error: Index {index_name} for table {table_name} already exists!")
    else:
        # create IAttribute element with the column_name inside: i.e. <IAttribute>column_name</IAttribute>
        # have it be inside the indexAttributes element of the table table_name
        indexFiles = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles"
                                                .format(db_name, table_name))
        
        # create container tag
        i_f_name = f"{table_name}_{'_'.join(columns)}_{index_name}_INDEX"

        indexFile = etree.SubElement(indexFiles, 'IndexFile', indexName=index_name, indexType="BTree", indexFileName=i_f_name)
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

        # Create indexFile inside Mongo for these tables
        column_indices = []
        for column in columns:
            # find index of column from structure
            structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/Structure"
                                        .format(db_name, table_name))

            # find index of column from structure
            col_index = None
            for i, attr in enumerate(structure.findall('Attribute')):
                if attr.get('attributeName') == column:
                    col_index = i
                    break

            if column == find_pk_column(structure):
                col_index = -1

            column_indices.append(col_index)

        return mongoHandler.create_index(mongoclient, db_name, table_name, index_name, columns, column_indices)

def find_pk_column(structure):
    pk_element = structure.getparent().find(".//primaryKey/pkAttribute")
    if pk_element is not None:
        return pk_element.text
    
    first_attribute = structure.find(".//Attribute")
    if first_attribute is not None:
        return first_attribute.get("attributeName")

def find_all_fk_columns(structure):
    fk_elements = structure.getparent().findall(".//foreignKeys/foreignKey")
    if fk_elements is not None:
        return [fk_element.text for fk_element in fk_elements]
    


def find_all_unique_columns(structure):
    unique_elements = structure.getparent().findall(".//uniqueKeys/uniqueKey")
    if unique_elements is not None:
        return [unique_element.text for unique_element in unique_elements]

def insert_into(db_name, table_name, columns, values, mongoclient):
    # check for valid database and table
    db_name = db_name.upper()
    table_name = table_name.upper()
    columns = [col.upper() for col in columns]
    values = [val.upper() for val in values]
    
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
        
        # convert types of values to match the data types of the columns
        for i in range(len(columns)):
            for attribute in attributes:
                if attribute.get('attributeName') == columns[i]:
                    # convert the value to the correct data type
                    if attribute.get('type') == 'INT':
                        values[i] = int(values[i])
                    elif attribute.get('type') == 'FLOAT':
                        values[i] = float(values[i])
                    elif attribute.get('type') == 'BIT':
                        values[i] = bool(values[i])
                    elif attribute.get('type') == 'DATE':
                        values[i] = datetime.strptime(values[i], '%Y-%m-%d')
                    elif attribute.get('type') == 'DATETIME':
                        values[i] = datetime.strptime(values[i], '%Y-%m-%d %H:%M:%S')
                    break
        # insert the values into the table
        primary_key_column = find_pk_column(structure)
        foreign_keys = find_all_fk_columns(structure)
        unique_keys = find_all_unique_columns(structure)
        i_f_structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles"
                            .format(db_name, table_name))
        # Extract the index configurations
        index_configs = []

        for index_file in i_f_structure.findall('IndexFile'):
            index_name = index_file.get('indexName')
            index_columns = [i_attribute.text for i_attribute in index_file.findall('IAttribute')]

            index_config = {
                'index_name': index_name,
                'index_columns': index_columns,
                'index_column_indices': [columns.index(col) for col in index_columns]
            }

            index_configs.append(index_config)

        #print(f"primary_key_column: {primary_key_column}\n foreign_keys: {foreign_keys}\n unique_keys: {unique_keys}")
        return mongoHandler.insert_into(mongoclient, db_name, table_name, primary_key_column, foreign_keys, unique_keys, columns, values, index_configs)

def get_column_index(xml_file, db_name, table_name, column_name):
    # get the index of the column inside the db_name.table_name
    # get the structure of the table
    structure = xml_file.find(".//Database[@name='{}']/Tables/Table[@name='{}']/Structure"
                                .format(db_name, table_name))
    # get the list of attributes
    attributes = structure.findall('Attribute')
    # check if the number of columns and values match
    for i in range(len(attributes)):
        if attributes[i].get('attributeName') == column_name:
            return i
    return None

def find_all_columns(structure):
    return [attribute.get('attributeName') for attribute in structure.findall('Attribute')]

def find_all_index_file_names(structure):
    return [index_file.get('indexFileName') for index_file in structure.findall('IndexFile')]

def delete_from(db_name, table_name, filter_conditions, mongoclient):
    # check for valid database and table
    db_name = db_name.upper()
    table_name = table_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    
    structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/Structure"
                                    .format(db_name, table_name))

    unique_keys = find_all_unique_columns(structure)
    foreign_keys = find_all_fk_columns(structure)
    columns = find_all_columns(structure)
    i_f_structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles"
                            .format(db_name, table_name))
    index_file_names = find_all_index_file_names(i_f_structure)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    else:
        return mongoHandler.delete_from(mongoclient, table_name, db_name, filter_conditions, columns, unique_keys, foreign_keys, index_file_names)


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