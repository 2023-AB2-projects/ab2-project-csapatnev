from lxml import *
from lxml import etree
import mongoHandler
import datetime
from datagetter import load_table_data, load_table_data_from_index
import re

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

            if column in find_pk_columns(structure):
                col_index = -1

            column_indices.append(col_index)

        return mongoHandler.create_index(mongoclient, db_name, table_name, index_name, columns, column_indices)

def find_pk_columns(structure):
    pk_element = structure.getparent().find(".//primaryKey")
    if pk_element is not None:
        return [pk_attribute.text for pk_attribute in pk_element.findall("pkAttribute")]
    
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

def find_all_fk_references(structure):
    foreign_key_references = {}
    
    # Find the Table element that is the parent of the given structure
    table = structure.getparent()

    # Find foreign keys in the same table as the given structure
    for foreign_keys in table.findall('foreignKeys'):
        for foreign_key in foreign_keys.findall('foreignKey'):
            fk_column = foreign_key.text
            references = foreign_key.getnext()
            ref_table = references.find('refTable').text
            ref_attribute = references.find('refAttribute').text
            foreign_key_references[fk_column] = {
                'collection': ref_table,
                'key': ref_attribute
            }

    return foreign_key_references


def find_all_fk_references_extended(structure):
    foreign_key_references = {}
    
    # Find the Tables element, which is the grandparent of the given structure
    tables = structure.getparent().getparent()

    # Iterate over all tables
    for table in tables.findall("Table"):
        # Find foreign keys in the current table
        foreign_keys_element = table.find("foreignKeys")
        if foreign_keys_element is not None:
            for foreign_key_element in foreign_keys_element.findall("foreignKey"):
                fk_column = foreign_key_element.text
                references = foreign_key_element.getnext()
                ref_table = references.find("refTable").text
                ref_attribute = references.find("refAttribute").text

                # Check if the foreign key references the table related to the given structure
                if ref_table == structure.getparent().attrib["name"]:
                    foreign_key_references[fk_column] = {
                        "collection": ref_table,
                        "key": ref_attribute,
                        "referencing_collection": table.attrib["name"],
                        "referencing_key": fk_column,
                    }

    return foreign_key_references


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
                    break
        # insert the values into the table
        primary_key_columns = find_pk_columns(structure)
        foreign_keys = find_all_fk_columns(structure)
        foreign_key_references = find_all_fk_references(structure)
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
        return mongoHandler.insert_into(mongoclient, db_name, table_name, primary_key_columns, foreign_keys, unique_keys, columns, values, index_configs, foreign_key_references)

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
    foreign_key_references = find_all_fk_references_extended(structure)
    columns = find_all_columns(structure)
    i_f_structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles"
                            .format(db_name, table_name))
    index_file_names = find_all_index_file_names(i_f_structure)
    primary_key_columns = find_pk_columns(structure)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif not table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")
    else:
        return mongoHandler.delete_from(mongoclient, table_name, db_name, filter_conditions, columns, unique_keys, foreign_keys, foreign_key_references, index_file_names, primary_key_columns)

def distinctify(data):
    seen = {}
    for key, value in data.items():
        pk_data = tuple(value['pk'].items())
        if pk_data not in seen:
            seen[pk_data] = value
    return seen

def apply_condition(data, condition):
    operator = condition['operator']
    lhs = condition['lhs']
    rhs = condition['rhs']

    new_data = {}

    if rhs['type'] == 'column':
        lhs_column_name = lhs['column_name'].upper()
        rhs_column_name = rhs['column_name'].upper()
        
        for _id, row in data.items():
            column_value_lhs = row['pk'][lhs_column_name] if lhs_column_name in row['pk'] else row['value'][lhs_column_name]
            column_value_rhs = row['pk'][rhs_column_name] if rhs_column_name in row['pk'] else row['value'][rhs_column_name]
            if compare_values(operator, column_value_lhs, column_value_rhs):
                new_data[_id] = row
    else:
        lhs_column_name = lhs['column_name'].upper()
        
        for _id, row in data.items():
            column_value = row['pk'][lhs_column_name] if lhs_column_name in row['pk'] else row['value'][lhs_column_name]
            comparison_value = rhs['value']
            if compare_values(operator, column_value, comparison_value):
                new_data[_id] = row

    if new_data == {}:
        # return the first row of the original data, but with the values set to none for each column
        dummydata = list(data.items())
        new_data = {
            0: {
                'pk': {column_name: None for column_name in dummydata[0][1]['pk'].keys()},
                'value': {column_name: None for column_name in dummydata[0][1]['value'].keys()}
            }
        }
    return new_data


def compare_values(operator, value1, value2):
    if isinstance(value1, str):
        value1 = value1.strip('\'"')
    if isinstance(value2, str):
        value2 = value2.strip('\'"')
   
    if operator == "$eq":
        return value1 == value2
    elif operator == "$gt":
        return value1 > value2
    elif operator == "$gte":
        return value1 >= value2
    elif operator == "$lt":
        return value1 < value2
    elif operator == "$lte":
        return value1 <= value2
    elif operator == "$ne":
        return value1 != value2
    elif operator == "$regex":
        return re.search(value2, value1)
    else:
        return False

def apply_where_clause(data, where_clause):
    for condition in where_clause:
        data = apply_condition(data, condition)
    return data

def filter_columns(data, select_clause):
    # Prepare a list of column names from the select_clause
    column_names = {col['column_name'].upper(): (col.get('alias') or col['column_name']).upper() for col in select_clause}

    for _id, row in data.items():
        new_pk = {}
        new_value = {}

        for original_name, alias_name in column_names.items():
            if original_name in row['pk']:
                new_pk[alias_name] = row['pk'][original_name]
            if original_name in row['value']:
                new_value[alias_name] = row['value'][original_name]
        
        row['pk'] = new_pk
        row['value'] = new_value

    return data

def get_index_columns(db_name, table_name, xml_root):
    # Fetch index columns for a table from XML
    index_columns = []
    indexes = xml_root.findall(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/IndexFiles/IndexFile")
    for index in indexes:
        index_cols = [iattrib.text.upper() for iattrib in index.findall('IAttribute')]
        index_columns.append(index_cols)

    # Automatically generated index files
    # Fetch unique key columns for the table
    uindexes = xml_root.findall(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/uniqueKeys/uniqueKey")
    for uindex in uindexes:
        uindex_cols = uindex.text.upper()
        index_columns.append([uindex_cols])

    # Fetch foreign key columns for the table
    findexes = xml_root.findall(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/foreignKeys/foreignKey")
    for findex in findexes:
        findex_cols = findex.text.upper()
        index_columns.append([findex_cols])

    return index_columns

def get_index_name(db_name, table_name, index_columns, xml_root, mongo_client):
    # Define the general formats of the index file names
    formats = ['{table}_{column}_UNIQUE_INDEX', '{table}_{column}_FOREIGN_INDEX']

    for index_column in index_columns:
        # Check for user-defined index files first
        index_file_elem = xml_root.find(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/IndexFiles/IndexFile[IAttribute='{index_column}']")
        if index_file_elem is not None:
            return index_file_elem.get('indexFileName')

        # Then check for automatically generated index files
        for fmt in formats:
            index_name = fmt.format(table=table_name, column=index_column)
            db = mongo_client[db_name]
            if index_name in db.list_collection_names():
                return index_name
    # If no matching index file was found, return None
    return None

def load_index_data(db_name, index_name, mongo_client):
    db = mongo_client[db_name]
    collection = db[index_name]

    # Here, each document's '_id' is the indexed columns' values concatenated with '#',
    # and 'value' is the primary key of the corresponding row in the table
    data = {doc['_id']: doc['value'] for doc in collection.find()}

    return 0, data

def perform_nested_join(db_name, join_clause, mongoclient, xml_root, starting_data=None):
    print("SIMPLE NESTED LOOP JOIN ALGORITHM IS TO BE PERFORMED")
    
    if starting_data is None:
        retVal, data = load_table_data(db_name, join_clause[0]['lhs']['table_name'].upper(), mongoclient, xml_root)
        if retVal < 0:
            return retVal, data
        starting_join_index = 0  # start the join from the first join in the join clause
    else:
        data = starting_data  # use the provided starting data as the left-hand side table in the join operation
        starting_join_index = 1  # start the join from the second join in the join clause

    for join in join_clause[starting_join_index:]:
        retVal, rhs_data = load_table_data(db_name, join['rhs']['table_name'].upper(), mongoclient, xml_root)
        if retVal < 0:
            return retVal, rhs_data

        new_data = {}

        for lhs_id, lhs_row in data.items():
            lhs_column_name = join['lhs']['column_name'].upper()
            lhs_value = lhs_row['value'].get(lhs_column_name, lhs_row['pk'].get(lhs_column_name))

            for rhs_id, rhs_row in rhs_data.items():
                rhs_column_name = join['rhs']['column_name'].upper()
                rhs_value = rhs_row['value'].get(rhs_column_name, rhs_row['pk'].get(rhs_column_name))

                if lhs_value == rhs_value:
                    # Construct the new row data
                    new_row = {'pk': {**lhs_row['pk'], **rhs_row['pk']},
                               'value': {**lhs_row['value'], **rhs_row['value']}}

                    # If lhs_id is not string convert it to a string
                    if not isinstance(lhs_id, str):
                        lhs_id = str(lhs_id)
                    # If rhs_id is not string convert it to a string
                    if not isinstance(rhs_id, str):
                        rhs_id = str(rhs_id)
                    new_id = lhs_id + "#" + rhs_id
                    new_data[new_id] = new_row

        data = new_data  # Update data for the next join

    return retVal, data

def value_exists_in_index_data(outer_column_value, index_data):
    for value in index_data.values():
        split_value = value.split('#')
        if outer_column_value in split_value:
            return True
        for val in split_value:
            try:
                val = int(val)
            except ValueError:
                pass
            if outer_column_value == val:
                return True
    return False

def perform_indexed_nested_loop_join(db_name, join_clause, mongo_client, xml_root, starting_data=None):
    data = {}
    retVal = 0

    if starting_data is None:
        # Load the data for the first table in the join clause
        retVal, outer_table_data = load_table_data(db_name, join_clause[0]['lhs']['table_name'].upper(), mongo_client, xml_root)
        if retVal < 0:
            return retVal, outer_table_data
        starting_join_index = 0  # start the join from the first join in the join clause
    else:
        outer_table_data = starting_data  # use the provided starting data as the left-hand side table in the join operation
        starting_join_index = 1  # start the join from the second join in the join clause

    for join in join_clause[starting_join_index:]:
        rhs_table_name = join['rhs']['table_name'].upper()

        # Get the index columns of the inner table
        inner_table_index_columns = get_index_columns(db_name, rhs_table_name, xml_root)

        # Use the join condition to select the appropriate index
        join_column = join['rhs']['column_name'].upper()
        index_columns = [index for index in inner_table_index_columns if join_column in index]

        # If there are no suitable indexes, return an error
        if not index_columns:
            return 1, outer_table_data
        
        print("INDEXED NESTED LOOP JOIN ALGORITHM IS TO BE PERFORMED")

        index_column = index_columns[0]  # choose the first suitable index

        # Load the index data
        index_name = get_index_name(db_name, rhs_table_name, index_column, xml_root, mongo_client)
        retVal, index_data = load_index_data(db_name, index_name, mongo_client)
        if retVal < 0:
            return retVal, index_data

        # Perform the Indexed Nested Loop Join
        new_data = {}
        for outer_id, outer_row in outer_table_data.items():
            outer_column_value = outer_row['value'].get(join['lhs']['column_name'].upper(), outer_row['pk'].get(join['lhs']['column_name'].upper()))

            try:
                outer_column_value = int(outer_column_value)
            except ValueError:
                pass

            # Check if the outer column value exists in the index
            if value_exists_in_index_data(outer_column_value, index_data):
                # Get the primary key of the matching row(s) in the inner table
                inner_pks = index_data[outer_column_value]  # this could be a list if there are multiple matches
                
                # Load the matching rows from the inner table
                retVal, matching_rows = load_table_data_from_index(db_name, rhs_table_name, {outer_column_value: inner_pks}, mongo_client, xml_root)
                if retVal < 0:
                    return retVal, matching_rows

                # Merge the outer row with each matching inner row
                for inner_id, inner_row in matching_rows.items():
                    new_row = {'pk': {**outer_row['pk'], **inner_row['pk']},
                               'value': {**outer_row['value'], **inner_row['value']}}
                    print(f"new_row: {new_row}")

                    # Ensure the ids are strings for concatenation
                    outer_id = str(outer_id)
                    inner_id = str(inner_id)
                    
                    new_id = outer_id + "#" + inner_id
                    new_data[new_id] = new_row

        # Replace the outer_table_data with the newly joined data for the next join operation
        outer_table_data = new_data

    return retVal, outer_table_data


def simplify_result(dbms_result):
    simplified_result = {
        k: {**v['pk'], **v['value']}
        for k, v in dbms_result.items()
    }

    # Remove duplicates
    simplified_result = list(simplified_result.values())

    return simplified_result

def apply_aggregate_function(data, select_clause, groupby_clause):
    # Determine if data is grouped by checking if keys are tuples
    is_grouped = isinstance(next(iter(data.keys())), tuple)

    results = []

    if is_grouped:
        for group_key, group_rows in data.items():
            result = {}
            # Include group-by columns in the result
            if isinstance(group_key, tuple):
                for i, col_name in enumerate(groupby_clause):
                    result[col_name] = group_key[i]
            else:
                result[groupby_clause[0]] = group_key

            for select_item in select_clause:
                function = select_item.get('function')
                column_name = select_item['column_name'].upper()
                if function:
                    column_values = group_rows['pk'].get(column_name, []) + group_rows['value'].get(column_name, [])
                    # Apply the aggregate function
                    result[select_item.get('alias', column_name)] = apply_function(function, column_values)
            results.append(result)
    else:
        result = {}
        for select_item in select_clause:
            function = select_item.get('function')
            column_name = select_item['column_name'].upper()
            if function:
                column_values = []
                for _id, row in data.items():
                    if column_name in row['pk']:
                        try:
                            row['pk'][column_name] = int(row['pk'][column_name])
                        except ValueError:
                            pass
                        column_values.append(row['pk'][column_name])
                    if column_name in row['value']:
                        try:
                            row['value'][column_name] = int(row['value'][column_name])
                        except ValueError:
                            pass
                        column_values.append(row['value'][column_name])
                # Apply the aggregate function
                result[select_item.get('alias', column_name)] = apply_function(function, column_values)
        results.append(result)

    return 0, results

def apply_function(function, column_values):
    # Apply the aggregate function
    if function == 'avg':
        column_values = [int(value) for value in column_values]
        result = sum(column_values) / len(column_values) if column_values else None
    elif function == 'count':
        result = len(column_values)
    elif function == 'min':
        result = min(column_values) if column_values else None
    elif function == 'max':
        result = max(column_values) if column_values else None
    elif function == 'sum':
        column_values = [int(value) for value in column_values]
        result = sum(column_values) if column_values else None
    else:
        result = None  # ignore unsupported functions
    return result

def group_by(data, groupby_clause):
    grouped_data = {}
    tmp = []
    for d in groupby_clause:
        tmp.append(d['column_name'].upper())
    groupby_clause = tmp

    for _id, row in data.items():
        key = tuple(row['pk'].get(col, row['value'].get(col, None)) for col in groupby_clause)
        
        if key not in grouped_data:
            grouped_data[key] = {'pk': {}, 'value': {}}

        # Add grouped columns to 'pk'
        for col in groupby_clause:
            if col in row['pk']:
                if col not in grouped_data[key]['pk']:
                    grouped_data[key]['pk'][col] = []
                grouped_data[key]['pk'][col].append(row['pk'][col])
            elif col in row['value']:
                if col not in grouped_data[key]['value']:
                    grouped_data[key]['value'][col] = []
                grouped_data[key]['value'][col].append(row['value'][col])

        # Add non-grouped columns to 'value'
        for col in set(row['pk'].keys()).difference(groupby_clause):
            if col not in grouped_data[key]['pk']:
                grouped_data[key]['pk'][col] = []
            grouped_data[key]['pk'][col].append(row['pk'][col])
        for col in set(row['value'].keys()).difference(groupby_clause):
            if col not in grouped_data[key]['value']:
                grouped_data[key]['value'][col] = []
            grouped_data[key]['value'][col].append(row['value'][col])

    return grouped_data


def select(db_name, select_clause, select_distinct, from_clause, join_clause, where_clause, groupby_clause, mongoclient):
    db_name = db_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)

    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")

    from_table_name = from_clause.upper()

    if not table_exists(xml_root, db_name, from_table_name):
        return (-2, f"Error: Table {from_table_name} in database {db_name} does not exist!")

    if join_clause: # handle multi-table queries (with JOINs)
        print(f"join_clause: {join_clause}")
        retVal, data = perform_indexed_nested_loop_join(db_name, join_clause, mongoclient, xml_root)
        if retVal < 0:
            return retVal, data
        elif retVal == 1:
            retVal, data = perform_nested_join(db_name, join_clause, mongoclient, xml_root, starting_data=data)
            if retVal < 0:
                return retVal, data
    else: # handle single table queries
        retVal, data = load_table_data(db_name, from_table_name, mongoclient, xml_root)
        if retVal < 0:
            return retVal, data

    if where_clause:
        data = apply_where_clause(data, where_clause)

    if groupby_clause:
        data = group_by(data, groupby_clause)
    
    # check if we have any aggregate functions in the select clause
    has_aggregate_function = False
    for select_item in select_clause:
        if select_item.get('function'):
            has_aggregate_function = True
            break
    
    if has_aggregate_function:
        tmp = []
        for d in groupby_clause:
            tmp.append(d['column_name'].upper())
        retVal, aggregate_results = apply_aggregate_function(data, select_clause, tmp)
        return retVal, aggregate_results

    print(f"data: {data}")

    if select_clause[0]['column_name'] != '*':
        data = filter_columns(data, select_clause)

    if select_distinct:
        data = distinctify(data)

    return (0, simplify_result(data))