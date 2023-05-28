from lxml import *
from lxml import etree
import mongoHandler
import datetime
from datagetter import load_table_data
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
    return new_data

def compare_values(operator, value1, value2):
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

def select(db_name, select_clause, select_distinct, from_clause, join_clause, where_clause, groupby_clause, mongoclient):
    # check if it's a simple SELECT * clause
    db_name = db_name.upper()
    table_name = from_clause.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)

    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    if select_clause[0]['column_name'] == '*':
        if not table_exists(xml_root, db_name, table_name):
            return (-2, f"Error: Table {table_name} in database {db_name} does not exist!")

        retVal, data = load_table_data(db_name, table_name, mongoclient, xml_root)
        if retVal < 0:
            return retVal, data
        # do we want disctinct values?
        if select_distinct:
            data = distinctify(data)
        return (0, data)

    else:
        retVal, data = load_table_data(db_name, table_name, mongoclient, xml_root)
        if retVal < 0:
            return retVal, data
        # if there is a where clause, apply it
        if where_clause:
            data = apply_where_clause(data, where_clause)
        # keep only the columns in the select clause
        data = filter_columns(data, select_clause)
        return (0, data)

