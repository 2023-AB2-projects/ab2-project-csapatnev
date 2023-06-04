from lxml import *
from lxml import etree
import backend.mongoHandler as mongoHandler
from backend.datagetter import *
import re
from backend.commands_helper import *
from backend.column_finder import *
from backend.select_helper import *
from backend.table_joiner import *

XML_FILE_LOCATION = './databases.xml'


# All server sided functions return a tuple (<error_code>, <error_message>)
# Error_code = 0 -> no error
# Error_code < 0 -> error, message returned needs to be printed on client side

def create_database(db_name):
    db_name = db_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if database_exists(xml_root, db_name):
        return (-1, "Error: Database already exists!")
    else:
        database = etree.Element('Database', name=db_name)
        database.text = "\n    "
        database.tail = "\n\n"

        tables = etree.Element('Tables')
        tables.text = "\n        "
        database.append(tables)

        
        xml_root.append(database)

        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

        return (0, f"Database {db_name} Created!")

def drop_database(db_name, mongoclient):
    db_name = db_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    else:
        database_to_remove = xml_root.find(".//Database[@name='{}']".format(db_name))
        xml_root.remove(database_to_remove)

        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print = True, xml_declaration = True, encoding = 'UTF-8'))

        mongoHandler.drop_database(db_name, mongoclient)

        return (0, f"Database {db_name} succesfully dropped!")
        
def create_table(db_name, table_name, list_of_columns, primary_keys, foreign_keys, unique_keys):
    db_name = db_name.upper()
    table_name = table_name.upper()
    xml_root = parse_xml_file(XML_FILE_LOCATION)
    if not database_exists(xml_root, db_name):
        return (-1, f"Error: Database {db_name} does not exist!")
    elif table_exists(xml_root, db_name, table_name):
        return (-2, f"Error: Table {table_name} in database {db_name} already exists!")
    else:
        table = etree.Element('Table', name=table_name)
        table.text = "\n        "
        table.tail = "\n    "
        
        structure = etree.SubElement(table, 'Structure')
        structure.text = "\n            "
        structure.tail = "\n        "
        for column in list_of_columns:
            attribute = etree.SubElement(structure, 'Attribute', attributeName=column[0].upper(), type=column[1].upper())
            if column == list_of_columns[-1]:
                attribute.tail = "\n        "
            else:
                attribute.tail = "\n            "
        primary_key = etree.SubElement(table, 'primaryKey')
        primary_key.text = "\n            "

        for pk in primary_keys:
            attribute = etree.SubElement(primary_key, 'pkAttribute')
            attribute.text = pk.upper()
            if pk == primary_keys[-1]:
                attribute.tail = "\n        "    
            else:
                attribute.tail = "\n            "
        primary_key.tail = "\n        "
        
        foreign_key = etree.SubElement(table, 'foreignKeys')
        foreign_key.text = "\n            "

        for fk in foreign_keys:
            fk_attr = etree.SubElement(foreign_key, 'foreignKey')
            fk_attr.text = fk[0].upper()

            references = etree.SubElement(foreign_key, 'references')
            references.text = "\n                "
            
            refTable = etree.SubElement(references, 'refTable')
            refTable.text = fk[1].upper()
            refTable.tail = "\n                "

            refAttribute = etree.SubElement(references, 'refAttribute')
            refAttribute.text = fk[2].upper()
            refAttribute.tail = "\n            "

            references.tail = "\n        "
            fk_attr.tail = "\n\n            "

        foreign_key.tail="\n        "

        unique_key = etree.SubElement(table, 'uniqueKeys')
        unique_key.text = "\n            "

        for uk in unique_keys:
            uk_attr = etree.SubElement(unique_key, 'uniqueKey')
            uk_attr.text = uk.upper()

            if uk == unique_keys[-1]:
                uk_attr.tail = "\n        "
            else:
                uk_attr.tail = "\n            "

        unique_key.tail = "\n        "

        IndexFiles = etree.SubElement(table, 'IndexFiles')
        IndexFiles.text = "\n\n        "
        IndexFiles.tail = "\n        "

        tables = xml_root.find(".//Database[@name='{}']/Tables".format(db_name))
        tables.append(table)
        tables.tail = "\n"

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
        table_to_remove = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']"
                                         .format(db_name, table_name))
        xml_root.find(".//Database[@name='{}']/Tables".format(db_name)).remove(table_to_remove)

        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

        mongoHandler.drop_collection(db_name, table_name, mongoclient)

        return (0, f"Table {table_name} in database {db_name} successfully dropped!")

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


        with open(XML_FILE_LOCATION, 'wb') as file:
            file.write(etree.tostring(xml_root, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

        column_indices = []
        for column in columns:
            # find index of column from structure
            structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/Structure"
                                        .format(db_name, table_name))

            col_index = None
            for i, attr in enumerate(structure.findall('Attribute')):
                if attr.get('attributeName') == column:
                    col_index = i
                    break

            if column in find_pk_columns(structure):
                col_index = -1

            column_indices.append(col_index)

        return mongoHandler.create_index(mongoclient, db_name, table_name, index_name, columns, column_indices)
    
def insert_into(db_name, table_name, columns, values, mongoclient):
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
        structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/Structure"
                                    .format(db_name, table_name))
        attributes = structure.findall('Attribute')
        if len(columns) != len(values):
            return (-3, f"Error: Number of columns and values do not match!")
        
        for i in range(len(columns)):
            for attribute in attributes:
                if attribute.get('attributeName') == columns[i]:
                    # convert the value to the correct data type
                    if attribute.get('type') == 'INT':
                        values[i] = int(values[i])
                    break

        primary_key_columns = find_pk_columns(structure)
        foreign_keys = find_all_fk_columns(structure)
        foreign_key_references = find_all_fk_references(structure)
        unique_keys = find_all_unique_columns(structure)
        i_f_structure = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles"
                            .format(db_name, table_name))
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


        return mongoHandler.insert_into(mongoclient, db_name, table_name, primary_key_columns, foreign_keys, unique_keys, columns, values, index_configs, foreign_key_references)

def delete_from(db_name, table_name, filter_conditions, mongoclient):
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
        try:
            retVal, data, join_performed = perform_indexed_nested_loop_join(db_name, join_clause, mongoclient, xml_root)
        except:
            retVal = 2
            join_performed = False
            data = None
            pass
        if retVal < 0:
            return retVal, data
        elif retVal == 1 or retVal == 2:
            retVal, data = perform_nested_join(db_name, join_clause, mongoclient, xml_root, join_performed, starting_data=data)
            if retVal < 0:
                return retVal, data
    else: # handle single table queries
        retVal, data = load_table_data(db_name, from_table_name, mongoclient, xml_root)
        if retVal < 0:
            return retVal, data

    if where_clause:
        print(f"where_clause: {where_clause}")
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