from lxml import *
from lxml import etree
import backend.mongoHandler as mongoHandler
from backend.datagetter import load_table_data, load_table_data_from_index
import re

def parse_xml_file(file_name):
    file_name = file_name.upper()
    with open(file_name, 'rb') as file:
        content = file.read()
    return etree.fromstring(content)

def database_exists(xml_root, database_name):
    database_name = database_name.upper()
    databases = xml_root.findall(".//Database[@name='{}']".format(database_name))
    return len(databases) > 0

def table_exists(xml_root, db_name, table_name):
    db_name = db_name.upper()
    table_name = table_name.upper()
    tables = xml_root.findall(".//Database[@name='{}']/Tables/Table[@name='{}']".format(db_name, table_name))
    return len(tables) == 1

def index_exists(xml_root, db_name, table_name, index_name):
    # index in table in db exists
    index = xml_root.find(".//Database[@name='{}']/Tables/Table[@name='{}']/IndexFiles/IndexFile[@indexName='{}']"
                            .format(db_name, table_name, index_name))
    if index is None:
        return False
    else:
        return True
    
def get_column_index(xml_file, db_name, table_name, column_name):

    structure = xml_file.find(".//Database[@name='{}']/Tables/Table[@name='{}']/Structure"
                                .format(db_name, table_name))
    attributes = structure.findall('Attribute')
    for i in range(len(attributes)):
        if attributes[i].get('attributeName') == column_name:
            return i
    return None
