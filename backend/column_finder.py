from lxml import *
from lxml import etree
import backend.mongoHandler as mongoHandler
from backend.datagetter import *
import re
from backend.commands_helper import *
from backend.column_finder import *
from backend.select_helper import *

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
    
    table = structure.getparent()

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
    
    tables = structure.getparent().getparent()

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

def find_all_columns(structure):
    return [attribute.get('attributeName') for attribute in structure.findall('Attribute')]

def find_all_index_file_names(structure):
    return [index_file.get('indexFileName') for index_file in structure.findall('IndexFile')]
