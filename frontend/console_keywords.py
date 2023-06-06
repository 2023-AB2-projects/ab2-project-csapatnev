import xml.etree.ElementTree as ET

# returns all possible sql keywords
def get_sql_keywords():
    return [
        # sql commands:
        'use',
        'create',
        'drop',
        'insert',
        'into',
        'values',
        'delete',
        'from',
        'where',
        'select',
        'from',
        'join',
        'on',
        'group',
        'by',
        'update',
        'primary',
        'key',
        'references',
        'unique',
        'as',
        'not',
        'and',
        'or',
        'like'
        # sql objects:
        'database',
        'table',
        'index',
        # sql types:
        'int',
        'varchar',
        'bit',
        'date',
        'datetime',
        'float'
    ]


def get_sql_function_keywords():
    return [
        'avg()',
        'min()',
        'max()',
        'count()',
        'sum()',
    ]


def get_database_and_table_keywords():
    tree = ET.parse('databases.xml')
    root = tree.getroot()

    DATABASE_KEY_WORDS = []
    TABLE_KEY_WORDS = []
    ATTRIBUTE_KEY_WORDS = []


    for database in root.findall('Database'):
        DATABASE_KEY_WORDS.append(database.attrib['name'])
        for table in database.findall('.//Table'):
            TABLE_KEY_WORDS.append(table.attrib['name'])
            for attribute in table.findall('.//Attribute'):
                ATTRIBUTE_KEY_WORDS.append(attribute.attrib['attributeName'])

    return DATABASE_KEY_WORDS, TABLE_KEY_WORDS, ATTRIBUTE_KEY_WORDS
