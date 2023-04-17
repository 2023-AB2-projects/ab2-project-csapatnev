# return (code) values:
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE
#   6 - USE
#   7 - INSERT
#   8 - DELETE
#   9 - UPDATE

import re


def parse_handle_invalid_object_type():
    return {
        'code': -1,
        'message': 'Invalid sql Object type!'
    }


def parse_handle_invalid_sql_command():
    return {
        'code': -1,
        'message': 'Invalid SQL command!'
    }


def parse_handle_invalid_syntax_for_creating_database():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating database!'
    }


def parse_handle_invalid_syntax_for_creating_table():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating a table.'
    }


def parse_handle_invalid_syntax_for_creating_index():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating index!'
    }


def parse_handle_invalid_syntax_for_dropping_database():
    return {
        'code': -1,
        'message': 'Invalid syntax for dropping database!'
    }


def parse_handle_invalid_syntax_for_dropping_table():
    return {
        'code': -1,
        'message': 'Invalid syntax for dropping table!'
    }


def parse_handle_invalid_syntax_for_use():
    return {
        'code': -1,
        'message': 'Invalid syntax for use!'
    }


def parse_handle_no_input():
    return {
        'code': -1,
        'message': 'No sql command has been provided!'
    }


def parse_handle_invalid_syntax_for_inserting():
    return {
        'code': -1,
        'message': 'Invalid syntax for inserting!'
    }


def parse_handle_invalid_syntax_for_deleting():
    return {
        'code': -1,
        'message': 'Invalid syntax for deleting!'
    }


def parse_handle_invalid_syntax_for_updating():
    return {
        'code': -1,
        'message': 'Invalid syntax for updating!'
    }


OPERATORS = {
    "=": "$eq",
    ">": "$gt",
    ">=": "$gte",
    "<": "$lt",
    "<=": "$lte",
    "<>": "$ne"
}


def parse_where_clause(where_clause):
    filter_conditions = {}
    top_level_operator = "$and"
    conditions = re.split(r'\s+and\s+', where_clause, flags=re.IGNORECASE)

    if len(conditions) > 1:
        filter_conditions[top_level_operator] = []

        for condition in reversed(conditions):
            match_or = re.match(r'\s+or\s+', condition, flags=re.IGNORECASE)

            if match_or != None:
                sub_conditions = re.split(
                    r'\s+or\s+', condition, flags=re.IGNORECASE)
                or_conditions = []
                for sub_condition in sub_conditions:
                    or_conditions.append(parse_condition(sub_condition))
                filter_conditions[top_level_operator].append(
                    {"$or": or_conditions})
            else:
                filter_conditions[top_level_operator].append(
                    parse_condition(condition))
    else:
        filter_conditions = parse_condition(where_clause)

    return filter_conditions


def parse_condition(condition):
    match_or = re.match(r'\s+or\s+', condition, flags=re.IGNORECASE)
    match_not = re.match(r'(\s+|\s*)not\+', condition, flags=re.IGNORECASE)
    if match_or != None:
        sub_conditions = re.split(r'\s*or\s*', condition, flags=re.IGNORECASE)
        or_conditions = []
        for sub_condition in sub_conditions:
            or_conditions.append(parse_condition(sub_condition))
        return {"$or": or_conditions}
    elif match_not != None:
        condition = re.split(r'(\s+|\s*)not\s+',
                             condition, flags=re.IGNORECASE)
        column = re.split(r' ', condition[1], flags=re.IGNORECASE)[0]
        column = column.upper()
        operator = re.split(r' ', condition[1], flags=re.IGNORECASE)[1]
        value = re.split(r' ', condition[1], flags=re.IGNORECASE)[2]
        return {column: {"$not": {OPERATORS[operator]: value}}}
    else:
        column = re.split(r' ', condition, flags=re.IGNORECASE)[0]
        column = column.upper()
        operator = re.split(r' ', condition, flags=re.IGNORECASE)[1]
        value = re.split(r' ', condition, flags=re.IGNORECASE)[2]
        return {column: {OPERATORS[operator]: value}}


# parse handling of the "create database" command, returned CODE: 1
def parse_handle_create_database(syntax_in_sql):
    create_database_pattern = r'^create\s+database\s+(\w+)\s*;?$'
    match = re.match(create_database_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_database()
    else:
        database_name = match.group(1)
        return {
            'code': 1,
            'type': 'create',
            'object_type': 'database',
            'database_name': database_name
        }


# getting the composite primary keys if there are any
def parse_handle_create_table_composite_primary_keys(columns_definitions_str):
    composite_primary_keys = []
    composite_pk_pattern = r'^(.*)\s+primary\s+key\s*\(\s*(.*)\s*\)\s*$'
    composite_pk_match = re.match(
        composite_pk_pattern, columns_definitions_str, flags=re.IGNORECASE | re.DOTALL)

    if composite_pk_match != None:
        primary_keys_str = composite_pk_match.group(2)

        for primary_key in primary_keys_str.split(','):
            composite_primary_keys.append(primary_key.strip())

        columns_definitions_str = composite_pk_match.group(
            1).rstrip().rstrip(',')
    return composite_primary_keys, columns_definitions_str


# getting the column definitions, references and the primary key from the create table command
def parse_handle_create_table_col_ref_pk_uq(columns_definitions_str):
    column_definitions = []
    primary_keys = []
    references = []
    unique = []

    for column_definition_str in columns_definitions_str.split(','):
        column_definition_pattern = r'^\s*(\w+)\s+([\w\(\)0-9]+)\s*(?:(.*)\s*)?$'
        column_definition_match = re.match(
            column_definition_pattern, column_definition_str.strip(), re.IGNORECASE)

        if column_definition_match != None:
            data_type_pattern = r'^\s*(int|varchar)(?:\(.*\))\s*$'
            data_type_match = re.match(
                data_type_pattern, column_definition_match.group(2), re.IGNORECASE)

            if data_type_match is None:
                data_type_pattern = r'^\s*(int|varchar|bit|date|datetime|float)\s*$'
                data_type_match = re.match(
                    data_type_pattern, column_definition_match.group(2), re.IGNORECASE)
                if data_type_match is None:
                    return column_definitions, primary_keys, references, unique, -1

            column_name = column_definition_match.group(1)
            data_type = data_type_match.group(1)
            column_definitions.append((column_name, data_type))

            pk_pattern = r'^\s*primary\s+key\s*$'
            match_pk = re.match(
                pk_pattern, column_definition_match.group(3), re.IGNORECASE)

            ref_pattern = r'^\s*references\s+(\w+)\s*\((\w+)\)\s*$'
            match_ref = re.match(
                ref_pattern, column_definition_match.group(3), re.IGNORECASE)

            uq_pattern = r'^\s*unique\s*$'
            match_uq = re.match(
                uq_pattern, column_definition_match.group(3), re.IGNORECASE)

            w_pattern = r'^\s*$'
            match_w = re.match(
                w_pattern, column_definition_match.group(3), re.IGNORECASE)

            if match_pk != None:
                if len(primary_keys) >= 1:
                    return column_definitions, primary_keys, references, unique, -1
                else:
                    primary_keys.append(column_name)

            if match_ref != None:
                ref_table_name = match_ref.group(1)
                ref_table_name = match_ref.group(1)
                ref_column_name = match_ref.group(2)
                references.append(
                    (column_name, ref_table_name, ref_column_name))

            if match_uq != None:
                unique.append(column_name)

            if match_ref == None and match_pk == None and match_w == None and match_uq == None:
                return column_definitions, primary_keys, references, unique, -1
        else:
            return column_definitions, primary_keys, references, unique, -1

    return column_definitions, primary_keys, references, unique, 0


# parse handling of the "create table" command, returned CODE: 2
def parse_handle_create_table(syntax_in_sql):
    create_table_pattern = r'^create\s+table\s+(\w+)\s*\(\s*(.*)\s*\)\s*;?$'
    match = re.match(create_table_pattern, syntax_in_sql,
                     flags=re.IGNORECASE | re.DOTALL)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_table()
    else:
        table_name = match.group(1)
        columns_definitions_str = match.group(2)

        composite_primary_keys, columns_definitions_str = parse_handle_create_table_composite_primary_keys(
            columns_definitions_str)

        column_definitions, primary_keys, references, unique, status_code = parse_handle_create_table_col_ref_pk_uq(
            columns_definitions_str)
        if status_code < 0:
            return parse_handle_invalid_syntax_for_creating_table()

        if len(primary_keys) != 0 and len(composite_primary_keys) != 0:
            return parse_handle_invalid_syntax_for_creating_table()

        for column_name in composite_primary_keys:
            if column_name not in [column for (column, _) in column_definitions]:
                return parse_handle_invalid_syntax_for_creating_table()

        if len(composite_primary_keys) != 0:
            return {
                'code': 2,
                'type': 'create',
                'object_type': 'table',
                'table_name': table_name,
                'column_definitions': column_definitions,
                'primary_keys': composite_primary_keys,
                'references': references,
                'unique': unique
            }

        return {
            'code': 2,
            'type': 'create',
            'object_type': 'table',
            'table_name': table_name,
            'column_definitions': column_definitions,
            'primary_keys': primary_keys,
            'references': references,
            'unique': unique
        }


# getting the columns from the create index command string
def parse_handle_create_index_columns(columns_str):
    columns = []
    for columns_str_splited in columns_str.split(','):
        column_match = re.match(r'\s*(\w+)\s*', columns_str_splited)
        if column_match is None:
            return columns, -1
        else:
            column = column_match.group(1)
            columns.append(column)
    return columns, 0


# parse handling of the "create index" command, returned CODE: 3
def parse_handle_create_index(syntax_in_sql):
    create_index_pattern = r'^create\s+index\s+((?:\w+\s+)?)on\s+(\w+)\s*\((.*)\)\s*;?$'
    match = re.match(create_index_pattern, syntax_in_sql, flags=re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_index()
    else:
        index_name = match.group(1)
        table_name = match.group(2)
        columns_str = match.group(3)
        columns, status_code = parse_handle_create_index_columns(columns_str)

        if status_code < 0:
            return parse_handle_invalid_syntax_for_creating_index()

        return {
            'code': 3,
            'type': 'create',
            'object_name': 'index',
            'index_name': index_name,
            'table_name': table_name,
            'columns': columns
        }


# parse handling of the "drop database" command, returned CODE: 4
def parse_handle_drop_database(syntax_in_sql):
    drop_database_pattern = r'^drop\s+database\s+(\w+)\s*;?$'
    match = re.match(drop_database_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_dropping_database()
    else:
        database_name = match.group(1)
        return {
            'code': 4,
            'type': 'drop',
            'object_type': 'database',
            'database_name': database_name
        }


# parse handling of the "drop table" command, returned CODE: 5
def parse_handle_drop_table(syntax_in_sql):
    drop_table_pattern = r'^drop\s+table\s+(\w+)\s*;?$'
    match = re.match(drop_table_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_dropping_table()
    else:
        table_name = match.group(1)
        return {
            'code': 5,
            'type': 'drop',
            'object_type': 'table',
            'table_name': table_name
        }


# parse handling of the "use" command, returned CODE: 6
def parse_handle_use(syntax_in_sql):
    use_pattern = r'^use\s+(\w+)\s*;?$'
    match = re.match(use_pattern, syntax_in_sql, flags=re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_use()
    else:
        database_name = match.group(1)
        return {
            'code': 6,
            'type': 'use',
            'database_name': database_name
        }


# getting the columns from the insert command string
def parse_handle_insert_columns(columns_str):
    columns = []
    for columns_str_splited in columns_str.split(','):
        column_match = re.match(r'\s*(\w+)\s*', columns_str_splited)
        if column_match is None:
            return columns, -1
        else:
            column = column_match.group(1)
            columns.append(column)
    return columns, 0


# getting the values from the insert command string
def parse_handle_insert_values(values_str):
    values = []
    for values_str_splited in values_str.split(','):
        value_match = re.match(r'\s*([\w\' \@\.\-\_]+)\s*', values_str_splited)
        if value_match is None:
            return values, -1
        else:
            value = value_match.group(1)
            values.append(value)
    return values, 0


# parse handling of the "insert" command, returned CODE: 7
def parse_handle_insert(syntax_in_sql):
    insert_pattern = r'^insert\s+into\s+(\w+)\s*\((.*)\)\s*values\s*\((.*)\)\s*;?$'
    match = re.match(insert_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_inserting()
    else:
        table_name = match.group(1)

        columns_str = match.group(2)
        columns, status_code = parse_handle_insert_columns(columns_str)
        if status_code < 0:
            return parse_handle_invalid_syntax_for_inserting()

        values_str = match.group(3)
        values, status_code = parse_handle_insert_values(values_str)
        if status_code < 0:
            return parse_handle_invalid_syntax_for_inserting()

        if len(values) != len(columns):
            return parse_handle_invalid_syntax_for_inserting()

        return {
            'code': 7,
            'type': 'insert',
            'table_name': table_name,
            'columns': columns,
            'values': values
        }


# parse handling of the "delete" command, returned CODE: 8
def parse_handle_delete(syntax_in_sql):
    delete_pattern = r'^delete\s+from\s+(\w+)\s+where\s+([^;.,]*|.*)\s*(?:;)?$'
    match = re.match(delete_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_deleting()
    else:
        table_name = match.group(1)
        condition_str = match.group(2)

        condition = parse_where_clause(condition_str)
        return {
            'code': 8,
            'type': 'delete',
            'table_name': table_name,
            'condition': condition
        }


def parse_handle_update_set_modifications(modification_str):
    modification_pattern = r'\s*(\w+)\s*=\s*(.*)\s*'
    modification_match = re.match(
        modification_pattern, modification_str, re.IGNORECASE)
    
    if modification_match != None:
        column_name = modification_match.group(1)
        value = modification_match.group(2)
        return (column_name, value), 0
    else:
        return (None, None), -1


def parse_handle_update(syntax_in_sql):
    update_pattern = r'^update\s+(\w+)\s+set\s+(.*)\s+where\s+([^;.,]*|.*)\s*(?:;)?$'
    match = re.match(update_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_updating()
    else:
        table_name = match.group(1)

        modification_str = match.group(2)
        modification, status_code = parse_handle_update_set_modifications(modification_str)
        if status_code < 0:
            return parse_handle_invalid_syntax_for_updating()

        condition_str = match.group(3)
        condition = parse_where_clause(condition_str)
        return {
            'code': 9,
            'type': 'update',
            'table_name': table_name,
            'modification': modification,
            'condition': condition
        }


# parsing of every single sql command
def parse(syntax_in_sql: str):
    # spliting the input command string so it can determine which command keyword it contains
    syntax_in_sql_splited = re.findall(r'\w+|[^\w\s]', syntax_in_sql.upper())

    if len(syntax_in_sql_splited) == 0:
        return parse_handle_no_input()

    command_type = syntax_in_sql_splited[0]

    # handling of every single command
    if command_type == 'CREATE':
        if len(syntax_in_sql_splited) == 1:
            return parse_handle_invalid_object_type()

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            return parse_handle_create_database(syntax_in_sql)
        elif object_type == 'TABLE':
            return parse_handle_create_table(syntax_in_sql)
        elif object_type == 'INDEX':
            return parse_handle_create_index(syntax_in_sql)
        else:
            return parse_handle_invalid_object_type()

    elif command_type == 'DROP':
        if len(syntax_in_sql_splited) < 3:
            return parse_handle_invalid_object_type()

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            return parse_handle_drop_database(syntax_in_sql)
        elif object_type == 'TABLE':
            return parse_handle_drop_table(syntax_in_sql)
        else:
            return parse_handle_invalid_object_type()
    elif command_type == 'USE':
        if len(syntax_in_sql_splited) < 2:
            return parse_handle_invalid_object_type()
        return parse_handle_use(syntax_in_sql)
    elif command_type == 'INSERT':
        return parse_handle_insert(syntax_in_sql)
    elif command_type == 'DELETE':
        return parse_handle_delete(syntax_in_sql)
    elif command_type == 'UPDATE':
            return parse_handle_update(syntax_in_sql)
    else:
        return parse_handle_invalid_sql_command()


# handle all of the input string as an sql code
def handle_my_sql_input(input_str: str):
    # removes all the substrings between sql comment identifiers /*(...)*/
    sql_code_without_comments = re.sub('/\*.*?\*/', '', input_str)

    # splits the string into commands
    commands_raw = re.split('(CREATE|INSERT|USE|DROP|DELETE|UPDATE)',
                            sql_code_without_comments, flags=re.IGNORECASE)

    # removes whitespaces
    commands_raw = [command_raw.strip()
                    for command_raw in commands_raw if command_raw.strip()]

    # putting the sql command keyword and the command itself into one element of the list
    for i in range(len(commands_raw)):
        if commands_raw[i].upper() in ['CREATE', 'INSERT', 'USE', 'DROP', 'DELETE', 'UPDATE']:
            commands_raw[i] += ' ' + commands_raw[i + 1]
            commands_raw[i + 1] = ''

    # removing any remaining whitespaces
    commands_in_sql = [command_raw.strip()
                       for command_raw in commands_raw if command_raw.strip()]

    # for every single sql command executes the parse function, and returns the full
    # list of commands for the server
    commands = []
    for command_in_sql in commands_in_sql:
        command = parse(command_in_sql)
        commands.append(command)

    return commands


input = """
update marks set Mark = 6 where StudID = 50 and DiscID = 'OS';
"""

asd = handle_my_sql_input(input)
for i in asd:
    print(i, end='\n')
