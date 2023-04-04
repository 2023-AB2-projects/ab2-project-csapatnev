# return (code) values:
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE
#   6 - USE
#   7 - INSERT
#   8 - DELETE

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


OPERATORS = {
    "=": "$eq",
    ">": "$gt",
    ">=": "$gte",
    "<": "$lt",
    "<=": "$lte",
    "<>": "$ne"
}


def parse_where_clause(where_clause):
    conditions = where_clause.split(' and ')
    top_level_operator = "$and"
    filter_conditions = {}
    if len(conditions) > 1:
        filter_conditions[top_level_operator] = []
        for condition in reversed(conditions):
            if ' or ' in condition:
                sub_conditions = condition.split(' or ')
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
    if ' or ' in condition:
        sub_conditions = condition.split(' or ')
        or_conditions = []
        for sub_condition in sub_conditions:
            or_conditions.append(parse_condition(sub_condition))
        return {"$or": or_conditions}
    elif 'not ' in condition:
        condition = condition.split('not ')
        column = condition[1].split()[0]
        column = column.upper()
        operator = condition[1].split()[1]
        value = condition[1].split()[2]
        return {column: {"$not": {OPERATORS[operator]: value}}}
    else:
        column = condition.split(' ')[0]
        column = column.upper()
        operator = condition.split(' ')[1]
        value = condition.split(' ')[2]
        return { column: { OPERATORS[operator]: value } }


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


def parse_handle_create_table_col_ref_pk(columns_definitions_str):
    column_definitions = []
    primary_keys = []
    references = []

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
                    return column_definitions, primary_keys, references, -1

            column_name = column_definition_match.group(1)
            data_type = data_type_match.group(1)
            column_definitions.append((column_name, data_type))

            pk_pattern = r'^\s*primary\s+key\s*$'
            match_pk = re.match(
                pk_pattern, column_definition_match.group(3), re.IGNORECASE)

            ref_pattern = r'^\s*references\s+(\w+)\s*\((\w+)\)\s*$'
            match_ref = re.match(
                ref_pattern, column_definition_match.group(3), re.IGNORECASE)

            w_pattern = r'^\s*$'
            match_w = re.match(
                w_pattern, column_definition_match.group(3), re.IGNORECASE)

            if match_pk != None:
                if len(primary_keys) > 1:
                    return column_definitions, primary_keys, references, -1
                else:
                    primary_keys.append(column_name)

            if match_ref != None:
                ref_table_name = match_ref.group(1)
                ref_table_name = match_ref.group(1)
                ref_column_name = match_ref.group(2)
                references.append(
                    (column_name, ref_table_name, ref_column_name))

            if match_ref == None and match_pk == None and match_w == None:
                return column_definitions, primary_keys, references, -1
        else:
            return column_definitions, primary_keys, references, -1

    return column_definitions, primary_keys, references, 0


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

        column_definitions, primary_keys, references, status_code = parse_handle_create_table_col_ref_pk(
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
                'references': references
            }

        return {
            'code': 2,
            'type': 'create',
            'object_type': 'table',
            'table_name': table_name,
            'column_definitions': column_definitions,
            'primary_keys': primary_keys,
            'references': references
        }


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


def parse_handle_delete(syntax_in_sql):
    delete_pattern = r'^delete\s+from\s+(\w+)\s+where\s+([^;.,]*|.*)\s*(?:;)?$'
    match = re.match(delete_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_deleting()
    else:
        table_name = match.group(1)
        condition_str = match.group(2)
        print(condition_str)

        condition = parse_where_clause(condition_str)
        return {
            'code': 8,
            'type': 'delete',
            'table_name': table_name,
            'condition': condition
        }


def parse(syntax_in_sql: str):
    syntax_in_sql_splited = re.findall(r'\w+|[^\w\s]', syntax_in_sql.upper())

    if len(syntax_in_sql_splited) == 0:
        return parse_handle_no_input()

    command_type = syntax_in_sql_splited[0]

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
    else:
        return parse_handle_invalid_sql_command()


def handle_my_sql_input(input_str: str):
    sql_code_without_comments = re.sub('/\*.*?\*/', '', input_str)

    commands_raw = re.split('(CREATE|INSERT|USE|DROP|DELETE)',
                            sql_code_without_comments, flags=re.IGNORECASE)

    commands_raw = [command_raw.strip()
                    for command_raw in commands_raw if command_raw.strip()]

    for i in range(len(commands_raw)):
        if commands_raw[i].upper() in ['CREATE', 'INSERT', 'USE', 'DROP', 'DELETE']:
            commands_raw[i] += ' ' + commands_raw[i + 1]
            commands_raw[i + 1] = ''

    commands_in_sql = [command_raw.strip()
                       for command_raw in commands_raw if command_raw.strip()]

    commands = []
    for command_in_sql in commands_in_sql:
        command = parse(command_in_sql)
        commands.append(command)

    return commands


input = """
DELETE FROM DISCIPLINES WHERE CreditNr = 2;
"""

asd = handle_my_sql_input(input)
for i in asd:
    print(i, end='\n')
