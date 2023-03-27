# return (code) values:
#   0 - USE
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE
#   6 - INSERT
#   7 - DELETE

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


def parse_handle_condition(condition_str):
    condition_dict = {}
    logical_op = None

    # Mapping SQL operators to MongoDB operators
    operator_mapping = {
        '>': '$gt',
        '<': '$lt',
        '>=': '$gte',
        '<=': '$lte',
        '=': '$eq',
        '!=': '$ne',
    }

    # If condition is present, extract clauses and logical operator
    if condition_str:
        clauses = re.findall(
            r"(\w+)\s*(=|!=|<=|>=|<|>)\s*('[^']*'|[^\s]+)", condition_str)
        clauses = [(clause[0], operator_mapping[clause[1]],
                    clause[2].strip("'")) for clause in clauses]
        logical_ops = re.findall(
            r"\b(AND|OR)\b", condition_str, flags=re.IGNORECASE)

        # Handle logical operator
        if logical_ops:
            logical_op = logical_ops[0].upper()

        # Create filter_conditions dictionary
        if logical_op == 'AND':
            for column, operator, value in clauses:
                condition_dict[column] = {operator: value}
        elif logical_op == 'OR':
            condition_dict['$or'] = [{column: {operator: value}}
                                     for column, operator, value in clauses]
        elif len(clauses) == 1:
            column, operator, value = clauses[0]
            condition_dict[column] = {operator: value}
        else:
            raise ValueError("Invalid input or unsupported condition format.")

    print(condition_dict)


def parse_handle_create_database(syntax_in_sql):
    create_database_pattern = r'^create\s+database\s+(\w+)\s*;?$'
    match = re.match(create_database_pattern, syntax_in_sql, re.IGNORECASE)

    if match in None:
        return parse_handle_invalid_syntax_for_creating_database()
    else:
        database_name = match.group(1)
        return {
            'code': 1,
            'type': 'create',
            'object_type': 'database',
            'database_name': database_name
        }


def parse_handle_create_table(syntax_in_sql, data_types):
    create_table_pattern = r'^create\s+table\s+(\w+)\s*\((.*)\)\s*;?$'
    match = re.match(create_table_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_table()
    else:
        table_name = match.group(1)
        column_definitions_str = match.group(2)
        column_definitions = []

        for column_definition_str in column_definitions_str.split(','):
            column_definition_match = re.match(
                r'\s*(\w+)\s+(\w+)\s*', column_definition_str)
            if column_definition_match is None:
                return parse_handle_invalid_syntax_for_creating_table()
            else:
                column_name = column_definition_match.group(1)
                data_type = column_definition_match.group(2)
                column_definitions.append((column_name, data_type))
        return {
            'code': 2,
            'type': 'create',
            'object_type': 'table',
            'table_name': table_name,
            'column_definitions': column_definitions
        }


def parse_handle_create_index(syntax_in_sql):
    create_index_pattern = r'^create\s+index\s+(\w+)\s+on\s+(\w+)\s*\((\w+)\)\s*;?$'
    match = re.match(create_index_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_index()
    else:
        index_name = match.group(1)
        table_name = match.group(2)
        column_name = match.group(3)
        return {
            'code': 3,
            'type': 'create',
            'object_name': 'index',
            'index_name': index_name,
            'table_name': table_name,
            'column_name': column_name
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
    match = re.match(use_pattern, syntax_in_sql, re.IGNORECASE)

    database_name = match.group(1)
    return {
        'code': 0,
        'type': 'use',
        database_name: database_name
    }


def parse_handle_insert(syntax_in_sql):
    insert_pattern = r'^insert\s+into\s+(\w+)\s*\((.*)\)\s*values\s*\((.*)\)\s*;?$'
    match = re.match(insert_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_inserting()
    else:
        table_name = match.group(1)

        columns = []
        columns_str = match.group(2)
        for columns_str_splited in columns_str.split(','):
            column_match = re.match(r'\s*(\w+)\s*', columns_str_splited)
            if column_match is None:
                return parse_handle_invalid_syntax_for_inserting()
            else:
                column = column_match.group(1)
                columns.append(column)

        values = []
        values_str = match.group(3)
        for values_str_splited in values_str.split(','):
            value_match = re.match(r'\s*(\w+)\s*', values_str_splited)
            if value_match is None:
                return parse_handle_invalid_syntax_for_inserting()
            else:
                value = value_match.group(1)
                values.append(value)

        if len(values) != len(columns):
            return parse_handle_invalid_syntax_for_inserting()

        return {
            'code': 6,
            'type': 'insert',
            'table_name': table_name,
            'columns': columns,
            'values': values
        }


def parse_handle_delete(syntax_in_sql):
    delete_pattern = r'^delete\s+from\s+(\w+)\s+where\s+(.*)\s*;?$'
    match = re.match(delete_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_deleting()
    else:
        table_name = match.group(1)
        condition_str = match.group(2)

        condition = parse_handle_condition(condition_str)
        return {
            'code': 7,
            'type': 'delete',
            'table_name': table_name,
            'condition': condition
        }


def parse(syntax_in_sql: str):
    data_types = ['int', 'float', 'bit', 'date', 'datetime']

    syntax_in_sql_splited = re.findall(r'\w+|[^\w\s]', syntax_in_sql.upper())

    print(syntax_in_sql_splited, sep=':')

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
            return parse_handle_create_table(syntax_in_sql, data_types)
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


# syntax = "DELETE FROM Customers WHERE age < 21 or score > 100 or score = 100 and anyad = 'te';"
# syntax_split = parse(syntax)
# print(syntax_split)
