import re


def parse_handle_invalid_syntax_for_inserting():
    return {
        'code': -1,
        'message': 'Invalid syntax for inserting!'
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