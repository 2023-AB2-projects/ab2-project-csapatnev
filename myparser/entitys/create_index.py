import re


def parse_handle_invalid_syntax_for_creating_index():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating index!'
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
    