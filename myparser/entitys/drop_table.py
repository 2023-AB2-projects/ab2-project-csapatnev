import re


def parse_handle_invalid_syntax_for_dropping_table():
    return {
        'code': -1,
        'message': 'Invalid syntax for dropping table!'
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
