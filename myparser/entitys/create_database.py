import re


def parse_handle_invalid_syntax_for_creating_database():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating database!'
    }


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
