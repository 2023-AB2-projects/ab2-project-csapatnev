import re


def parse_handle_invalid_syntax_for_use():
    return {
        'code': -1,
        'message': 'Invalid syntax for use!'
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
