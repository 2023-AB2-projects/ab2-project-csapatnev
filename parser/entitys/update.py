import re


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
    "<>": "$ne",
    "like": "$regex",
    "not": "$ne"
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
    match = re.match(update_pattern, syntax_in_sql,
                     flags=re.IGNORECASE | re.DOTALL)

    if match is None:
        return parse_handle_invalid_syntax_for_updating()
    else:
        table_name = match.group(1)

        modification_str = match.group(2)
        modification, status_code = parse_handle_update_set_modifications(
            modification_str)
        if status_code < 0:
            return parse_handle_invalid_syntax_for_updating()

        condition_str = match.group(3)
        condition = parse_where_clause(condition_str)  # fixelni kell
        return {
            'code': 9,
            'type': 'update',
            'table_name': table_name,
            'modification': modification,
            'condition': condition
        }