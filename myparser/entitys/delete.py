import re


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
    "<>": "$ne",
    "like": "$regex",
    "not": "$ne"
}


def parse_build_conditions(partials, logical_operator):
    conditions = {}
    if logical_operator != None:
        conditions = {logical_operator: []}
    
    condition_pattern = r'^\s*(not\s+)?(\w+)\s+(=|>|>=|<|<=|<>|like)\s+(.+?)\s*$'
    
    for condition_str in partials:
        condition_match = re.match(
            condition_pattern, condition_str, flags=re.IGNORECASE | re.DOTALL)
        if condition_match != None:
            column = condition_match.group(2).upper()
            condition = {}
            if condition_match.group(1) != None:
                condition = {column: {
                    '$ne': {OPERATORS[condition_match.group(3)]: condition_match.group(4)}}}
            else:
                condition = {
                    column: {OPERATORS[condition_match.group(3)]: condition_match.group(4)}}

            if logical_operator != None:
                conditions[logical_operator].append(condition)
            else: 
                conditions = condition
        else:
            return conditions, -1

    return conditions, 0


def parse_filter_conditions_for_delete(condition_str):
    if condition_str == None:
        return [], -1

    partials = re.split(r'\s*and\s*', condition_str, flags=re.IGNORECASE)
    if (len(partials) > 1):
        return parse_build_conditions(partials, '$and')
    else: 
        return parse_build_conditions(partials, None)

    return [], -1


# parse handling of the "delete" command, returned CODE: 8
def parse_handle_delete(syntax_in_sql):
    delete_pattern = r'^delete\s+from\s+(\w+)\s+where\s+([^;.,]*|.*)\s*(?:;)?$'
    match = re.match(delete_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_deleting()
    else:
        table_name = match.group(1)
        condition_str = match.group(2)

        condition, status_code = parse_filter_conditions_for_delete(
            condition_str)
        if status_code < 0:
            return parse_handle_invalid_syntax_for_deleting()

        return {
            'code': 8,
            'type': 'delete',
            'table_name': table_name,
            'condition': condition
        }
