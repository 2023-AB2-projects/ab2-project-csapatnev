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


def parse_filter_conditions_for_update(condition_str):
    if condition_str == None:
        return [], -1

    partials = re.split(r'\s*and\s*', condition_str, flags=re.IGNORECASE)
    if (len(partials) > 1):
        return parse_build_conditions(partials, '$and')
    else: 
        return parse_build_conditions(partials, None)

    return [], -1


def parse_handle_update_set_modifications(modification_str):
    modification_pattern = r'^\s*(\w+)\s*=\s*(.*)\s*$'
    modification_match = re.match(
        modification_pattern, modification_str, re.IGNORECASE)

    if modification_match != None:
        column_name = modification_match.group(1)
        value = modification_match.group(2)
        return (column_name, value), 0
    else:
        return (None, None), -1


def parse_handle_update_set_clause(set_clause_str: str):
    modifications = []
    for modification_str in set_clause_str.split(','):
        modification, status_code = parse_handle_update_set_modifications(modification_str)
        if status_code < 0:
            return [], -1
        
        modifications.append(modification)

    return modifications, 0

def parse_handle_update(syntax_in_sql):
    update_pattern = r'^update\s+(\w+)\s+set\s+(.+?)\s+where\s+([^;.,]*|.*)\s*(?:;)?$'
    match = re.match(update_pattern, syntax_in_sql,
                     flags=re.IGNORECASE | re.DOTALL)

    if match is None:
        return parse_handle_invalid_syntax_for_updating()

    table_name = match.group(1)

    modification_str = match.group(2)
    modification, status_code = parse_handle_update_set_clause(
        modification_str)
    if status_code < 0:
        return parse_handle_invalid_syntax_for_updating()

    condition_str = match.group(3)
    condition, status_code = parse_filter_conditions_for_update(condition_str)  
    if status_code < 0:
        return parse_handle_invalid_syntax_for_updating()
    
    return {
        'code': 9,
        'type': 'update',
        'table_name': table_name,
        'modification': modification,
        'condition': condition
    }