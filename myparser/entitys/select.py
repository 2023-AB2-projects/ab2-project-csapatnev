import re


def parse_handle_invalid_syntax_for_selecting():
    return {
        'code': -1,
        'message': 'Invalid syntax for selecting!'
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


def parse_handle_select_from_clause(from_clause_str):
    from_clause = []
    table_name_pattern = r'^\s*\w+\s*$'
    for table_name in from_clause_str.split(','):
        table_match = re.match(
            table_name_pattern, table_name, flags=re.IGNORECASE)

        if table_match != None:
            from_clause.append(table_name)
        else:
            return from_clause, -1
    return from_clause, 0


def parse_handle_select_select_clause(select_clause_str):
    columns_to_select = []
    for column_to_select in select_clause_str.split(','):
        function_simple_column_pattern = r'^\s*(\w+)\(\s*(\*|\w+)\s*\)(?:\s+as\s+(\w+)\s*)?\s*$'
        function_simple_match = re.match(
            function_simple_column_pattern, column_to_select, flags=re.IGNORECASE)
        if function_simple_match != None:
            functions = ['avg', 'count', 'min', 'max', 'sum']
            if function_simple_match.group(1) not in functions:
                return column_to_select, -1

            columns_to_select.append({
                'table_name': 'unknown',
                'column_name': function_simple_match.group(2),
                'alias': function_simple_match.group(3),
                'function': function_simple_match.group(1)
            })
            continue

        function_composite_column_pattern = r'^\s*(\w+)\(\s*(\w+)\.(\w+)\s*\)(?:\s+as\s+(\w+)\s*)?\s*$'
        function_composite_match = re.match(
            function_composite_column_pattern, column_to_select, flags=re.IGNORECASE)
        if function_composite_match != None:
            functions = ['avg', 'count', 'min', 'max', 'sum']
            if function_composite_match.group(1) not in functions:
                return column_to_select, -1

            columns_to_select.append({
                'table_name': function_composite_match.group(2),
                'column_name': function_composite_match.group(3),
                'alias': function_composite_match.group(4),
                'function': function_composite_match.group(1)
            })
            continue

        simple_column_pattern = r'^\s*(\*|\w+)(?:\s+as\s+(\w+)\s*)?\s*$'
        simple_match = re.match(simple_column_pattern,
                                column_to_select, flags=re.IGNORECASE)
        if simple_match != None:
            columns_to_select.append({
                'table_name': 'unknown',
                'column_name': simple_match.group(1),
                'alias': simple_match.group(2),
                'function': None
            })
            continue

        composite_column_pattern = r'^\s*(\w+)\.(\w+)(?:\s+as\s+(\w+)\s*)?\s*$'
        composite_match = re.match(composite_column_pattern,
                                   column_to_select, flags=re.IGNORECASE)
        if composite_match != None:
            columns_to_select.append({
                'table_name': composite_match.group(1),
                'column_name': composite_match.group(2),
                'alias': composite_match.group(3),
                'function': None
            })
            continue

        return columns_to_select, -1

    return columns_to_select, 0


def parse_join_condition(condition_str):
    lhs = {}
    rhs = {}

    condition_pattern = r'^\s*(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*$'
    
    condition_match = re.match(condition_pattern, condition_str, flags=re.IGNORECASE)
    if condition_match is None:
        return {}, {}, -1
    
    lhs = {
        'table_name': condition_match.group(1),
        'column_name': condition_match.group(2)
    }

    rhs = {
        'table_name': condition_match.group(3),
        'column_name': condition_match.group(4)
    }

    return lhs, rhs, 0


def parse_join_clause(join_str):
    join_item = {}
    
    join_pattern = r'^\s*join\s+(\w+)\s+on\s+(.+?)\s*$'
    
    join_match = re.match(join_pattern, join_str, flags=re.IGNORECASE)
    if join_match is None:
        return {}, -1
    
    table = join_match.group(1)
    condition_str = join_match.group(2)
    
    lhs, rhs, status_code = parse_join_condition(condition_str)
    if status_code < 0:
        return {}, -1

    return {'table': table, 'lhs': lhs, 'rhs': rhs}, 0   


def parse_handle_select_join_clause(join_clause_str):
    join_clause = []

    join_clauses = re.findall(r'join\s+.+?(?=\s+join|$)', join_clause_str, flags=re.IGNORECASE | re.DOTALL)
    
    for join_str in join_clauses:
        join_item, status_code = parse_join_clause(join_str)
        if status_code < 0:
            return [], -1
        
        join_clause.append(join_item)

    return join_clause, 0


def parse_where_condition_lhs(lhs_str):
    simple_pattern = r'^\s*(\w+)\s*$'
    simple_match = re.match(simple_pattern, lhs_str, flags=re.IGNORECASE)
    if simple_match != None:
        return {
            'table_name': 'unknown',
            'column_name': simple_match.group(1)
        }, 0

    composite_pattern = r'^\s*(\w+)\.(\w+)\s*$'
    composite_match = re.match(composite_pattern, lhs_str, flags=re.IGNORECASE)
    if composite_match != None:
        return {
            'table_name': composite_match.group(1),
            'column_name': composite_match.group(2)
        }, 0

    return {}, -1


def parse_where_condition_rhs(rhs_str):
    number_pattern = r'^\s*([0-9]+)\s*$'
    number_match = re.match(number_pattern, rhs_str, flags=re.IGNORECASE)
    if number_match != None:
        return {
            'type': 'number',
            'value': number_match.group(1)
        }, 0
    
    string_pattern = r'^\s*\'(.*?)\'\s*$'
    string_match = re.match(string_pattern, rhs_str, flags=re.IGNORECASE)
    if string_match != None:
        return {
            'type': 'string',
            'value': string_match.group(1)
        }, 0
    
    simple_pattern = r'^\s*(\w+)\s*$'
    simple_match = re.match(simple_pattern, rhs_str, flags=re.IGNORECASE)
    if simple_match != None:
        return {
            'type': 'column',
            'table_name': 'unknown',
            'column_name': simple_match.group(1)
        }, 0

    composite_pattern = r'^\s*(\w+)\.(\w+)\s*$'
    composite_match = re.match(composite_pattern, rhs_str, flags=re.IGNORECASE)
    if composite_match != None:
        return {
            'type': 'column',
            'table_name': composite_match.group(1),
            'column_name': composite_match.group(2)
        }, 0

    return {}, -1


def parse_where_condition(where_condition_str):
    condition_pattern = r'^\s*(not\s+)?(.+?)\s*(<=|>=|<>|=|<|>|like)\s*(.+?)\s*$'

    condition_match = re.match(condition_pattern, where_condition_str, flags=re.IGNORECASE)
    if condition_match is None:
        return {}, -1
    
    negation = False
    if condition_match.group(1) != None:
        negation = True

    operator = OPERATORS[condition_match.group(3)]

    lhs, status_code = parse_where_condition_lhs(condition_match.group(2))
    if status_code < 0:
        return {}, -1

    rhs, status_code = parse_where_condition_rhs(condition_match.group(4))
    if status_code < 0:
        return {}, -1

    return {'negation': negation, 'operator': operator, 'lhs': lhs, 'rhs': rhs}, 0


def parse_handle_select_where_clause(where_clause_str):
    if where_clause_str is None:
        return [], 0
    
    where_clause = []

    where_pattern = r'^\s*where\s+(.+?)$'

    where_match = re.match(where_pattern, where_clause_str, flags=re.IGNORECASE | re.DOTALL)
    if where_match is None:
        return {}, -1
    
    conditions = re.split(r'\s+and\s+', where_match.group(1), flags=re.IGNORECASE)
    for where_condition_str in conditions:
        condition_item, status_code = parse_where_condition(where_condition_str)
        if status_code < 0:
            return {}, -1
        
        where_clause.append(condition_item)

    return where_clause, 0


def parse_handle_select_groupby_clause(groupby_clause_str):
    if groupby_clause_str is None:
        return [], 0
    
    columns_to_groupby = []

    if groupby_clause_str == None:
        return columns_to_groupby, -1

    group_pattern = r'^\s*group\s+by\s+(.+?)\s*$'
    
    group_match = re.match(group_pattern, groupby_clause_str, flags=re.IGNORECASE)
    if group_match is None:
        return [], -1

    for column_to_groupby in group_match.group(1).split(','):
        groupby_simple_pattern = r'^\s*(\w+)\s*$'
        groupby_simple_match = re.match(
            groupby_simple_pattern, column_to_groupby, flags=re.IGNORECASE)
        if groupby_simple_match != None:
            columns_to_groupby.append({
                'table_name': 'unknown',
                'column_name': groupby_simple_match.group(1)
            })
            continue

        groupby_composite_pattern = r'^\s*(\w+)\.(\w+)\s*$'
        groupby_composite_match = re.match(
            groupby_composite_pattern, column_to_groupby, flags=re.IGNORECASE)
        if groupby_composite_match != None:
            columns_to_groupby.append({
                'table_name': groupby_composite_match.group(1),
                'column_name': groupby_composite_match.group(2)
            })
            continue

        return columns_to_groupby, -1

    return columns_to_groupby, 0


def parse_handle_select(syntax_in_sql):
    select_pattern = r'^\s*select(\s+distinct)?\s+(.+?)\s+from\s+(\w+)((\s+join\s+\w+\s+on\s+.+?)*?)(\s+where\s+.+?)?(\s+group\s+by\s+.+?)?\s*;?\s*$'
    match = re.match(select_pattern, syntax_in_sql,
                     flags=re.IGNORECASE | re.DOTALL)

    if match is None:
        # print('1')
        return parse_handle_invalid_syntax_for_selecting()

    from_clause = match.group(3)
    # from_clause, status_code = parse_handle_select_from_clause(
    #     from_clause_str)
    # if status_code < 0:
    #     print('2')
    #     return parse_handle_invalid_syntax_for_selecting()

    select_clause_str = match.group(2)
    select_clause, status_code = parse_handle_select_select_clause(
        select_clause_str)
    if status_code < 0:
        # print('3')
        return parse_handle_invalid_syntax_for_selecting()

    select_distinct = False
    if match.group(1) != None:
        select_distinct = True

    join_clause_str = match.group(4)
    join_clause, status_code = parse_handle_select_join_clause(join_clause_str)
    if status_code < 0:
        # print('4')
        return parse_handle_invalid_syntax_for_selecting()

    where_clause_str = match.group(6)
    where_clause, status_code = parse_handle_select_where_clause(where_clause_str)
    if status_code < 0:
        # print('5')
        return parse_handle_invalid_syntax_for_selecting()

    groupby_clause_str = match.group(7)
    groupby_clause, status_code = parse_handle_select_groupby_clause(
        groupby_clause_str)
    if status_code < 0:
        # print('4')
        return parse_handle_invalid_syntax_for_selecting()

    return {
        'code': 10,
        'type': 'select',
        'select_clause': select_clause,
        'select_distinct': select_distinct,
        'from_clause': from_clause,
        'join_clause': join_clause,
        'where_clause': where_clause,
        'groupby_clause': groupby_clause
    }