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
                'rename': function_simple_match.group(3),
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
                'rename': function_composite_match.group(4),
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
                'rename': simple_match.group(2),
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
                'rename': composite_match.group(3),
                'function': None
            })
            continue

        return columns_to_select, -1

    return columns_to_select, 0


def parse_handle_select_groupby_clause(groupby_clause_str):
    columns_to_groupby = []

    if groupby_clause_str == None:
        return columns_to_groupby, -1

    for column_to_groupby in groupby_clause_str.split(','):
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
    select_pattern = r'^\s*select(\s+distinct)?\s+(.+)\s+from\s+(.+?)\s+(:?join\s+(.+?))?(?:\s+where\s+(.+?)(?:\s+group\s+by\s+(.+?))?)?\s*(?:;)?$'
    match = re.match(select_pattern, syntax_in_sql,
                     flags=re.IGNORECASE | re.DOTALL)

    if match is None:
        print('1')
        return parse_handle_invalid_syntax_for_selecting()
    else:
        from_clause_str = match.group(3)
        from_clause, status_code = parse_handle_select_from_clause(
            from_clause_str)
        if status_code < 0:
            print('2')
            return parse_handle_invalid_syntax_for_selecting()

        select_clause_str = match.group(2)
        select_clause, status_code = parse_handle_select_select_clause(
            select_clause_str)
        if status_code < 0:
            print('3')
            return parse_handle_invalid_syntax_for_selecting()

        select_distinct = False
        if match.group(1) != None:
            select_distinct = True

        # where_clause_str = match.group(4)
        # where_clause, status_code = parse_handle_select_build_conditions(where_clause_str)
        # if status_code < 0:
        #     print('5')
        #     return parse_handle_invalid_syntax_for_selecting()

        # groupby_clause_str = match.group(5)
        # groupby_clause, status_code = parse_handle_select_groupby_clause(
        #     groupby_clause_str)
        # if status_code < 0:
        #     print('4')
        #     return parse_handle_invalid_syntax_for_selecting()

        # print(where_clause)

        return {
            'code': 10,
            'type': 'select',
            'select_clause': select_clause,
            'select_distinct': select_distinct,
            'from_clause': from_clause,
            'where_clause': "",
            'groupby_clause': 'groupby_clause'
        }