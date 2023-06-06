import re


def parse_handle_invalid_syntax_for_creating_table():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating a table.'
    }


# getting the composite primary keys if there are any
def parse_handle_create_table_composite_primary_keys(columns_definitions_str):
    composite_primary_keys = []
    composite_pk_pattern = r'^(.*)\s+primary\s+key\s*\(\s*(.*)\s*\)\s*$'
    composite_pk_match = re.match(
        composite_pk_pattern, columns_definitions_str, flags=re.IGNORECASE | re.DOTALL)

    if composite_pk_match != None:
        primary_keys_str = composite_pk_match.group(2)

        for primary_key in primary_keys_str.split(','):
            composite_primary_keys.append(primary_key.strip())

        columns_definitions_str = composite_pk_match.group(
            1).rstrip().rstrip(',')
    return composite_primary_keys, columns_definitions_str


# getting the column definitions, references and the primary key from the create table command
def parse_handle_create_table_col_ref_pk_uq(columns_definitions_str):
    column_definitions = []
    primary_keys = []
    references = []
    unique = []

    for column_definition_str in columns_definitions_str.split(','):
        column_definition_pattern = r'^\s*(\w+)\s+([\w\(\)0-9]+)\s*(?:(.*)\s*)?$'
        column_definition_match = re.match(
            column_definition_pattern, column_definition_str.strip(), re.IGNORECASE)

        if column_definition_match != None:
            data_type_pattern = r'^\s*(int|varchar)(?:\(.*\))\s*$'
            data_type_match = re.match(
                data_type_pattern, column_definition_match.group(2), re.IGNORECASE)

            if data_type_match is None:
                data_type_pattern = r'^\s*(int|varchar|bit|date|datetime|float)\s*$'
                data_type_match = re.match(
                    data_type_pattern, column_definition_match.group(2), re.IGNORECASE)
                if data_type_match is None:
                    return column_definitions, primary_keys, references, unique, -1

            column_name = column_definition_match.group(1)
            data_type = data_type_match.group(1)
            column_definitions.append((column_name, data_type))

            pk_pattern = r'^\s*primary\s+key\s*$'
            match_pk = re.match(
                pk_pattern, column_definition_match.group(3), re.IGNORECASE)

            ref_pattern = r'^\s*references\s+(\w+)\s*\((\w+)\)\s*$'
            match_ref = re.match(
                ref_pattern, column_definition_match.group(3), re.IGNORECASE)

            uq_pattern = r'^\s*unique\s*$'
            match_uq = re.match(
                uq_pattern, column_definition_match.group(3), re.IGNORECASE)

            w_pattern = r'^\s*$'
            match_w = re.match(
                w_pattern, column_definition_match.group(3), re.IGNORECASE)

            if match_pk != None:
                if len(primary_keys) >= 1:
                    return column_definitions, primary_keys, references, unique, -1
                else:
                    primary_keys.append(column_name)

            if match_ref != None:
                ref_table_name = match_ref.group(1)
                ref_table_name = match_ref.group(1)
                ref_column_name = match_ref.group(2)
                references.append(
                    (column_name, ref_table_name, ref_column_name))

            if match_uq != None:
                unique.append(column_name)

            if match_ref == None and match_pk == None and match_w == None and match_uq == None:
                return column_definitions, primary_keys, references, unique, -1
        else:
            return column_definitions, primary_keys, references, unique, -1

    return column_definitions, primary_keys, references, unique, 0


# parse handling of the "create table" command, returned CODE: 2
def parse_handle_create_table(syntax_in_sql):
    create_table_pattern = r'^create\s+table\s+(\w+)\s*\(\s*(.*)\s*\)\s*;?$'
    match = re.match(create_table_pattern, syntax_in_sql,
                     flags=re.IGNORECASE | re.DOTALL)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_table()
    else:
        table_name = match.group(1)
        columns_definitions_str = match.group(2)

        composite_primary_keys, columns_definitions_str = parse_handle_create_table_composite_primary_keys(
            columns_definitions_str)

        column_definitions, primary_keys, references, unique, status_code = parse_handle_create_table_col_ref_pk_uq(
            columns_definitions_str)
        if status_code < 0:
            return parse_handle_invalid_syntax_for_creating_table()

        if len(primary_keys) != 0 and len(composite_primary_keys) != 0:
            return parse_handle_invalid_syntax_for_creating_table()

        for column_name in composite_primary_keys:
            if column_name not in [column for (column, _) in column_definitions]:
                return parse_handle_invalid_syntax_for_creating_table()

        if len(composite_primary_keys) != 0:
            return {
                'code': 2,
                'type': 'create',
                'object_type': 'table',
                'table_name': table_name,
                'column_definitions': column_definitions,
                'primary_keys': composite_primary_keys,
                'references': references,
                'unique': unique
            }

        return {
            'code': 2,
            'type': 'create',
            'object_type': 'table',
            'table_name': table_name,
            'column_definitions': column_definitions,
            'primary_keys': primary_keys,
            'references': references,
            'unique': unique
        }