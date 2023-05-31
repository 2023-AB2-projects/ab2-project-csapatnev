from lxml import *
from lxml import etree
import backend.mongoHandler as mongoHandler
from backend.datagetter import *
import re
from backend.commands_helper import *
from backend.column_finder import *
from backend.select_helper import *

def distinctify(data):
    seen = {}
    for key, value in data.items():
        pk_data = tuple(value['pk'].items())
        if pk_data not in seen:
            seen[pk_data] = value
    return seen

def apply_condition(data, condition):
    operator = condition['operator']
    lhs = condition['lhs']
    rhs = condition['rhs']

    new_data = {}

    if rhs['type'] == 'column':
        lhs_column_name = lhs['column_name'].upper()
        rhs_column_name = rhs['column_name'].upper()
        
        for _id, row in data.items():
            column_value_lhs = row['pk'][lhs_column_name] if lhs_column_name in row['pk'] else row['value'][lhs_column_name]
            column_value_rhs = row['pk'][rhs_column_name] if rhs_column_name in row['pk'] else row['value'][rhs_column_name]
            if compare_values(operator, column_value_lhs, column_value_rhs):
                new_data[_id] = row
    else:
        lhs_column_name = lhs['column_name'].upper()
        
        for _id, row in data.items():
            column_value = row['pk'][lhs_column_name] if lhs_column_name in row['pk'] else row['value'][lhs_column_name]
            comparison_value = rhs['value']
            if compare_values(operator, column_value, comparison_value):
                new_data[_id] = row

    if new_data == {}:
        # return the first row of the original data, but with the values set to none for each column
        dummydata = list(data.items())
        new_data = {
            0: {
                'pk': {column_name: None for column_name in dummydata[0][1]['pk'].keys()},
                'value': {column_name: None for column_name in dummydata[0][1]['value'].keys()}
            }
        }
    return new_data


def compare_values(operator, value1, value2):
    if isinstance(value1, str):
        value1 = value1.strip('\'"')
    if isinstance(value2, str):
        value2 = value2.strip('\'"')
   
    if operator == "$eq":
        return value1 == value2
    elif operator == "$gt":
        return value1 < value2
    elif operator == "$gte":
        return value1 <= value2
    elif operator == "$lt":
        return value1 > value2
    elif operator == "$lte":
        return value1 >= value2
    elif operator == "$ne":
        return value1 != value2
    elif operator == "$regex":
        return re.search(value2, value1)
    else:
        return False

def apply_where_clause(data, where_clause):
    for condition in where_clause:
        data = apply_condition(data, condition)
    return data

def filter_columns(data, select_clause):
    # Prepare a list of column names from the select_clause
    column_names = {col['column_name'].upper(): (col.get('alias') or col['column_name']).upper() for col in select_clause}

    for _id, row in data.items():
        new_pk = {}
        new_value = {}

        for original_name, alias_name in column_names.items():
            if original_name in row['pk']:
                new_pk[alias_name] = row['pk'][original_name]
            if original_name in row['value']:
                new_value[alias_name] = row['value'][original_name]
        
        row['pk'] = new_pk
        row['value'] = new_value

    return data

def get_index_columns(db_name, table_name, xml_root):
    index_columns = []
    indexes = xml_root.findall(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/IndexFiles/IndexFile")
    for index in indexes:
        index_cols = [iattrib.text.upper() for iattrib in index.findall('IAttribute')]
        index_columns.append(index_cols)

    uindexes = xml_root.findall(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/uniqueKeys/uniqueKey")
    for uindex in uindexes:
        uindex_cols = uindex.text.upper()
        index_columns.append([uindex_cols])

    findexes = xml_root.findall(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/foreignKeys/foreignKey")
    for findex in findexes:
        findex_cols = findex.text.upper()
        index_columns.append([findex_cols])

    return index_columns

def get_index_name(db_name, table_name, index_columns, xml_root, mongo_client):
    # Define the general formats of the index file names
    formats = ['{table}_{column}_UNIQUE_INDEX', '{table}_{column}_FOREIGN_INDEX']

    for index_column in index_columns:
        # Check for user-defined index files first
        index_file_elem = xml_root.find(f"./Database[@name='{db_name}']/Tables/Table[@name='{table_name}']/IndexFiles/IndexFile[IAttribute='{index_column}']")
        if index_file_elem is not None:
            return index_file_elem.get('indexFileName')

        # Then check for automatically generated index files
        for fmt in formats:
            index_name = fmt.format(table=table_name, column=index_column)
            db = mongo_client[db_name]
            if index_name in db.list_collection_names():
                return index_name
    return None


def value_exists_in_index_data(outer_column_value, index_data):
    for value in index_data.values():
        split_value = value.split('#')
        if outer_column_value in split_value:
            return True
        for val in split_value:
            try:
                val = int(val)
            except ValueError:
                pass
            if outer_column_value == val:
                return True
    return False

def simplify_result(dbms_result):
    simplified_result = {
        k: {**v['pk'], **v['value']}
        for k, v in dbms_result.items()
    }

    # Remove duplicates
    simplified_result = list(simplified_result.values())

    return simplified_result

def apply_aggregate_function(data, select_clause, groupby_clause):
    is_grouped = isinstance(next(iter(data.keys())), tuple)

    results = []

    if is_grouped:
        for group_key, group_rows in data.items():
            result = {}
            if isinstance(group_key, tuple):
                for i, col_name in enumerate(groupby_clause):
                    result[col_name] = group_key[i]
            else:
                result[groupby_clause[0]] = group_key

            for select_item in select_clause:
                function = select_item.get('function')
                column_name = select_item['column_name'].upper()
                if function:
                    column_values = group_rows['pk'].get(column_name, []) + group_rows['value'].get(column_name, [])
                    # Apply the aggregate function
                    result[select_item.get('alias', column_name)] = apply_function(function, column_values)
            results.append(result)
    else:
        result = {}
        for select_item in select_clause:
            function = select_item.get('function')
            column_name = select_item['column_name'].upper()
            if function:
                column_values = []
                for _id, row in data.items():
                    if column_name in row['pk']:
                        try:
                            row['pk'][column_name] = int(row['pk'][column_name])
                        except ValueError:
                            pass
                        column_values.append(row['pk'][column_name])
                    if column_name in row['value']:
                        try:
                            row['value'][column_name] = int(row['value'][column_name])
                        except ValueError:
                            pass
                        column_values.append(row['value'][column_name])
                # Apply the aggregate function
                result[select_item.get('alias', column_name)] = apply_function(function, column_values)
        results.append(result)

    return 0, results

def apply_function(function, column_values):
    # Apply the aggregate function
    if function == 'avg':
        column_values = [int(value) for value in column_values]
        result = sum(column_values) / len(column_values) if column_values else None
    elif function == 'count':
        result = len(column_values)
    elif function == 'min':
        result = min(column_values) if column_values else None
    elif function == 'max':
        result = max(column_values) if column_values else None
    elif function == 'sum':
        column_values = [int(value) for value in column_values]
        result = sum(column_values) if column_values else None
    else:
        result = None  # ignore unsupported functions
    return result

def group_by(data, groupby_clause):
    grouped_data = {}
    tmp = []
    for d in groupby_clause:
        tmp.append(d['column_name'].upper())
    groupby_clause = tmp

    for _id, row in data.items():
        key = tuple(row['pk'].get(col, row['value'].get(col, None)) for col in groupby_clause)
        
        if key not in grouped_data:
            grouped_data[key] = {'pk': {}, 'value': {}}

        # Add grouped columns to 'pk'
        for col in groupby_clause:
            if col in row['pk']:
                if col not in grouped_data[key]['pk']:
                    grouped_data[key]['pk'][col] = []
                grouped_data[key]['pk'][col].append(row['pk'][col])
            elif col in row['value']:
                if col not in grouped_data[key]['value']:
                    grouped_data[key]['value'][col] = []
                grouped_data[key]['value'][col].append(row['value'][col])

        # Add non-grouped columns to 'value'
        for col in set(row['pk'].keys()).difference(groupby_clause):
            if col not in grouped_data[key]['pk']:
                grouped_data[key]['pk'][col] = []
            grouped_data[key]['pk'][col].append(row['pk'][col])
        for col in set(row['value'].keys()).difference(groupby_clause):
            if col not in grouped_data[key]['value']:
                grouped_data[key]['value'][col] = []
            grouped_data[key]['value'][col].append(row['value'][col])

    return grouped_data
