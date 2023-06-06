from backend.select_helper import *
from backend.column_finder import *
from backend.commands_helper import *
from backend.datagetter import *

def perform_nested_join(db_name, join_clause, mongoclient, xml_root, join_performed, starting_data=None):
    print("SIMPLE NESTED LOOP JOIN ALGORITHM IS TO BE PERFORMED")
    
    if starting_data is None or not join_performed:
        retVal, data = load_table_data(db_name, join_clause[0]['lhs']['table_name'].upper(), mongoclient, xml_root)
        if retVal < 0:
            return retVal, data
        starting_join_index = 0
    else:
        data = starting_data
        starting_join_index = 1

    for join in join_clause[starting_join_index:]:
        retVal, rhs_data = load_table_data(db_name, join['rhs']['table_name'].upper(), mongoclient, xml_root)
        if retVal < 0:
            return retVal, rhs_data

        new_data = {}

        for lhs_id, lhs_row in data.items():
            lhs_column_name = join['lhs']['column_name'].upper()
            lhs_value = lhs_row['value'].get(lhs_column_name, lhs_row['pk'].get(lhs_column_name))

            for rhs_id, rhs_row in rhs_data.items():
                rhs_column_name = join['rhs']['column_name'].upper()
                rhs_value = rhs_row['value'].get(rhs_column_name, rhs_row['pk'].get(rhs_column_name))

                if lhs_value == rhs_value:
                    # Construct the new row data
                    new_row = {'pk': {**lhs_row['pk'], **rhs_row['pk']},
                               'value': {**lhs_row['value'], **rhs_row['value']}}

                    # If lhs_id is not string convert it to a string
                    if not isinstance(lhs_id, str):
                        lhs_id = str(lhs_id)
                    # If rhs_id is not string convert it to a string
                    if not isinstance(rhs_id, str):
                        rhs_id = str(rhs_id)
                    new_id = lhs_id + "#" + rhs_id
                    new_data[new_id] = new_row

        data = new_data

    return retVal, data

def perform_indexed_nested_loop_join(db_name, join_clause, mongo_client, xml_root, starting_data=None):
    data = {}
    retVal = 0

    join_performed = False

    if starting_data is None:
        # Load the data for the first table in the join clause
        retVal, outer_table_data = load_table_data(db_name, join_clause[0]['lhs']['table_name'].upper(), mongo_client, xml_root)
        if retVal < 0:
            return retVal, outer_table_data, join_performed
        starting_join_index = 0
    else:
        outer_table_data = starting_data
        starting_join_index = 1

    for join in join_clause[starting_join_index:]:
        rhs_table_name = join['rhs']['table_name'].upper()

        inner_table_index_columns = get_index_columns(db_name, rhs_table_name, xml_root)

        # Use the join condition to select the appropriate index
        join_column = join['rhs']['column_name'].upper()
        index_columns = [index for index in inner_table_index_columns if join_column in index]

        if not index_columns:
            return 1, outer_table_data, join_performed
        
        print("INDEXED NESTED LOOP JOIN ALGORITHM IS TO BE PERFORMED")
        join_performed = True

        index_column = index_columns[0]  # choose the first suitable index

        # Load the index data
        index_name = get_index_name(db_name, rhs_table_name, index_column, xml_root, mongo_client)
        retVal, index_data = load_index_data(db_name, index_name, mongo_client)
        if retVal < 0:
            return retVal, index_data, join_performed

        # Perform the Indexed Nested Loop Join
        new_data = {}
        for outer_id, outer_row in outer_table_data.items():
            outer_column_value = outer_row['value'].get(join['lhs']['column_name'].upper(), outer_row['pk'].get(join['lhs']['column_name'].upper()))

            try:
                outer_column_value = int(outer_column_value)
            except ValueError:
                pass

            if value_exists_in_index_data(outer_column_value, index_data):
                inner_pks = index_data[outer_column_value]  # this could be a list if there are multiple matches
                
                retVal, matching_rows = load_table_data_from_index(db_name, rhs_table_name, {outer_column_value: inner_pks}, mongo_client, xml_root)
                if retVal < 0:
                    return retVal, matching_rows, join_performed

                for inner_id, inner_row in matching_rows.items():
                    new_row = {'pk': {**outer_row['pk'], **inner_row['pk']},
                               'value': {**outer_row['value'], **inner_row['value']}}
                    print(f"new_row: {new_row}")

                    outer_id = str(outer_id)
                    inner_id = str(inner_id)
                    
                    new_id = outer_id + "#" + inner_id
                    new_data[new_id] = new_row

        outer_table_data = new_data

    return retVal, outer_table_data, join_performed