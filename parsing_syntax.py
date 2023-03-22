# return (code) values:
#   0 - USE
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE
#   6 - INSERT
#   7 - DELETE

import re


def parse_handle_invalid_object_type():
    return {
        'code': -1,
        'message': 'Invalid sql Object type!'
    }


def parse_handle_invalid_sql_command():
    return {
        'code': -1,
        'message': 'Invalid SQL command!'
    }


def parse_handle_invalid_syntax_for_creating_database():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating database!'
    }


def parse_handle_invalid_syntax_for_creating_table():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating a table.'
    }


def parse_handle_invalid_syntax_for_creating_index():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating index!'
    }


def parse_handle_no_input():
    return {
        'code': -1,
        'message': 'No sql command has been provided!'
    }


def parse_handle_invalid_syntax_for_inserting():
    return {
        'code': -1,
        'message': 'Invalid syntax for inserting!'
    }


def parse_handle_invalid_syntax_for_deleting():
    return {
        'code': -1,
        'message': 'Invalid syntax for deleting!'
    }


def parse_handle_condition(condition_to_parse):
    key_words = ['AND', 'OR', '=', '<=', '>=', '<', '>']
    condtions = []
    
    condition_parsed = re.split(key_words, condition_to_parse)
    print(condition_parsed)



def parse_handle_create_database(syntax_in_sql_splited):
    if len(syntax_in_sql_splited) == 3:
        database_name = syntax_in_sql_splited[2]
        return {
            'code': 1,
            'type': 'create',
            'object_type': 'database',
            'database_name': database_name 
        }
    else:
        return parse_handle_invalid_syntax_for_creating_database()


def parse_handle_create_table(syntax_in_sql_splited, data_types):
    if len(syntax_in_sql_splited) < 4:
        return parse_handle_invalid_syntax_for_creating_table()
                
    table_name = syntax_in_sql_splited[2]
    column_definitions = []
    
    error_occured = False
    i = 3
    while i < len(syntax_in_sql_splited):
        if syntax_in_sql_splited[i] == '(':
            i += 1
            while i + 2 <= len(syntax_in_sql_splited) and syntax_in_sql_splited[i] != ')':
                column_name = syntax_in_sql_splited[i]
                data_type = syntax_in_sql_splited[i + 1]
                
                if data_type not in data_types:
                    error_occured = True   
                    break
                
                column_definitions.append((column_name, data_type))
                if i + 2 == len(syntax_in_sql_splited):
                    error_occured = True
                    break
                elif syntax_in_sql_splited[i + 2] == ')':
                    break
                elif syntax_in_sql_splited[i + 2] == ',':
                    i += 3
                else:
                    error_occured = True
                    break

            if i + 1 == len(syntax_in_sql_splited):
                error_occured = True
                break
            
            break
        else:
            i += 1

    if i == len(syntax_in_sql_splited):
        error_occured = True
        
    if error_occured:
        return parse_handle_invalid_syntax_for_creating_table()
    else:
        return {
            'code': 2, 
            'type': 'create', 
            'object_type': 'table', 
            'table_name': table_name, 
            'column_definitions': column_definitions
        }


def parse_handle_create_index(syntax_in_sql_splited):
    if len(syntax_in_sql_splited) != 8:
        return parse_handle_invalid_syntax_for_creating_index()

    if syntax_in_sql_splited[3] != 'ON':
        return parse_handle_invalid_syntax_for_creating_index()

    if syntax_in_sql_splited[5] != '(' or syntax_in_sql_splited[7] != ')':
        return parse_handle_invalid_syntax_for_creating_index()

    index_name = syntax_in_sql_splited[2]
    table_name = syntax_in_sql_splited[4]
    column_name = syntax_in_sql_splited[6]
    return {
        'code': 3,
        'type': 'create',
        'object_name': 'index',
        'index_name': index_name,
        'table_name': table_name,
        'column_name': column_name
    }


def parse_handle_drop_database(syntax_in_sql_splited):
    database_name = syntax_in_sql_splited[2]
    return {
        'code': 4,
        'type': 'drop',
        'object_type': 'database',
        'database_name': database_name
    }


def parse_handle_drop_table(syntax_in_sql_splited):
    table_name = syntax_in_sql_splited[2]
    return {
        'code': 5,
        'type': 'drop',
        'object_type': 'table',
        'table_name': table_name
    }


def parse_handle_use(syntax_in_sql_splited):
    database_name = syntax_in_sql_splited[1]
    return {
        'code': 0,
        'type': 'use',
        database_name: database_name
    }


def parse_handle_inserting(syntax_in_sql_splited):
    if len(syntax_in_sql_splited) < 8:
        return parse_handle_invalid_syntax_for_inserting()

    if len(syntax_in_sql_splited) % 2 == 1:
        print('1')
        return parse_handle_invalid_syntax_for_inserting()

    if syntax_in_sql_splited[1] != 'INTO':
        print('2')
        return parse_handle_invalid_syntax_for_inserting()

    table_name = syntax_in_sql_splited[2]

    if syntax_in_sql_splited[3] != '(':
        print('3')
        return parse_handle_invalid_syntax_for_inserting() 

    columns = []
    
    i = 4
    while syntax_in_sql_splited[i] != ')' and i < len(syntax_in_sql_splited):
        columns.append(syntax_in_sql_splited[i])
        print(i, syntax_in_sql_splited[i], syntax_in_sql_splited[i + 1])
        if syntax_in_sql_splited[i + 1] != ')':
            if syntax_in_sql_splited[i + 1] != ',':
                print('4')
                return parse_handle_invalid_syntax_for_inserting()
        else:
            i += 1
            break
        
        i += 2

    if i >= len(syntax_in_sql_splited):
        print('5')
        return parse_handle_invalid_syntax_for_inserting()
    
    i += 1

    if syntax_in_sql_splited[i] != 'VALUES':
        print('6')
        return parse_handle_invalid_syntax_for_inserting()
    
    i += 1

    if syntax_in_sql_splited[3] != '(':
        print('7')
        return parse_handle_invalid_syntax_for_inserting()
    
    values = []

    i += 1

    while syntax_in_sql_splited[i] != ')' and i < len(syntax_in_sql_splited):
        values.append(syntax_in_sql_splited[i])

        if syntax_in_sql_splited[i + 1] != ')':
            if syntax_in_sql_splited[i + 1] != ',':
                print('4')
                return parse_handle_invalid_syntax_for_inserting()
        else:
            i += 1
            break
        
        i += 2
        
    if i >= len(syntax_in_sql_splited):
        print('9')
        return parse_handle_invalid_syntax_for_inserting()
    
    if len(columns) != len(values):
        print('10')
        return parse_handle_invalid_syntax_for_inserting()
        
    return {
        'code': 6,
        'type': 'insert',
        'table_name': table_name,
        'columns': columns,
        'values': values
    }


def parse_handle_deleting(syntax_in_sql_splited, where_clause):
    if len(where_clause) == 0:
        return parse_handle_invalid_syntax_for_deleting()

    if len(syntax_in_sql_splited) < 5:
        return parse_handle_invalid_syntax_for_deleting()
    
    if syntax_in_sql_splited[1] != 'FROM':
        return parse_handle_invalid_syntax_for_deleting()
    
    table_name = syntax_in_sql_splited[2]

    if syntax_in_sql_splited[3] != 'WHERE':
        return parse_handle_invalid_syntax_for_deleting()

    conditions = parse_handle_condition(where_clause)


def parse(syntax_in_sql: str):
    data_types = ['int', 'float', 'bit', 'date', 'datetime', 'varchar', 'INT', 'FLOAT', 'BIT', 'DATE', 'DATETIME', 'VARCHAR']

    syntax_in_sql_splited = re.findall(r'\w+|[^\w\s]', syntax_in_sql.upper())

    where_clause_if_exists = re.search(r'WHERE (.+)', syntax_in_sql).group(1)
    print(where_clause_if_exists)

    print(syntax_in_sql_splited, sep=':')

    if len(syntax_in_sql_splited) == 0:
        return parse_handle_no_input()

    command_type = syntax_in_sql_splited[0]

    if command_type == 'CREATE':
        if len(syntax_in_sql_splited) == 1:
            return parse_handle_invalid_object_type()   

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            return parse_handle_create_database(syntax_in_sql_splited)
        elif object_type == 'TABLE':
            return parse_handle_create_table(syntax_in_sql_splited, data_types)
        elif object_type == 'INDEX':
            return parse_handle_create_index(syntax_in_sql_splited)
        else:
            return parse_handle_invalid_object_type()
                    
    elif command_type == 'DROP':
        if len(syntax_in_sql_splited) < 3:
            return parse_handle_invalid_object_type()
        
        object_type = syntax_in_sql_splited[1]
        
        if object_type == 'DATABASE':
            return parse_handle_drop_database(syntax_in_sql_splited)
        elif object_type == 'TABLE':
            return parse_handle_drop_table(syntax_in_sql_splited)
        else: 
            return parse_handle_invalid_object_type()
    elif command_type == 'USE':
        if len(syntax_in_sql_splited) < 2:
            return parse_handle_invalid_object_type()
        return parse_handle_use(syntax_in_sql_splited)
    elif command_type == 'INSERT':
        return parse_handle_inserting(syntax_in_sql_splited)
    elif command_type == 'DELETE':
        return parse_handle_deleting(syntax_in_sql_splited, where_clause_if_exists)
    else:
        return parse_handle_invalid_sql_command()
                    

syntax = "DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';"
syntax_split = parse(syntax)
print(syntax_split)