# return (code) values:
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE

import re

def parse(syntax_in_sql: str):
    data_types = ['int', 'float', 'bit', 'date', 'datetime', 'varchar', 'INT', 'FLOAT', 'BIT', 'DATE', 'DATETIME', 'VARCHAR']

    syntax_in_sql_splited = re.findall(r'\w+|[^\w\s]', syntax_in_sql.upper())
    print(syntax_in_sql_splited, sep=':')

    if len(syntax_in_sql_splited) == 0:
        return {
            'code': -1,
            'message': 'No sql command has been provided!'
        }

    command_type = syntax_in_sql_splited[0]

    if command_type == 'CREATE':

        if len(syntax_in_sql_splited) == 1:
            return {
                'code': -1,
                'message': 'Object type expected!'
            }   

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':

            if len(syntax_in_sql_splited) == 3:
                database_name = syntax_in_sql_splited[2]
                return {
                    'code': 1,
                    'type': 'create',
                    'object_type': 'database',
                    'database_name': database_name 
                }
            else:
                return {
                    'code': -1,
                    'message': 'Invalid syntax for creating database!'
                }
            
        elif object_type == 'TABLE':
            if len(syntax_in_sql_splited) < 4:
                return {
                    'code': -1,
                    'message': 'Invalid syntax for creating a table.'
                }
                
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
                return {
                    'code': -1,
                    'message': 'Invalid syntax for creating a table.'
                }
            
            else:
                return {
                    'code': 2, 
                    'type': 'create', 
                    'object_type': 'table', 
                    'table_name': table_name, 
                    'column_definitions': column_definitions
                }
            
        elif object_type == 'INDEX':
            if len(syntax_in_sql_splited) != 8:
                return {
                    'code': -1,
                    'message': 'Invalid syntax for creating index!'
                }

            if syntax_in_sql_splited[3] != 'ON':
                return {
                    'code': -1,
                    'message': 'Invalid syntax for creating index!'
                }

            if syntax_in_sql_splited[5] != '(' or syntax_in_sql_splited[7] != ')':
                return {
                    'code': -1,
                    'message': 'Invalid syntax for creating index!'
                }

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
        else:
            return {
                'code': -1,
                'message': 'Invalid sql Object type!'
            }
        
    elif command_type == 'DROP':
        if len(syntax_in_sql_splited) > 2:
            object_type = syntax_in_sql_splited[1]
            
            if object_type == 'DATABASE':
                database_name = syntax_in_sql_splited[2]
                return {
                    'code': 4,
                    'type': 'drop',
                    'object_type': 'database',
                    'database_name': database_name
                }
            elif object_type == 'TABLE':
                table_name = syntax_in_sql_splited[2]
                return {
                    'code': 5,
                    'type': 'drop',
                    'object_type': 'table',
                    'table_name': table_name
                }
        else :
            return {
                'code': -1,
                'message': 'Object type expected!'
            }
    else:
        return {
            'code': -1,
            'message': 'Invalid SQL command!'
        }
                    
        

