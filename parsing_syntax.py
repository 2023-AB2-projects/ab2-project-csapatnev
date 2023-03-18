# return values:
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE

command_dict = {
    "code" : -1,
    "database_name" : "",
    "table_name" : "",
    "index_name" : "",

}

def parse(syntax_in_sql: str):
    syntax_in_sql_splited = syntax_in_sql.upper.split()
    
    command_type = syntax_in_sql_splited[0]

    if command_type == 'CREATE':
        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            database_name = syntax_in_sql_splited[2]
            return {
                'code': 1,
                'type': 'create',
                'object_type': 'database',
                'database_name': database_name 
            }
        elif object_type == 'TABLE':
            table_name = syntax_in_sql_splited[2]
            column_definitions = []
            
            i = 4
            while i < len(syntax_in_sql_splited):
                if syntax_in_sql_splited[i] == '(':
                    i += 1
                    while i < len(syntax_in_sql_splited) and syntax_in_sql_splited[i] != ')':
                        column_name = syntax_in_sql_splited[i]
                        data_type = syntax_in_sql_splited[i + 1]
                        column_definitions.append((column_name, data_type))
                        i += 2
                    break
                else:
                    i += 1
            return {
                'code': 2,
                'type': 'create', 
                'object_type': 'table',
                'table_name': table_name,
                'column_definitions': column_definitions
            }
        elif object_type == 'INDEX':
            index_name = syntax_in_sql_splited[2]
            table_name = syntax_in_sql_splited[4]
            column_name = syntax_in_sql_splited[6]
            return {
                'type': 'create',
                'object_name': 'index',
                'index_name': index_name,
                'table_name': table_name,
                'column_name': column_name
            }
    elif command_type == 'DROP':
        object_type = syntax_in_sql_splited[1]
        
        if object_type == 'database':
            database_name = syntax_in_sql_splited[2]
            return {
                'code': 4,
                'type': 'drop',
                'object_type': 'database',
                'database_name': database_name
            }
        elif object_type == 'table':
            table_name = syntax_in_sql_splited[2]
            return {
                'code': 5,
                'type': 'drop',
                'object_type': 'table',
                'table_name': table_name
            }
    else:
        return {
            'code': -1,
            'error': 'CommandError',
            'message': 'Invalid SQL command!'
        }
                    

        

