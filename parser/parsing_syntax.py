# return (code) values:
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE
#   6 - USE
#   7 - INSERT
#   8 - DELETE
#   9 - UPDATE

import re

from entitys.create_database import parse_handle_create_database
from entitys.create_table import parse_handle_create_table
from entitys.create_index import parse_handle_create_index
from entitys.drop_database import parse_handle_drop_database
from entitys.drop_table import parse_handle_drop_table
from entitys.use import parse_handle_use
from entitys.insert import parse_handle_insert
from entitys.delete import parse_handle_delete
from entitys.select import parse_handle_select
from entitys.update import parse_handle_update


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


def parse_handle_no_input():
    return {
        'code': -1,
        'message': 'No sql command has been provided!'
    }


# parsing of every single sql command
def parse(syntax_in_sql: str):
    # spliting the input command string so it can determine which command keyword it contains
    syntax_in_sql_splited = re.findall(r'\w+|[^\w\s]', syntax_in_sql.upper())

    if len(syntax_in_sql_splited) == 0:
        return parse_handle_no_input()

    command_type = syntax_in_sql_splited[0]

    # handling of every single command
    if command_type == 'CREATE':
        if len(syntax_in_sql_splited) == 1:
            return parse_handle_invalid_object_type()

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            return parse_handle_create_database(syntax_in_sql)
        elif object_type == 'TABLE':
            return parse_handle_create_table(syntax_in_sql)
        elif object_type == 'INDEX':
            return parse_handle_create_index(syntax_in_sql)
        else:
            return parse_handle_invalid_object_type()

    elif command_type == 'DROP':
        if len(syntax_in_sql_splited) < 3:
            return parse_handle_invalid_object_type()

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            return parse_handle_drop_database(syntax_in_sql)
        elif object_type == 'TABLE':
            return parse_handle_drop_table(syntax_in_sql)
        else:
            return parse_handle_invalid_object_type()
    elif command_type == 'USE':
        if len(syntax_in_sql_splited) < 2:
            return parse_handle_invalid_object_type()
        return parse_handle_use(syntax_in_sql)
    elif command_type == 'INSERT':
        return parse_handle_insert(syntax_in_sql)
    elif command_type == 'DELETE':
        return parse_handle_delete(syntax_in_sql)
    elif command_type == 'UPDATE':
        return parse_handle_update(syntax_in_sql)
    elif command_type == 'SELECT':
        return parse_handle_select(syntax_in_sql)
    else:
        return parse_handle_invalid_sql_command()


# handle all of the input string as an sql code
def handle_my_sql_input(input_str: str):
    # removes all the substrings between sql comment identifiers /*(...)*/
    sql_code_without_comments = re.sub('/\*.*?\*/', '', input_str)

    # splits the string into commands
    commands_raw = re.split('(CREATE|INSERT|USE|DROP|DELETE|UPDATE|SELECT)',
                            sql_code_without_comments, flags=re.IGNORECASE)

    # removes whitespaces
    commands_raw = [command_raw.strip()
                    for command_raw in commands_raw if command_raw.strip()]

    # putting the sql command keyword and the command itself into one element of the list
    for i in range(len(commands_raw) - 1):
        if commands_raw[i].upper() in ['CREATE', 'INSERT', 'USE', 'DROP', 'DELETE', 'UPDATE', 'SELECT']:
            commands_raw[i] += ' ' + commands_raw[i + 1]
            commands_raw[i + 1] = ''

    # removing any remaining whitespaces
    commands_in_sql = [command_raw.strip()
                       for command_raw in commands_raw if command_raw.strip()]

    # for every single sql command executes the parse function, and returns the full
    # list of commands for the server
    commands = []
    for command_in_sql in commands_in_sql:
        command = parse(command_in_sql)
        print(command)
        commands.append(command)

    return commands


# syntax = '''
# select DName, CreditNr, Mark
# from students
# join marks on students.StudID = marks.StudID 
# join disciplines on marks.discID = disciplines.discID
# where not StudName >= 10 and not DName = 'Pista'
# group by DName;
# '''

# asd = handle_my_sql_input(syntax)

