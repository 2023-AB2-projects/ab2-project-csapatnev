# Official Server side for DBMSName
# ToDo:
#       Create Classes for individual syntax keywords
#       Create a parsing function
#       Establish connection to client side, check for errors / error messages
#       CREATE
#       DATABASE
#       TABLE
#       DROP
#       CREATE INDEX
#       xml file handling / reading / writing
#       Required data types: int, float, bit, date, datetime, 
#                            varchar(or string without specified length)

import socket as sck
import backend.commands as cmd
import myparser.parsing_syntax as prs
import backend.mongoHandler as mh
import pdb

import xml.etree.ElementTree as ET

from protocol.simple_protocol import *

HOST = '127.0.0.1'
PORT = 6969 # nice

DATABASE_IN_USE = "MASTER"

RESPONSE_MESSAGES = []
TABLES_TO_SELECT = []


# CREATE DATABASE database_name;
def create_database_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = res['database_name']
    
    ret_val, err_msg = cmd.create_database(db_name)
    if ret_val >= 0:
        global DATABASE_IN_USE
        DATABASE_IN_USE = db_name
        response_msg = err_msg

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# CREATE TABLE table_name (col_definitions); 
def create_table_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    columns = res['column_definitions']
    pk = res['primary_keys']
    fk = res['references']
    uk = res['unique']

    ret_val, err_msg = cmd.create_table(db_name, table_name, columns, pk, fk, uk)
    if ret_val >= 0:
        response_msg = err_msg 

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        print(f'{ret_val} : {err_msg}')
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# CREATE INDEX <index_name> on table_name (col1, col2, ...);
def create_index_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    index_name = res['index_name'].upper()
    table_name = res['table_name'].upper()
    columns = res['columns']
    
    ret_val, err_msg = cmd.create_index(mongoclient, db_name, table_name, index_name, columns)
    if ret_val >= 0:
        response_msg = err_msg
        
        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else:
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# DROP DATABASE database_name
def drop_database_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = res['database_name']
    
    ret_val, err_msg = cmd.drop_database(db_name, mongoclient)
    if ret_val >= 0:
        global DATABASE_IN_USE
        DATABASE_IN_USE = "MASTER"
        response_msg = err_msg
        
        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# DROP TABLE table_name;
def drop_table_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    
    ret_val, err_msg = cmd.drop_table(db_name, table_name, mongoclient)
    if ret_val >= 0:
        response_msg = err_msg

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')
        
    return ret_val


# USE DATABASE db_name;
def use_request(mode, res, mongoclient, connection_socket: sck.socket):
    global DATABASE_IN_USE
    DATABASE_IN_USE = res['database_name']
    return 0


# INSERT INTO table_name (col1, col2, col3, ...) values (val1, val2, val3, ...);
def insert_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    columns = res['columns']
    values = res['values']
    
    ret_val, err_msg = cmd.insert_into(db_name, table_name, columns, values, mongoclient)
    if ret_val >= 0:
        response_msg = err_msg
        
        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# DELETE FROM table_name WHERE condition;
def delete_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    filter_conditions = res['condition']
    
    ret_val, err_msg = cmd.delete_from(db_name, table_name, filter_conditions, mongoclient)
    if ret_val >= 0:
        response_msg = err_msg

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else:
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# UPDATE table_name SET (col1 = value1, col2 = value2, ...) WHERE condition;
def update_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE

    table_name = res['table_name']
    return 0


# SELECT columns FROM table_name <JOIN join_condition> <WHERE condition> <GROUP BY col1, col2, ...>;
def select_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    select_clause = res['select_clause']
    select_distinct = res['select_distinct']
    from_clause = res['from_clause']
    join_clause = res['join_clause']
    where_clause = res['where_clause']
    groupby_clause = res['groupby_clause']
    
    ret_val, err_msg = cmd.select(db_name, select_clause, select_distinct, from_clause, join_clause, where_clause, groupby_clause, mongoclient)
    if ret_val >= 0:
        response_msg = 'Selected successfully!'
        
        if mode == 'debug': print(response_msg)
        else:
            RESPONSE_MESSAGES.append(response_msg)
            RESPONSE_MESSAGES.append('select') 
            RESPONSE_MESSAGES.append(str(err_msg)) 
    else:
        if mode == 'debug': print(err_msg)
        else:
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# parse through the whole input file, and search for errors            
def first_parse(syntax: str, mode = ''):
    error_found = False
    error_messages = []

    for res in syntax:
        if res['code'] < 0:
            error_found = True
            error_messages.append(res['message'])
            
            if mode == 'debug': print(res)
    
    return error_found, error_messages


REQUEST_HANDLING = {
    1: create_database_request,
    2: create_table_request,
    3: create_index_request,
    4: drop_database_request,
    5: drop_table_request,
    6: use_request,
    7: insert_request,
    8: delete_request,
    9: update_request,
    10: select_request
}


def test_syntax(syntax: str, connection_socket: sck.socket, mode=''):
    # first parse:
    error_found, error_messages = first_parse(syntax, mode)
    if error_found:
        if mode == 'debug': print("ERROR FOUND AT FIRST PARSE!")
        
        for msg in error_messages:
            if mode == 'debug': print(msg)
            else: send_one_message(connection_socket, msg)
        
        print("breaking out")
        
        if mode != 'debug':
            send_one_message(connection_socket, 'breakout')
        return -1

    global DATABASE_IN_USE
    global TABLES_TO_SELECT
    mongodb, mongoclient = mh.connect_mongo(DATABASE_IN_USE)

    status_code = 0
    for res in syntax: 
        status_code = REQUEST_HANDLING[res['code']](mode, res, mongoclient, connection_socket) 
        if status_code < 0: break      

    if status_code == 0:
        for response_msg in RESPONSE_MESSAGES:
            if mode != 'debug':
                send_one_message(connection_socket, response_msg)

        if mode != 'debug':
            send_one_message(connection_socket, 'breakout')
        

def create_tree_from_xml():
    response = {}
    tree = ET.parse('databases.xml')
    root = tree.getroot()

    for database in root.findall('Database'):
        response[database.attrib['name']] = []
        table_response = {}
        for table in database.findall('.//Table'):
            table_response[table.attrib['name']] = []
            for attribute in table.findall('.//Attribute'):
                table_response[table.attrib['name']].append(attribute.attrib['attributeName'])
            response[database.attrib['name']].append(table_response)
            table_response = {}

    return response


def get_databases_and_tables_from_xml():
    response_databases = {}
    response_tables = {}
    
    tree = ET.parse('databases.xml')
    root = tree.getroot()

    for database in root.findall('Database'):
        response_databases[database.attrib['name']] = []
        for table in database.findall('.//Table'):
            response_databases[database.attrib['name']].append(table.attrib['name'])
            response_tables[table.attrib['name']] = []
            for attribute in table.findall('.//Attribute'):
                response_tables[table.attrib['name']].append(attribute.attrib['attributeName'])

    return response_databases, response_tables


# CLIENT's 'run' COMMAND:
def handle_run_request(connection_socket):
    global RESPONSE_MESSAGES
    RESPONSE_MESSAGES = []
            
    message = recv_one_message(connection_socket).decode()

    full_request = prs.handle_my_sql_input(message)
    test_syntax(full_request, connection_socket)


# CLIENT's 'console' COMMAND:
def handle_console_request(connection_socket):
    global RESPONSE_MESSAGES
    RESPONSE_MESSAGES = []
            
    message = recv_one_message(connection_socket).decode()

    full_request = prs.handle_my_sql_input(message)
    test_syntax(full_request, connection_socket)


# CLIENT's 'tree' COMMAND:
def handle_tree_request(connection_socket):
    response = create_tree_from_xml()

    send_one_message(connection_socket, str(response))
    send_one_message(connection_socket, 'breakout')


# send additional data for console command:
def handle_sql_prompt_request(connection_socket):
    db_in_use = DATABASE_IN_USE
    databases, tables = get_databases_and_tables_from_xml()

    response = {
        'db_in_use': db_in_use,
        'databases': databases,
        'tables': tables,
    }
    send_one_message(connection_socket, str(response))


def server_side():
    global DATABASE_IN_USE
    global RESPONSE_MESSAGES

    HOST = '127.0.0.1'
    PORT = 6969

    server_socket = sck.socket(sck.AF_INET, sck.SOCK_STREAM)
    server_socket.bind((HOST, PORT))

    server_socket.listen(1)
    print('The server is available')

    connection_socket, addr = server_socket.accept()
    print('The client has connected to the server', addr)

    while True:
        request = recv_one_message(connection_socket).decode()

        if request == 'kill yourself':
            break

        if request == 'run':
            handle_run_request(connection_socket)
            continue
        
        if request == 'sql_prompt_data':
            handle_sql_prompt_request(connection_socket)
            continue

        if request == 'console':
            handle_console_request(connection_socket)
            continue
        
        if request == 'tree':
            handle_tree_request(connection_socket)
            continue


if __name__ == "__main__":
    #server_side()

    syntax = """
    drop database tmp;
    create database tmp;

    use tmp;

    create table Subjects (
        SubjectID int primary key,
        SubjectName varchar(255),
        Courses int
    );

    create table Felhasznalok ( 
        FID int primary key,
        FName varchar(255),
        FType varchar(255)
    );


    create table Resources (
        RID int primary key,
        RName varchar(255),
        SubjectID int REFERENCES Subjects (SubjectID)
    );


    insert into SUBJECTS (SubjectID, SubjectName, Courses) values (1, 'asd', 3);
    insert into Subjects (SubjectID, SubjectName, Courses) values (2, 'asd', 10);
    insert into Subjects (SubjectID, SubjectName, Courses) values (3, 'asd', 2);
    insert into Subjects (SubjectID, SubjectName, Courses) values (4, 'asd', 10);
    insert into Subjects (SubjectID, SubjectName, Courses) values (5, 'asd', 1);
    insert into Subjects (SubjectID, SubjectName, Courses) values (6, 'asd', 8);
    insert into Subjects (SubjectID, SubjectName, Courses) values (7, 'asd', 7);
    insert into Subjects (SubjectID, SubjectName, Courses) values (8, 'asd', 6);
    insert into Subjects (SubjectID, SubjectName, Courses) values (9, 'asd', 5);


    insert into Felhasznalok (FID, FName, FType) values (1, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (2, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (3, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (4, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (5, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (6, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (7, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (8, 'Name', 'client');
    insert into Felhasznalok (FID, FName, FType) values (9, 'Name', 'client');


    insert into Resources (RID, RName, SubjectID) values (11, 'video', 1);
    insert into Resources (RID, RName, SubjectID) values (2, 'video', 1);
    insert into Resources (RID, RName, SubjectID) values (3, 'video', 1);
    insert into Resources (RID, RName, SubjectID) values (4, 'video', 2);
    insert into Resources (RID, RName, SubjectID) values (5, 'video', 2);
    insert into Resources (RID, RName, SubjectID) values (6, 'video', 2);
    insert into Resources (RID, RName, SubjectID) values (7, 'video', 3);
    insert into Resources (RID, RName, SubjectID) values (8, 'video', 3);
    insert into Resources (RID, RName, SubjectID) values (9, 'video', 3);

    select *
    from Subjects
    where Subjects.Courses = 10;

    select * from SUBJECTS join RESOURCES on SUBJECTS.SUBJECTID = RESOURCES.SUBJECTID where SUBJECTS.SUBJECTNAME = 'ASD';
    """

    syntax = prs.handle_my_sql_input(syntax)
    test_syntax(syntax, '', 'debug')