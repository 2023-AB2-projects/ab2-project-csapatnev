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

from socket import *
import commands as cmd
import parsing_syntax as prs
import mongoHandler as mh
import pdb


def test_syntax_WO_client(syntax):
    global DATABASE_IN_USE
    mongodb, mongoclient = mh.connect_mongo(DATABASE_IN_USE)

    for res in syntax:    # iterate through each request
        if res['code'] < 0:
            errmsg = res['message']
            print(errmsg)
            #connection_socket.send(errmsg.encode())
        else:
            code = res['code']
            if code == 1:
                # create db
                db_name = res['database_name']
                ret_val, err_msg = cmd.create_database(db_name)
                if ret_val >= 0:
                    DATABASE_IN_USE = db_name
                    response_msg = 'Database has been created!'
                    print(response_msg)
                    #connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    #connection_socket.send(err_msg.encode())
            elif code == 2:
                # create table
                table_name = res['table_name']
                columns = res['column_definitions']
                pk = res['primary_keys']
                fk = res['references']
                db_name = DATABASE_IN_USE
                ret_val, err_msg = cmd.create_table(db_name, table_name, columns, pk, fk)
                if ret_val >= 0:
                    response_msg = 'Table has been created!'
                    print(response_msg)
                    #connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    #connection_socket.send(err_msg.encode())
            elif code == 3:
                # create index
                db_name = DATABASE_IN_USE
                index_name = res['index_name'].upper()
                table_name = res['table_name'].upper()
                columns = res['columns']
                ret_val, err_msg = cmd.create_index(db_name, table_name, index_name, columns)
                if ret_val >= 0:
                    response_msg = 'Index has been created!'
                    print(response_msg)
                    #connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    #connection_socket.send(err_msg.encode())
            elif code == 4:
                # drop db
                db_name = res['database_name']
                ret_val, err_msg = cmd.drop_database(db_name, mongoclient)
                if ret_val >= 0:
                    DATABASE_IN_USE = "MASTER"
                    response_msg = 'Database has been droped!'
                    print(response_msg)
                    #connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    #connection_socket.send(err_msg.encode())
            elif code == 5:
                # drop table
                db_name = DATABASE_IN_USE
                table_name = res['table_name']
                ret_val, err_msg = cmd.drop_table(db_name, table_name, mongodb)
                if ret_val >= 0:
                    response_msg = 'Table has been created!'
                    print(response_msg)
                    #connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    #connection_socket.send(err_msg.encode())
            elif code == 6:
                # use database db_name
                DATABASE_IN_USE = res['database_name']
            elif code == 7:
                # insert into table_name (col1, col2, col3) values (val1, val2, val3)
                db_name = DATABASE_IN_USE
                table_name = res['table_name']
                columns = res['columns']
                values = res['values']
                # def insert_into(db_name, table_name, columns, values, mongodb):
                ret_val, err_msg = cmd.insert_into(db_name, table_name, columns, values, mongoclient)
                if ret_val >= 0:
                    response_msg = 'Data has been inserted!'
                    print(response_msg)
                    #connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    #connection_socket.send(err_msg.encode())
            elif code == 8:
                # delete from table_name where studid > 1000
                table_name = res['table_name']
                db_name = DATABASE_IN_USE
                filter_conditions = res['condition']
                print(filter_conditions)
                ret_val, err_msg = cmd.delete_from(db_name, table_name, filter_conditions, mongoclient)
                if ret_val >= 0:
                    response_msg = 'Data has been deleted!'
                    print(response_msg)
                    #connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    #connection_socket.send(err_msg.encode())
    
    data = cmd.select_all("UNIVERSITY", "DISCIPLINES", mongodb)
    for d in data:
        print(d)
    #connection_socket.send("breakout".encode()) # close the client



HOST = '127.0.0.1'
PORT = 6969 # nice

DATABASE_IN_USE = "MASTER"

def server_side():
    global DATABASE_IN_USE
    mongodb, mongoclient = mh.connect_mongo(DATABASE_IN_USE)


    HOST = '127.0.0.1'
    PORT = 6969

    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)

    print('The server is available')

    while True:
        connection_socket, addr = server_socket.accept()
        print('The client has connected to the server', addr)
        data = connection_socket.recv(1024)
        message = data.decode()

        full_request = prs.handle_my_sql_input(message)
        test_syntax(full_request, connection_socket)

# parse through the whole input file, and search for errors            
def first_parse(syntax):
    error_found = 0
    error_messages = []
    for res in syntax:
        if res['code'] < 0:
            error_found = 1
            error_messages.append(res['message'])
    return error_found, error_messages
            

def test_syntax(syntax, connection_socket):
    # first parse:
    error_found, error_messages = first_parse(syntax)
    if error_found:
        for msg in error_messages:
            connection_socket.send(msg.encode())
        connection_socket.send("breakout".encode())
        return -1

    global DATABASE_IN_USE
    mongodb, mongoclient = mh.connect_mongo(DATABASE_IN_USE)

    for res in syntax:    # iterate through each request
        if res['code'] < 0:
            errmsg = res['message']
            print(errmsg)
            connection_socket.send(errmsg.encode())
        else:
            code = res['code']
            if code == 1:
                # create db
                db_name = res['database_name']
                ret_val, err_msg = cmd.create_database(db_name)
                if ret_val >= 0:
                    DATABASE_IN_USE = db_name
                    response_msg = 'Database has been created!'
                    print(response_msg)
                    connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    connection_socket.send(err_msg.encode())
            elif code == 2:
                # create table
                table_name = res['table_name']
                columns = res['column_definitions']
                pk = res['primary_keys']
                fk = res['references']
                db_name = DATABASE_IN_USE
                ret_val, err_msg = cmd.create_table(db_name, table_name, columns, pk, fk)
                if ret_val >= 0:
                    response_msg = 'Table has been created!'
                    print(response_msg)
                    connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    connection_socket.send(err_msg.encode())
            elif code == 3:
                # create index
                db_name = DATABASE_IN_USE
                index_name = res['index_name'].upper()
                table_name = res['table_name'].upper()
                columns = res['columns']
                ret_val, err_msg = cmd.create_index(db_name, table_name, index_name, columns)
                if ret_val >= 0:
                    response_msg = 'Index has been created!'
                    print(response_msg)
                    connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    connection_socket.send(err_msg.encode())
            elif code == 4:
                # drop db
                db_name = res['database_name']
                ret_val, err_msg = cmd.drop_database(db_name, mongoclient)
                if ret_val >= 0:
                    DATABASE_IN_USE = "MASTER"
                    response_msg = 'Database has been droped!'
                    print(response_msg)
                    connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    connection_socket.send(err_msg.encode())
            elif code == 5:
                # drop table
                db_name = DATABASE_IN_USE
                table_name = res['table_name']
                ret_val, err_msg = cmd.drop_table(db_name, table_name, mongoclient)
                if ret_val >= 0:
                    response_msg = err_msg
                    print(response_msg)
                    connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    connection_socket.send(err_msg.encode())
            elif code == 6:
                # use database db_name
                DATABASE_IN_USE = res['database_name']
            elif code == 7:
                # insert into table_name (col1, col2, col3) values (val1, val2, val3)
                db_name = DATABASE_IN_USE
                table_name = res['table_name']
                columns = res['columns']
                values = res['values']
                # def insert_into(db_name, table_name, columns, values, mongodb):
                ret_val, err_msg = cmd.insert_into(db_name, table_name, columns, values, mongoclient)
                if ret_val >= 0:
                    response_msg = 'Data has been inserted!'
                    print(response_msg)
                    connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    connection_socket.send(err_msg.encode())
            elif code == 8:
                # delete from table_name where studid > 1000
                table_name = res['table_name']
                db_name = DATABASE_IN_USE
                filter_conditions = res['condition']
                ret_val, err_msg = cmd.delete_from(db_name, table_name, filter_conditions, mongodb)
                if ret_val >= 0:
                    response_msg = 'Data has been deleted!'
                    print(response_msg)
                    connection_socket.send(response_msg.encode())
                else:
                    print(err_msg)
                    connection_socket.send(err_msg.encode())

    print("breaking out")
    connection_socket.send("breakout".encode()) # close the client
        

if __name__ == "__main__":
    server_side()
    # syntax = """
    # USE University;

    # DELETE FROM DISCIPLINES WHERE CreditNr = 2
    # """
    # syntax = prs.handle_my_sql_input(syntax)
    # test_syntax_WO_client(syntax)