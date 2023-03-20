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

HOST = '127.0.0.1'
PORT = 6969 # nice

DATABASE_IN_USE = ""

def server_side():
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

        res = prs.parse(message)
        if res['code'] < 0:
            errmsg = res['message']
            connection_socket.send(errmsg.encode())
        else:
            code = res['code']
            if code == 1:
                # create db
                db_name = res['database_name']
                cmd.create_database(db_name)
                DATABASE_IN_USE = db_name
                response_msg = 'Database has been created!'
                connection_socket.send(response_msg.encode())
            elif code == 2:
                # create table
                table_name = res['table_name']
                columns = res['column_definitions']
                db_name = DATABASE_IN_USE
                cmd.create_table(db_name, table_name, columns)
                response_msg = 'Table has been created!'
                connection_socket.send(response_msg.encode())
            elif code == 3:
                # create index
                # kossed ossze a create_index functionnal
                response_msg = 'Index has been created!'
                connection_socket.send(response_msg.encode())
            elif code == 4:
                # drop db
                db_name = res['database_name']
                cmd.drop_database(db_name)
                DATABASE_IN_USE = ""
                response_msg = 'Database has been droped!'
                connection_socket.send(response_msg.encode())
            elif code == 5:
                # drop table
                db_name = DATABASE_IN_USE
                table_name = res['table_name']
                cmd.drop_table(db_name, table_name)
                response_msg = 'Table has been created!'
                connection_socket.send(response_msg.encode())
            elif code == 6:
                # use database db_name
                DATABASE_IN_USE = res['database_name']


if __name__ == "__main__":
    server_side()
    
    # # testing create index
    # cmd.create_database("testDB")
    # cmd.create_table("testDB", "testTable", ["col1 int", "col2 int", "col3 int"])
    # cmd.create_index("testDB", "testTable", "col1")
        
