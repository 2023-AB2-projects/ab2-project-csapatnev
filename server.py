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

def client_message_recv():
    with socket(AF_INET, SOCK_STREAM) as server_socket:
        server_socket.bind(('', PORT))
        server_socket.listen()

        print("Server is waiting for a message")

        while True:
            conn, addr = server_socket.accept()
            print("Connection established to client")

            data = conn.recv(1024)

            if data:
                response = "ok"
            else:
                response = "error"
            
            response = response.encode()

            conn.send(response)

            conn.close()
            return data.decode()
        
def server_send_message(message):
    with socket(AF_INET, SOCK_STREAM) as client_socket:
        client_socket.connect((HOST, PORT))

        message = message.encode()

        client_socket.send(message)

        response = client_socket.recv(1024)

        if response.decode() == "ok":
            print("Message sent successfully")
        else:
            print("Error: Message not sent")


if __name__ == "__main__":
    DATABASE_IN_USE = ""

    while True:
        msg = client_message_recv()

        res = prs.parse(msg)

        if res['code'] < 0:
            errmsg = res['message']
            server_send_message(errmsg)
        else:
            code = res['code']
            if code == 1:
                # create db
                db_name = res['database_name']
                cmd.create_database(db_name)
                DATABASE_IN_USE = db_name
            elif code == 2:
                # create table
                table_name = res['table_name']
                columns = res['column_definitions']
                db_name = DATABASE_IN_USE
                cmd.create_table(db_name, table_name, columns)
            elif code == 3:
                # create index
                print("ahahahaha")
            elif code == 4:
                # drop db
                db_name = res['database_name']
                cmd.drop_database(db_name)
                DATABASE_IN_USE = ""
            elif code == 5:
                # drop table
                db_name = DATABASE_IN_USE
                table_name = res['table_name']
                cmd.drop_table(db_name, table_name)
