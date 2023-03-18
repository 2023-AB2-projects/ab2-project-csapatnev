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
            return data

if __name__ == "__main__":
    #msg = client_message_recv()

    retTuple = cmd.create_database("LajosAB")
    print(retTuple[1])

    list_of_columns = [("telefonszam", "int"), ("Csalad", "string")]

    retTuple = cmd.create_table("LajosAB", "Telefonok", list_of_columns)
    print(retTuple[1])

    # retTuple = cmd.drop_table("LajosAB", "Telefonok")
    # print(retTuple[1])