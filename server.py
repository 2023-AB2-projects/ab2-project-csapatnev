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

            print(f"Message: {data}")
            conn.close()

if __name__ == "__main__":
    client_message_recv()