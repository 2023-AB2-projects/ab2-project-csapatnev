# Official client side of DBMSName
# ToDo: 
#       Socket comm handling
#       Server handshake check and error output if needed

from socket import *
import tkinter as tk

HOST = '127.0.0.1'
PORT = 6969     # nice

def send_message_to_server(message):
    message = message.encode()
    with socket(AF_INET, SOCK_STREAM) as client_socket:
        client_socket.connect((HOST,PORT))
        client_socket.send(message)

        response = client_socket.recv(1024)
        if response.decode() == "error":
            # print to console part of app
            print("Error: server returned an error message")
        else:
            print(response)


def client_message_recv():
    with socket(AF_INET, SOCK_STREAM) as server_socket:
        server_socket.bind(('', PORT))
        server_socket.listen()

        print("Server is waiting for a message")

        # while True:
        conn, addr = server_socket.accept()
        print("Connection established to client")

        data = conn.recv(1024)

        if data:
            # Process the received message
            response = "ok"
            print(data.decode())
        else:
            response = "error"
        
        response = response.encode()

        conn.send(response)

        conn.close()

    return data.decode()


if __name__ == "__main__":
    f_input = open("input.txt", "r")
    input = f_input.read()
    send_message_to_server(input)
    # client_message_recv()