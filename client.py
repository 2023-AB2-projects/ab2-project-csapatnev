# Official client side of DBMSName
# ToDo:
#       Socket comm handling
#       Server handshake check and error output if needed

from socket import *
import re
import subprocess
import sys

HOST = '127.0.0.1'
PORT = 6969     # nice

connected_to_server = False
client_socket = socket(AF_INET, SOCK_STREAM)

class console_colors:
    red = '\x1b[0;31;3m'
    green = '\x1b[0;32;3m'
    yellow = '\x1b[0;33;3m'
    blue = '\x1b[0;34;3m'
    end = '\x1b[0m'


def send_message_to_server(message):
    client_socket = socket(AF_INET, SOCK_STREAM)
    client_socket.connect((HOST, PORT))
    client_socket.send(message.encode())

    while True:
        response = client_socket.recv(9999999).decode()
        if response == "breakout":
            print("Server finished with it's tasks, client can rest now!")
            break
        else:
            print(response)


# def client_message_recv():
#     with socket(AF_INET, SOCK_STREAM) as server_socket:
#         server_socket.bind(('', PORT))
#         server_socket.listen()

#         print("Server is waiting for a message")

#         conn, addr = server_socket.accept()
#         print("Connection established to client")

#         data = conn.recv(9999999)

#         if data:
#             # Process the received message
#             response = "ok"
#             print(data.decode())
#         else:
#             response = "error"

#         response = response.encode()

#         conn.send(response)

#         conn.close()

#     return data.decode()

def print_header():
    print(console_colors.green + 'Welcome to DBMSName!\n' + console_colors.end)
    print(console_colors.green + 'Commands:' + console_colors.end)
    print('\tconnect\t\t\t' + console_colors.blue +
          'Connect to server' + console_colors.end)
    print('\trun [FILENAME]\t\t' + console_colors.blue +
          'Run SQL code from file' + console_colors.end)
    print('\tconsole\t\t\t' + console_colors.blue +
          'Allows to write SQL code in console' + console_colors.end)
    print('\ttree\t\t\t' + console_colors.blue +
          'View stored databases' + console_colors.end)
    print('\texit\t\t\t' + console_colors.blue +
          'Exit DBMSName' + console_colors.end + '\n')


def client_parse_command(command):
    connect_pattern = r'^\s*connect\s*'
    match_connect = re.match(connect_pattern, command)
    if match_connect != None:
        print('\n' + console_colors.green + 'Succesfully connected to ' +
              HOST + ':' + str(PORT) + console_colors.end + '\n')
        subprocess.Popen([sys.executable, '/server.py'],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        client_socket.connect((HOST, PORT))

        connected_to_server = True
        return 1

    exit_pattern = r'\s*exit\s*'
    match_exit = re.match(exit_pattern, command)
    if match_exit != None:
        if connected_to_server:
            print('\n' + console_colors.green + 'Closing Server...' + console_colors.end)
            client_socket.send('kill yourself'.encode())

        print('\n' + console_colors.green + 'Have a great Day!' + console_colors.end + '\n')
        return 0


def start_application():
    print_header()

    status_code = -1
    while (status_code != 0):
        print(console_colors.green + '[Input]:' + console_colors.end, end=' ')
        command = input()
        status_code = client_parse_command(command)


if __name__ == "__main__":
    start_application()
    # f_input = open("input.txt", "r")
    # input = f_input.read()
    # send_message_to_server(input)
