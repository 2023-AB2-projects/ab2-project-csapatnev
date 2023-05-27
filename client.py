# Official client side of DBMSName
# ToDo:
#       Socket comm handling
#       Server handshake check and error output if needed

from socket import *
import re
import subprocess
import sys
import os.path

HOST = '127.0.0.1'
PORT = 6969     # nice

CONNECTED_TO_SERVER = False

client_socket = socket(AF_INET, SOCK_STREAM)


class console_colors:
    red = '\x1b[0;31;3m'
    green = '\x1b[0;32;3m'
    yellow = '\x1b[0;33;3m'
    blue = '\x1b[0;34;3m'
    end = '\x1b[0m'


def print_header():
    print(console_colors.green + 'Welcome to DBMSName!\n' + console_colors.end)
    print(console_colors.green + 'Commands:' + console_colors.end)
    print('\tconnect\t\t\t' + console_colors.blue +
          'Connect to server' + console_colors.end)
    print('\trun [FILENAME]\t\t' + console_colors.blue +
          'Run your SQL code from file' + console_colors.end)
    print('\tconsole\t\t\t' + console_colors.blue +
          'Allows you to write SQL code in console' + console_colors.end)
    print('\ttree\t\t\t' + console_colors.blue +
          'View stored databases' + console_colors.end)
    print('\texit\t\t\t' + console_colors.blue +
          'Exit DBMSName' + console_colors.end + '\n')


def send_message_to_server(message):
    client_socket = socket(AF_INET, SOCK_STREAM)
    client_socket.connect((HOST, PORT))
    client_socket.send(message.encode())

    while True:
        response = client_socket.recv(999999999).decode()
        if response == "breakout":
            print("Server finished with it's tasks, client can rest now!")
            break
        else:
            print(response)


def client_parse_command(command):
    global CONNECTED_TO_SERVER

    connect_pattern = r'^\s*connect\s*$'
    match_connect = re.match(connect_pattern, command)
    if match_connect != None:
        print('\n' + console_colors.green + 'Succesfully connected to ' +
              HOST + ':' + str(PORT) + console_colors.end + '\n')
        subprocess.Popen(['python', 'server.py'],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        client_socket.connect((HOST, PORT))

        CONNECTED_TO_SERVER = True
        return 1

    run_pattern = r'^\s*run\s([^\s]+)\s*$'
    match_run = re.match(run_pattern, command)
    if match_run != None:
        if CONNECTED_TO_SERVER != False:
            if os.path.isfile(match_run.group(1)):
                f_input = open(match_run.group(1), "r")
                request = f_input.read()
                client_socket.send(request.encode())
                response = client_socket.recv(999999999).decode()
                print(console_colors.yellow +
                      '[Server]:' + console_colors.end + response)
            else:
                print(console_colors.red +
                      '[Error]: Cannot open File! ' + console_colors.end + match_run.group(1))
        else:
            print(console_colors.red +
                  '[Error]: You need to connect to the server first!' + console_colors.end)
        return 2

    console_pattern = r'^\s*console\s*$'
    match_console = re.match(console_pattern, command)
    if match_console != None:
        if CONNECTED_TO_SERVER != False:
            console_input = ''
            request = ''
            while (console_input != '<'):
                print(console_colors.blue +
                      '[Console]: ' + console_colors.end, end='')
                console_input = input()
                if console_input != '<':
                    request += console_input
            client_socket.send(request.encode())
            response = client_socket.recv(999999999).decode()
            print(console_colors.yellow +
                  '[Server]:' + console_colors.end + response)
        else:
            print(console_colors.red +
                  '[Error]: You need to connect to the server first!' + console_colors.end)
        return 3

    exit_pattern = r'^\s*exit\s*$'
    match_exit = re.match(exit_pattern, command)
    if match_exit != None:
        if CONNECTED_TO_SERVER == True:
            print('\n' + console_colors.green +
                  'Closing Server...' + console_colors.end)
            client_socket.send('kill yourself'.encode())

        print('\n' + console_colors.green +
              'Have a great Day!' + console_colors.end + '\n')

        client_socket.close()
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
    # send_message_to_server(input)
