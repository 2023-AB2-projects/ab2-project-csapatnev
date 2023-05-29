# Official client side of DBMSName
# ToDo:
#       Socket comm handling
#       Server handshake check and error output if needed

import socket as sck
import re
import subprocess
import sys
import os.path

from enum import Enum
from protocol.simple_protocol import *

HOST = '127.0.0.1'
PORT = 6969     # nice

CONNECTED_TO_SERVER = False

client_socket = sck.socket(sck.AF_INET, sck.SOCK_STREAM)


class Color(Enum):
    RED = 'red'
    GREEN = 'green'
    YELLOW = 'yellow'
    BLUE = 'blue'


console_colors = {
    'red': '\x1b[0;31;3m',
    'green': '\x1b[0;32;3m',
    'yellow': '\x1b[0;33;3m',
    'blue': '\x1b[0;34;3m',
    'end': '\x1b[0m',
}


def str_color(string, color):
    if color in console_colors.keys():
        return console_colors[color] + string + console_colors['end']
    else:
        return string 


def print_header():
    print(str_color('Welcome to DBMSName!\n', Color.GREEN.value))
    print(str_color('Commands:', Color.GREEN.value))
    print('\tconnect\t\t\t' + str_color('Connect to server', Color.BLUE.value))
    print('\trun [FILENAME]\t\t' + str_color('Run your SQL code from file', Color.BLUE.value))
    print('\tconsole\t\t\t' + str_color('Allows you to write SQL code in console', Color.BLUE.value))
    print('\ttree\t\t\t' + str_color('View stored databases', Color.BLUE.value))
    print('\texit\t\t\t' + str_color('Exit DBMSName', Color.BLUE.value))
    print()


def get_message_from_socket(connection_socket: sck.socket):
    message = bytearray()
    while True:
        data = connection_socket.recv(4096)
        message.extend(data)
        if len(data) < 4096:
            break
    return message.decode('utf-8')


def client_parse_command(command):
    global CONNECTED_TO_SERVER

    connect_pattern = r'^\s*connect\s*$'
    match_connect = re.match(connect_pattern, command)
    if match_connect != None:
        print('\n' + str_color('Succesfully connected to ' + HOST + ':' + str(PORT), Color.GREEN.value) + '\n')
        # subprocess.Popen(['python', 'new_server.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

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
                
                send_one_message(client_socket, 'run')
                send_one_message(client_socket, request)

                while True:
                    response = recv_one_message(client_socket).decode()
                    if response == 'breakout': break                    
                    print(str_color('[Server]: ', Color.YELLOW.value) + response)
            else:
                print(str_color('[Error]: Cannot open File! ', Color.RED.value) + match_run.group(1))
        else:
            print(str_color('[Error]: You need to connect to the server first!', Color.RED.value))
        return 2

    console_pattern = r'^\s*console\s*$'
    match_console = re.match(console_pattern, command)
    if match_console != None:
        if CONNECTED_TO_SERVER != False:
            console_input = ''
            request = ''
            
            while (console_input != '<'):
                print(str_color('[Console]: ', Color.BLUE.value), end='')
                
                console_input = input()
                
                if console_input != '<':
                    request += console_input
            
            send_one_message(client_socket, 'console')
            send_one_message(client_socket, request)

            while True:
                    response = recv_one_message(client_socket).decode()
                    if response == 'breakout': break                    
                    print(str_color('[Server]: ', Color.YELLOW.value) + response)
        else:
            print(str_color('[Error]: You need to connect to the server first!', Color.RED.value))
        return 3

    exit_pattern = r'^\s*exit\s*$'
    match_exit = re.match(exit_pattern, command)
    if match_exit != None:
        if CONNECTED_TO_SERVER == True:
            print('\n' + str_color('Closing Server...', Color.GREEN.value))
            
            send_one_message(client_socket, 'kill yourself')

        print('\n' + str_color('Have a great Day!', Color.GREEN.value) + '\n')

        client_socket.close()
        return 0


def start_application():
    print_header()

    status_code = -1
    while (status_code != 0):
        print(str_color('[Input]:', Color.GREEN.value), end=' ')
        command = input()
        status_code = client_parse_command(command)


if __name__ == "__main__":
    start_application()
