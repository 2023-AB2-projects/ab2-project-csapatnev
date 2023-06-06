# Official client side of DBMSName

import socket as sck
import re
import subprocess
import sys
import os.path
import rich

from prompt.cli_prompts import cli_prompt, sql_prompt

from enum import Enum
from protocol.simple_protocol import *

from rich.tree import Tree
from rich.console import Console
from rich.table import Table


HOST = '127.0.0.1'
PORT = 6969     # nice

CONNECTED_TO_SERVER = False

client_socket = sck.socket(sck.AF_INET, sck.SOCK_STREAM)


class Color(Enum):
    RED = 'red'
    GREEN = 'green'
    YELLOW = 'yellow'
    BLUE = 'blue'
    LIGHT = 'light'


console_colors = {
    'red': '\x1b[0;31;3m',
    'green': '\x1b[0;32;3m',
    'yellow': '\x1b[0;33;3m',
    'blue': '\x1b[0;34;3m',
    'light': '\x1b[0;37;3m',
    'end': '\x1b[0m',
}


def str_color(string, color):
    if color in console_colors.keys():
        return console_colors[color] + string + console_colors['end']
    else:
        return string 


def print_commands():
    print(str_color('Commands:', Color.GREEN.value))
    print('\tconnect\t\t\t' + str_color('Connect to server', Color.BLUE.value))
    print('\trun [FILENAME]\t\t' + str_color('Run your SQL code from file', Color.BLUE.value))
    print('\tconsole\t\t\t' + str_color('Allows you to write SQL code in console', Color.BLUE.value))
    print('\ttree\t\t\t' + str_color('View stored databases', Color.BLUE.value))
    print('\tcommands\t\t' + str_color('Show commands', Color.BLUE.value))
    print('\texit\t\t\t' + str_color('Exit DBMSName', Color.BLUE.value))
    print()


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
        if not CONNECTED_TO_SERVER:
            print(str_color('[Error]: You need to connect to the server first!', Color.RED.value))
            return 2
        
        if not os.path.isfile(match_run.group(1)):
            print(str_color('[Error]: Cannot open File! ', Color.RED.value) + match_run.group(1))
            return 2
        
        f_input = open(match_run.group(1), "r")
        request = f_input.read()
        
        send_one_message(client_socket, 'run')
        send_one_message(client_socket, request)

        while True:
            response = recv_one_message(client_socket).decode()
            
            if response == 'breakout': break                    
            
            if response == 'select':
                response = recv_one_message(client_socket).decode()
                table_to_print = eval(response)

                configure_item = None
                if len(table_to_print) >= 1: 
                    configure_item = table_to_print[0]
                else:
                    print('Houston, we have a problem...')
                    send_one_message(client_socket, 'kill yourself')
                    return -1

                table = Table(show_lines=True, header_style='yellow')
                table.add_column('No.', style='yellow')
            
                for columns in configure_item.keys():
                    if columns == None:
                        table.add_column('', no_wrap=True)
                    else:
                        table.add_column(columns, no_wrap=True)

                row_counter = 0
                for row_item in table_to_print:
                    row_counter += 1

                    row = [str(row_counter) + '.'] + [str(value) for _, value in list(row_item.items())]
                    table.add_row(*row)

                console = Console()
                console.print(table)
            else:
                print('\n' + str_color('[Server]: ', Color.YELLOW.value) + response)

        print()

        return 2
            
    console_pattern = r'^\s*console\s*$'
    match_console = re.match(console_pattern, command)
    if match_console != None:
        if not CONNECTED_TO_SERVER:
            print(str_color('[Error]: You need to connect to the server first!', Color.RED.value))
            return 3
        
        send_one_message(client_socket, 'sql_prompt_data')
        response = recv_one_message(client_socket).decode()
        prompt_info = eval(response)
        
        db_in_use = prompt_info['db_in_use'].upper()
        databases = prompt_info['databases']
        tables = prompt_info['tables']

        tables_with_columns_in_database = {}
        tables_in_database = []

        if db_in_use != 'MASTER':
            tables_in_database = databases[db_in_use]
            tables_with_columns_in_database = {}
            for table in tables_in_database:
                tables_with_columns_in_database[table] = tables.get(table)

        console_input = ''
        request = ''

        while console_input != '<':
            console_input = sql_prompt(None, databases.keys(), tables_in_database, tables_with_columns_in_database)

            use_pattern = r'^use\s+(\w+)\s*;?.*?$'
            match = re.match(use_pattern, console_input, flags=re.IGNORECASE)
            if match:
                db_in_use = match.group(1)
                if db_in_use in databases.keys():
                    tables_in_database = databases[db_in_use]
                    tables_with_columns_in_database = {}
                    for table in tables_in_database:
                        tables_with_columns_in_database[table] = tables.get(table)
                else:
                    tables_with_columns_in_database = {}
                    tables_in_database = []

            if console_input != '<':
                request += ' ' + console_input
        
        send_one_message(client_socket, 'console')
        send_one_message(client_socket, request)

        while True:
            response = recv_one_message(client_socket).decode()
            
            if response == 'breakout': break                    
            
            if response == 'select':
                response = recv_one_message(client_socket).decode()
                table_to_print = eval(response)

                configure_item = None
                if len(table_to_print) >= 1: 
                    configure_item = table_to_print[0]
                else:
                    print('Houston, we have a problem...')
                    send_one_message(client_socket, 'kill yourself')
                    return -1

                table = Table(show_lines=True, header_style='yellow')
                table.add_column('No.', style='yellow')
            
                for columns in configure_item.keys():
                    if columns == None:
                        table.add_column('', no_wrap=True)
                    else:
                        table.add_column(columns, no_wrap=True)

                row_counter = 0
                for row_item in table_to_print:
                    row_counter += 1

                    row = [str(row_counter) + '.'] + [str(value) for _, value in list(row_item.items())]
                    table.add_row(*row)

                console = Console()
                console.print(table)
            else:
                print('\n' + str_color('[Server]: ', Color.YELLOW.value) + response)

        print()
        return 3  

    tree_pattern = r'^\s*tree\s*$'
    match_tree = re.match(tree_pattern, command)
    if match_tree != None:
        if not CONNECTED_TO_SERVER:
            print(str_color('[Error]: You need to connect to the server first!', Color.RED.value))
            return 4
        
        send_one_message(client_socket, 'tree')

        while True:
            response = recv_one_message(client_socket).decode()
            if response == 'breakout': break                    
            
            tree = Tree(str_color('<Tree>', Color.GREEN.value))
            
            tree_to_print = eval(response)
            
            for database in tree_to_print.keys():
                database_tree = Tree(str_color('<Database> ', Color.BLUE.value) + str_color(database, Color.LIGHT.value))
                for table in tree_to_print[database]:
                    table_tree = Tree(str_color('<Table> ', Color.YELLOW.value) + str_color(list(table.keys())[0] , Color.LIGHT.value))
                    for attribute in table[list(table.keys())[0]]:
                        table_tree.add(str_color('<Attribute> ', Color.GREEN.value) + str_color(attribute, Color.LIGHT.value))
                    database_tree.add(table_tree)
                tree.add(database_tree)

            print()
            rich.print(tree)
            print()

        return 4
    
    commands_pattern = r'^\s*commands\s*$'
    match_commands = re.match(commands_pattern, command)
    if match_commands != None:
        print()
        print_commands()
        print()

        return 5

    exit_pattern = r'^\s*exit\s*$'
    match_exit = re.match(exit_pattern, command)
    if match_exit != None:
        if CONNECTED_TO_SERVER:
            print('\n' + str_color('Closing Server...', Color.GREEN.value))
            send_one_message(client_socket, 'kill yourself')

        print('\n' + str_color('Have a great Day!', Color.GREEN.value) + '\n')

        client_socket.close()
       
        return 0


def start_application():
    print(str_color('Welcome to DBMSName!\n', Color.GREEN.value))
    print_commands()

    status_code = -1
    while (status_code != 0):
        command = cli_prompt()
        status_code = client_parse_command(command)


if __name__ == "__main__":
    start_application()
