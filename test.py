import socket

def send_large_data():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_host = '127.0.0.1' 
    server_port = 6969  

    client_socket.connect((server_host, server_port))
    print("Connected to server {}:{}".format(server_host, server_port))

    data = "This is a large data that needs to be sent to the server." * 1000000
    client_socket.sendall(data.encode())

    client_socket.close()

send_large_data()
