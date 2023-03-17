# Official client side of DBMSName
# ToDo: 
#       Create a window application that has:
#           - "Execute" Button
#           - Textbox for inputs
#           - Textbox for outputs
#       Socket comm handling
#       Server handshake check and error output if needed

from socket import *
import tkinter as tk

HOST = '127.0.0.1'
PORT = 6969     # nice

def send_message_to_server(message):
    message = message.encode()
    with socket(AF_INET, SOCK_STREAM) as client_socket:
        client_socket.connect(HOST,PORT)
        client_socket.send(message)

        response = client_socket.recv(1024)
        if response.decode() == "error":
            # print to console part of app
            print("kutyak")
        else:
            print("macskak")

class GUIWindow:
    def __init__(self):
        self.window = tk.Tk()
        
        self.input_label = tk.Label(text = "SQL text goes here")
        self.input_label.pack()

        self.input_box = tk.Text(width = 50, height = 15)
        self.input_box.pack()

        self.output_label = tk.Label(text="Console Output:")
        self.output_label.pack()

        # Add the output box
        self.output_box = tk.Text(height=5, width=50)
        self.output_box.pack()

        # Add the Execute button
        self.execute_button = tk.Button(text="Execute", command=self.execute)
        self.execute_button.pack()

    def execute(self):
        self.output_box.insert(tk.END, self.input_box.get() + "\n")

    def start(self):
        self.window.mainloop()

if __name__ == "__main__":
    gui = GUIWindow()
    gui.start()