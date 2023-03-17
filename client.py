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
            gui.print_to_output_box("Error: server returned an error message")
        else:
            print("ok")


#############################################
#######        GUI USAGE:      ##############
#############################################
# gui = GUIWindow()     - creates a new gui window object
# gui.print_to_output_box("Message")    - prints to the output box
# text = gui.input_text     - text now contains the content of the input text box after
#                               execute had been pressed
#

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
        print("Exec button pressed")
        self.input_text = self.input_box.get("1.0", tk.END)
        
        send_message_to_server(self.input_text)
        
        return self.input_text

    def print_to_output_box(self, message):
        self.output_box.insert(tk.END, message)

    def start(self):
        self.window.mainloop()

if __name__ == "__main__":
    gui = GUIWindow()
    gui.start()
    