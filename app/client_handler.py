import socket
from typing import Any
from .constants import CRLF


class ClientHandler:
    """Handles client connections and processes commands"""
    
    def __init__(self, connection: socket.socket, server):
        self.connection = connection
        self.server = server
    
    def handle(self):
        """Process client connection and commands"""
        try:
            while True:
                # Read data from the client
                data = self.connection.recv(1024)  # Read up to 1024 bytes

                # If client closed connection or no data received, break the loop
                if not data:
                    print("Client disconnected")
                    break

                # Process the received data
                print(data)
                try:
                    data_str = data.decode('utf-8')
                    elems = data_str.split()
                    print(elems)
                    
                    if len(elems) < 3:
                        self.connection.sendall(b'-ERR invalid command format' + CRLF.encode())
                        continue
                        
                    cmd = elems[2].lower()
                    print(f'{cmd=}')
                    
                    # Pass connection to command handler - handlers now send responses directly
                    self.server.command_handler.process_command(cmd, elems[2:], self.connection)
                    
                except UnicodeDecodeError:
                    self.connection.sendall(b'-ERR invalid encoding' + CRLF.encode())
                except Exception as e:
                    import traceback 
                    traceback.print_exc()
                    print(f"Command processing error: {e}")
                    self.connection.sendall(b'-ERR internal server error' + CRLF.encode())

        except Exception as e:
            print(f"Connection error: {e}")
        finally:
            # Clean up the connection
            self.connection.close()