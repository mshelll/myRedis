import socket
from typing import Any
from .constants import CRLF
from .command_handler import RedisCommandHandler


class ClientHandler:
    """Handles client connections and processes commands"""
    
    def __init__(self, connection: socket.socket, server):
        self.connection = connection
        self.server = server
        # Create a command handler instance for this client
        self.command_handler = RedisCommandHandler(server, connection)
    
    def handle(self):
        """Process client connection and commands"""
        psync_handled = False
        try:
            while True:
                # Read data from the client
                data = self.connection.recv(1024)  # Read up to 1024 bytes

                # If client closed connection or no data received, break the loop
                if not data:
                    print("Client disconnected")
                    break

                # Print received data for debugging
                print(f"Received data: {data!r}")

                # Parse the RESP protocol
                elems = self.parse_resp(data.decode('utf-8'))
                print(f"Parsed elements: {elems}")

                if not elems:
                    continue

                # Extract command and arguments
                cmd = elems[0].lower()
                print(f"Command: {cmd}")

                # If this is a PSYNC command (master connection for replication), handle it and then break
                if cmd == 'psync':
                    self.command_handler.process_command(elems)
                    print("Detected PSYNC command, breaking client handler loop for replication stream.")
                    psync_handled = True
                    break

                # Process the command using the client's command handler
                self.command_handler.process_command(elems)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error handling client: {e!r}")
        finally:
            if not psync_handled:
                self.connection.close()

    def parse_resp(self, data: str) -> list:
        """Parse RESP (Redis Serialization Protocol) data"""
        lines = data.strip().split(CRLF)
        elems = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            if line.startswith('*'):
                # Array
                count = int(line[1:])
                i += 1
                for _ in range(count):
                    if i < len(lines) and lines[i].startswith('$'):
                        # Bulk string
                        length = int(lines[i][1:])
                        i += 1
                        if i < len(lines):
                            elems.append(lines[i])
                            i += 1
                    else:
                        # Simple string
                        if i < len(lines):
                            elems.append(lines[i])
                            i += 1
            elif line.startswith('$'):
                # Bulk string
                length = int(line[1:])
                i += 1
                if i < len(lines):
                    elems.append(lines[i])
                    i += 1
            else:
                # Simple string
                elems.append(line)
                i += 1
        
        return elems