import socket
import threading
from typing import Dict, Any
import random

from .config import ServerConfig
from .rdb_parser import RDBParser
from .client_handler import ClientHandler
from .replication import Replication
from .storage import Storage
from .constants import CRLF

class RedisServer:
    """Main Redis server class that manages configuration and server lifecycle"""

    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.storage = Storage()
        self.config_manager = ServerConfig()
        self.replication = None
        self.master_connection = None  # Store connection to master for slaves
        self.replication_offset = 0  # Track replication offset for slave servers
        self.first_getack_sent = False  # Track if first GETACK has been sent
        self.pending_data = b''  # Store any remaining data for propagation handler
    
    def initialize(self, args=None):
        """Initialize server configuration from command line args"""
        self.config = self.config_manager.initialize(args)
        # Set up replication
        self.replication = Replication(
            role=self.config.get('role', 'master'),
            connected_slaves=0,
            master_replid=None,
            master_repl_offset=0
        )
        # Initialize cache from RDB file if it exists
        self.load_cache_from_rdb()

    def load_cache_from_rdb(self):
        """Load keys and values from RDB file into storage"""
        try:
            rdb_parser = RDBParser(self.config['dbpath'])
            keys_values = rdb_parser.load_keys_from_rdb()
            
            if keys_values:
                self.storage.load_from_rdb(keys_values)
        except Exception as e:
            print(f"Error loading cache from RDB: {e}")

    def server_to_master_handshake(self):
        """If this server is a slave, connect to the master and perform the handshake: PING, REPLCONF, and PSYNC commands."""
        if self.config.get('role') != 'slave':
            return
        host = self.config.get('replica_host')
        port = self.config.get('replica_port')
        my_port = self.config.get('port')
        if not host or not port:
            print("Replica host/port not set, cannot handshake with master.")
            return
        try:
            sock = socket.create_connection((host, port), timeout=2)
            # Send RESP2 PING command: *1{CRLF}$4{CRLF}PING{CRLF}
            ping_cmd = f'*1{CRLF}$4{CRLF}PING{CRLF}'.encode()
            sock.sendall(ping_cmd)
            resp = sock.recv(1024)
            print(f"Pinged master at {host}:{port}, got response: {resp!r}")

            # Send REPLCONF listening-port <PORT>
            replconf_port = (
                f"*3{CRLF}$8{CRLF}REPLCONF{CRLF}$14{CRLF}listening-port{CRLF}$" +
                f"{len(str(my_port))}{CRLF}{my_port}{CRLF}"
            ).encode()
            sock.sendall(replconf_port)
            resp2 = sock.recv(1024)
            print(f"Sent REPLCONF listening-port, got response: {resp2!r}")

            # Send REPLCONF capa psync2
            replconf_capa = f'*3{CRLF}$8{CRLF}REPLCONF{CRLF}$4{CRLF}capa{CRLF}$6{CRLF}psync2{CRLF}'.encode()
            sock.sendall(replconf_capa)
            resp3 = sock.recv(1024)
            print(f"Sent REPLCONF capa psync2, got response: {resp3!r}")

            # Send PSYNC ? -1 (always use ? and -1 for initial handshake)
            psync_cmd = (
                f"*3{CRLF}$5{CRLF}PSYNC{CRLF}$1{CRLF}?{CRLF}$2{CRLF}-1{CRLF}"
            ).encode()
            sock.sendall(psync_cmd)
            resp4 = sock.recv(1024)
            print(f"Sent PSYNC, got response: {resp4!r}")

            # After PSYNC, we need to read the RDB file
            if b'FULLRESYNC' in resp4:
                print("Received FULLRESYNC, reading RDB file...")
                
                # Check if RDB data is already in the response
                if b'$' in resp4 and b'REDIS' in resp4:
                    # RDB data is already in the response, extract it
                    # Find the start of RDB data (after the FULLRESYNC line)
                    fullresync_end = resp4.find(b'\r\n') + 2
                    rdb_start = resp4.find(b'$', fullresync_end)
                    
                    if rdb_start != -1:
                        # Extract RDB data and any remaining data
                        rdb_length_line_end = resp4.find(b'\r\n', rdb_start) + 2
                        rdb_length_str = resp4[rdb_start+1:rdb_length_line_end-2]
                        rdb_length = int(rdb_length_str)
                        
                        # Calculate where RDB data ends
                        rdb_data_end = rdb_length_line_end + rdb_length + 2  # +2 for \r\n after data
                        
                        # Extract RDB data
                        rdb_data = resp4[rdb_length_line_end:rdb_data_end-2]
                        print(f"RDB data extracted: {len(rdb_data)} bytes")
                        
                        # Save any remaining data for propagation handler
                        remaining_data = resp4[rdb_data_end:]
                        if remaining_data:
                            self.pending_data = remaining_data
                            print(f"Saved remaining data for propagation: {len(remaining_data)} bytes")
                            print(f"Remaining data: {remaining_data!r}")
                    else:
                        print("Unexpected response format")
                        return
                else:
                    # Need to read RDB file separately
                    # Read the RDB file length first
                    rdb_length_line = b''
                    while not rdb_length_line.endswith(b'\r\n'):
                        rdb_length_line += sock.recv(1)
                    
                    # Parse the length
                    rdb_length_str = rdb_length_line.decode().strip()[1:]  # Remove '$'
                    rdb_length = int(rdb_length_str)
                    print(f"RDB file length: {rdb_length}")
                    
                    # Read the RDB file content
                    rdb_data = b''
                    while len(rdb_data) < rdb_length:
                        chunk = sock.recv(min(1024, rdb_length - len(rdb_data)))
                        if not chunk:
                            break
                        rdb_data += chunk
                    
                    print(f"Read RDB file of {len(rdb_data)} bytes")
                
                # Now we need to keep the connection open to receive propagated commands
                # This connection should stay alive for the duration of the server
                self.master_connection = sock
                # Reset replication offset after RDB transfer
                self.replication_offset = 0
                self.first_getack_sent = False
                print("Master connection established and ready for command propagation")

        except Exception as e:
            print(f"Failed to handshake with master at {host}:{port}: {e}")

    def master_to_slave_handshake(self):
        """If this server is a master and has a replica configured, connect to the slave and perform the handshake: PING, REPLCONF, and REPLCONF capa psync2 commands."""
        if self.config.get('role') != 'master':
            return
        host = self.config.get('replica_host')
        port = self.config.get('replica_port')
        my_port = self.config.get('port')
        if not host or not port:
            print("Replica host/port not set, cannot handshake with slave.")
            return
        try:
            with socket.create_connection((host, port), timeout=2) as sock:
                # Send RESP2 PING command: *1{CRLF}$4{CRLF}PING{CRLF}
                ping_cmd = f'*1{CRLF}$4{CRLF}PING{CRLF}'.encode()
                sock.sendall(ping_cmd)
                resp = sock.recv(1024)
                print(f"Pinged slave at {host}:{port}, got response: {resp!r}")

                # Send REPLCONF listening-port <PORT>
                replconf_port = (
                    f"*3{CRLF}$8{CRLF}REPLCONF{CRLF}$14{CRLF}listening-port{CRLF}$" +
                    f"{len(str(my_port))}{CRLF}{my_port}{CRLF}"
                ).encode()
                sock.sendall(replconf_port)
                resp2 = sock.recv(1024)
                print(f"Sent REPLCONF listening-port to slave, got response: {resp2!r}")

                # Send REPLCONF capa psync2
                replconf_capa = f'*3{CRLF}$8{CRLF}REPLCONF{CRLF}$4{CRLF}capa{CRLF}$6{CRLF}psync2{CRLF}'.encode()
                sock.sendall(replconf_capa)
                resp3 = sock.recv(1024)
                print(f"Sent REPLCONF capa psync2 to slave, got response: {resp3!r}")

        except Exception as e:
            print(f"Failed to handshake with slave at {host}:{port}: {e}")

    def handle_master_propagation(self):
        """Handle propagated commands from master (for slave servers)"""
        if not self.master_connection or self.config.get('role') != 'slave':
            return
        
        try:
            # Start with any pending data from handshake
            buffer = getattr(self, 'pending_data', b'')
            if buffer:
                print(f"Processing pending data: {len(buffer)} bytes")
                print(f"Pending data: {buffer!r}")
                
                # Check if pending data is missing the array header
                if buffer.startswith(b'$') and b'REPLCONF' in buffer:
                    # This is likely a REPLCONF command missing the array header
                    # Reconstruct the complete message: *3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n
                    reconstructed_data = b'*3\r\n' + buffer
                    print(f"Reconstructed data: {reconstructed_data!r}")
                    buffer = reconstructed_data
                elif buffer.startswith(b'\r\n$') and b'REPLCONF' in buffer:
                    # This is a REPLCONF command missing the array header, with extra \r\n
                    # Reconstruct the complete message: *3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n
                    reconstructed_data = b'*3' + buffer
                    print(f"Reconstructed data: {reconstructed_data!r}")
                    buffer = reconstructed_data
                
                self.pending_data = b''  # Clear pending data
            
            while True:
                data = self.master_connection.recv(1024)
                if not data:
                    print("Master connection closed")
                    break
                
                buffer += data
                
                # Process complete commands from buffer
                while True:
                    complete_message = self.extract_complete_resp_message(buffer)
                    if not complete_message:
                        break  # Wait for more data
                    
                    # Check the message type
                    message_text = complete_message.decode('utf-8')
                    
                    if message_text.startswith('*'):
                        # Array message - this is a command
                        elems = self.parse_resp(message_text)
                        print(f"Parsed propagated elements: {elems}")
                        
                        # Apply the command (reuse command handler logic)
                        if elems and elems[0].lower() == 'set' and len(elems) >= 3:
                            key = elems[1]
                            value = elems[2]
                            self.storage.set(key, value)
                            print(f"Applied propagated SET: {key} = {value}")
                        elif elems and elems[0].lower() == 'replconf' and len(elems) >= 2:
                            subcommand = elems[1].upper()
                            print(f"Processing REPLCONF subcommand: {subcommand}")
                            if subcommand == 'GETACK':
                                # Respond with current replication offset
                                if not self.first_getack_sent:
                                    # First GETACK after RDB should return 0
                                    ack_response = f'*3{CRLF}$8{CRLF}REPLCONF{CRLF}$3{CRLF}ACK{CRLF}$1{CRLF}0{CRLF}'
                                    self.first_getack_sent = True
                                    print("Sent REPLCONF ACK 0 to master (first GETACK)")
                                else:
                                    # Subsequent GETACKs return current offset
                                    ack_response = f'*3{CRLF}$8{CRLF}REPLCONF{CRLF}$3{CRLF}ACK{CRLF}${len(str(self.replication_offset))}{CRLF}{self.replication_offset}{CRLF}'
                                    print(f"Sent REPLCONF ACK {self.replication_offset} to master")
                                print(f"Sending ACK response: {ack_response!r}")
                                self.master_connection.sendall(ack_response.encode('utf-8'))
                                print("ACK response sent successfully")
                        
                        # Increment replication offset by the length of this command
                        self.replication_offset += len(complete_message)
                        print(f"Updated replication offset to {self.replication_offset}")
                    elif message_text.startswith('$'):
                        # Bulk string message - this is RDB data, just consume it
                        print(f"Consumed RDB data: {len(complete_message)} bytes")
                    elif message_text.startswith('+') or message_text.startswith('-') or message_text.startswith(':'):
                        # Simple string, error, or integer message - just consume it
                        print(f"Consumed message: {message_text.strip()}")
                    
                    # Remove processed message from buffer
                    buffer = buffer[len(complete_message):]
                    
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error handling master propagation: {e}")
    
    def extract_complete_resp_message(self, buffer):
        """
        Extract one complete RESP (Redis Serialization Protocol) message from buffer.
        
        RESP Protocol Overview:
        - Messages start with a type indicator: *, $, +, -, :
        - Arrays start with * followed by count: *3\r\n
        - Bulk strings start with $ followed by length: $4\r\n
        - Simple strings start with +: +OK\r\n
        - Errors start with -: -ERR\r\n
        - Integers start with :: :1000\r\n
        
        Args:
            buffer (bytes): Raw bytes containing RESP data, may be partial
        
        Returns:
            bytes or None: Complete RESP message if found, None if incomplete
        """
        # Try to decode buffer as UTF-8 string for parsing
        try:
            text = buffer.decode('utf-8')
        except UnicodeDecodeError:
            # If we can't decode, the buffer is incomplete (partial UTF-8 sequence)
            return None
        
        if not text:
            return None
        
        # Handle different RESP types
        if text.startswith('*'):
            # Array message
            return self.extract_array_message(text)
        elif text.startswith('$'):
            # Bulk string message
            return self.extract_bulk_string_message(text)
        elif text.startswith('+') or text.startswith('-') or text.startswith(':'):
            # Simple string, error, or integer message
            return self.extract_simple_message(text)
        else:
            # Unknown message type
            return None
    
    def extract_array_message(self, text):
        """Extract complete array message"""
        # Find the first CRLF to extract the array header
        header_end = text.find('\r\n')
        if header_end == -1:
            return None
        
        try:
            array_count = int(text[1:header_end])
        except ValueError:
            return None
        
        lines = text.split('\r\n')
        expected_lines = 1 + array_count * 2  # header + (length + value) pairs
        
        if len(lines) < expected_lines:
            return None
        
        complete_lines = lines[:expected_lines]
        complete_message = '\r\n'.join(complete_lines) + '\r\n'
        return complete_message.encode()
    
    def extract_bulk_string_message(self, text):
        """Extract complete bulk string message"""
        # Find the first CRLF to get the length
        header_end = text.find('\r\n')
        if header_end == -1:
            return None
        
        try:
            length = int(text[1:header_end])
        except ValueError:
            return None
        
        # For bulk strings, we need: $<length>\r\n<data>\r\n
        # So we need 2 lines + the data
        lines = text.split('\r\n')
        if len(lines) < 2:
            return None
        
        # Check if we have enough data
        data_line = lines[1]
        if len(data_line) < length:
            return None
        
        # Extract the complete message
        complete_message = f'${length}\r\n{data_line[:length]}\r\n'
        return complete_message.encode()
    
    def extract_simple_message(self, text):
        """Extract complete simple string, error, or integer message"""
        # These messages end with \r\n
        if '\r\n' not in text:
            return None
        
        end_pos = text.find('\r\n') + 2
        complete_message = text[:end_pos]
        return complete_message.encode()

    def parse_resp(self, data: str) -> list:
        """Parse RESP (Redis Serialization Protocol) data - reused from client handler"""
        lines = data.strip().split('\r\n')
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

    def start(self):
        """Start the Redis server and listen for connections"""
        print("Logs from your program will appear here!")

        # Ping master if this is a slave
        self.server_to_master_handshake()
        
        # If this is a slave and we have a master connection, start propagation handler
        if self.config.get('role') == 'slave' and self.master_connection:
            propagation_thread = threading.Thread(target=self.handle_master_propagation)
            propagation_thread.daemon = True
            propagation_thread.start()
            print("Started master propagation handler thread")
        
        # If this is a master and has a replica configured, handshake with slave
        if self.config.get('role') == 'master' and self.config.get('replica_host') and self.config.get('replica_port'):
            self.master_to_slave_handshake()

        # Get port from configuration
        port = self.config.get('port', 6379)
        
        # Create server socket
        server_socket = socket.create_server(("localhost", port), reuse_port=True)

        try:
            while True:
                # Wait for a client to connect
                connection, client_address = server_socket.accept()
                print(f"New connection from {client_address}")

                # Create a new thread to handle this client
                client_handler = ClientHandler(connection, self)
                client_thread = threading.Thread(target=client_handler.handle)
                client_thread.daemon = True  # Set as daemon so it exits when main thread exits
                client_thread.start()

        except KeyboardInterrupt:
            print("Server shutting down")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(e)
        finally:
            server_socket.close() 