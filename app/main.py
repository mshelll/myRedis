import os
import socket  # noqa: F401
import threading
from datetime import datetime, timezone
import argparse

class RedisServer:
    """Main Redis server class that manages configuration and server lifecycle"""

    def __init__(self):
        self.config = {}
        self.cache = {}
        self.CRLF = '\r\n'  # CarriageReturn \r followed by LineFeed \n
        self.command_handler = RedisCommandHandler(self)
    
    def initialize(self, args=None):
        """Initialize server configuration from command line args"""
        if args is None:
            args = self.parse_args()
    
        # Store the data directory and filename
        data_dir = args.dir
        db_filename = args.dbfilename
    
        # Full path to the database file
        db_path = os.path.join(data_dir, db_filename)

        self.config = {
            'dir': data_dir,
            'dbfilename': db_filename,
            'dbpath': db_path,
        }
    
        print(f"Redis server starting. Data will be stored at: {db_path}")
    
        # Initialize cache from RDB file if it exists
        self.load_cache_from_rdb()

    def load_cache_from_rdb(self):
        """Load keys and values from RDB file into cache"""
        try:
            if not os.path.exists(self.config['dbpath']):
                print(f"RDB file does not exist: {self.config['dbpath']}")
                return
            
            keys_values = self.read_keys_from_rdb()
            if keys_values:
                for key, value in keys_values:
                    # Store with no expiry (0)
                    self.cache[key] = (value, 0)
                print(f"Loaded {len(keys_values)} keys from RDB file")
        except Exception as e:
            print(f"Error loading cache from RDB: {e}")

    def read_keys_from_rdb(self):
        """Read keys and values from RDB file
    
        Returns:
            list: List of (key, value) tuples
        """
        try:
            fd = open(self.config['dbpath'], 'rb')  # Open in binary mode
            content = fd.read()
            fd.close()
        
            # Find DB section (between FB and FF markers)
            start_marker = b'\xfb'
            end_marker = b'\xff'
        
            start_idx = content.find(start_marker)
            end_idx = content.find(end_marker, start_idx) if start_idx != -1 else -1
        
            if start_idx == -1 or end_idx == -1:
                print("Could not find database section in RDB file")
                return []

            db_section = content[start_idx+1:end_idx]
        
            # Basic format validation
            if len(db_section) < 3:
                print("Database section too small")
                return []

            # Skip metadata bytes
            nkeys, nexpiry, encoding = db_section[0], db_section[1], db_section[2]
        
            keys_values = []
            i = 3
            while i < len(db_section):
                try:
                    # Read key
                    key_len = db_section[i]
                    i += 1
                    if i + key_len > len(db_section):
                        print(f"Key length {key_len} exceeds remaining bytes")
                        break
                
                    key = db_section[i:i+key_len].decode('utf-8', errors='replace')
                    i += key_len
                
                    # Read value
                    if i >= len(db_section):
                        print("Unexpected end of data while reading value length")
                        break
                    
                    val_len = db_section[i]
                    i += 1
                    if i + val_len > len(db_section):
                        print(f"Value length {val_len} exceeds remaining bytes")
                        break
                    
                    value = db_section[i:i+val_len].decode('utf-8', errors='replace')
                    i += val_len
                
                    keys_values.append((key, value))
                except Exception as e:
                    print(f"Error parsing key-value pair: {e}")
                    break
        
            return keys_values
        
        except Exception as e:
            print(f"Error reading keys from RDB file: {e}")
            return []

    @staticmethod
    def parse_args():
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(description='Redis server implementation')
        parser.add_argument('--dir', type=str, default='/tmp',
                            help='Directory for Redis data files (default: /tmp)')
        parser.add_argument('--dbfilename', type=str, default='dump.rdb',
                            help='Filename for the Redis database (default: dump.rdb)')
    
        return parser.parse_args()

    def start(self):
        """Start the Redis server and listen for connections"""
        print("Logs from your program will appear here!")

        # Create server socket
        server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

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


class ClientHandler:
    """Handles client connections and processes commands"""
    
    def __init__(self, connection, server):
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
                        self.connection.sendall(b'-ERR invalid command format\r\n')
                        continue
                        
                    cmd = elems[2].lower()
                    print(f'{cmd=}')
                    
                    resp = self.server.command_handler.process_command(cmd, elems[2:])
                    self.connection.sendall(resp)
                except UnicodeDecodeError:
                    self.connection.sendall(b'-ERR invalid encoding\r\n')
                except Exception as e:
                    print(f"Command processing error: {e}")
                    self.connection.sendall(b'-ERR internal server error\r\n')

        except Exception as e:
            print(f"Connection error: {e}")
        finally:
            # Clean up the connection
            self.connection.close()


class RedisCommandHandler:
    """Handles Redis commands processing"""
    
    def __init__(self, server):
        self.server = server
        # Map of command names to their handler methods
        self.cmd_handlers = {
            'echo': self.handle_echo,
            'ping': self.handle_ping,
            'set': self.handle_set,
            'get': self.handle_get,
            'keys': self.handle_keys,
            'config': self.handle_config,
        }
    
    def process_command(self, cmd, args):
        """Process a command and return the response"""
        handler = self.cmd_handlers.get(cmd)
        if not handler:
            return b'-ERR unknown command\r\n'
        return handler(args)
    
    def handle_keys(self, elems):
        """Handle KEYS command - retrieve keys from RDB file"""
        try:
            fd = open(self.server.config['dbpath'], 'rb')  # Open in binary mode
            content = fd.read()
            fd.close()
            
            print(f"content={content!r}")
            
            # Find DB section (between FB and FF markers)
            start_marker = b'\xfb'
            end_marker = b'\xff'
            
            start_idx = content.find(start_marker)
            end_idx = content.find(end_marker, start_idx) if start_idx != -1 else -1
            
            print(f"db section {start_idx} {end_idx} {content[start_idx+1:end_idx] if start_idx != -1 and end_idx != -1 else b''}")
            print("Find DB section Its between fb and ff")

            db_section = content[start_idx+1:end_idx]

            nkeys, nexpiry, encoding = db_section[0], db_section[1], db_section[2]

            keys = []
            i = 3
            while i < len(db_section):
                key_len = db_section[i]
                print(f'{key_len=}')
                key = db_section[i+1:i+key_len+1]
                print(f'{key=}')
                i += key_len+1
                val_len = db_section[i]
                print(f'{val_len=}')
                val = db_section[i+1:i+val_len+1]
                print(f'{val=}')
                keys += [key.decode()]
                i += val_len+1

            resp = f'*1{self.server.CRLF}${len(keys[0])}{self.server.CRLF}{keys[0]}{self.server.CRLF}'

            return resp.encode()
            
        except Exception as e:
            print(f"Error reading database file: {e}")
            return b'-ERR Failed to read database file\r\n'
    
    def handle_ping(self, elems):
        """Handle PING command"""
        return b'+PONG\r\n'
    
    def handle_echo(self, elems):
        """Handle ECHO command"""
        inp = elems[-1]
        return b'+' + inp.encode('utf-8') + b'\r\n'
    
    def handle_set(self, elems):
        """Handle SET command - store a key-value pair with optional expiry"""
        print(f'set {elems=}')
        key = elems[2]
        value = elems[4]

        expiry = 0
        if len(elems) > 5 and elems[6] == 'px':
            px = int(elems[8])
            # Calculate expiry in milliseconds
            expiry = datetime.now(timezone.utc).timestamp() + (px / 1000.0)
            print(f'expiry {expiry}')

        self.server.cache[key] = (value, expiry)
        return b'+OK\r\n'
    
    def handle_get(self, elems):
        """Handle GET command - retrieve a value by key"""
        print(f'get {elems=}')
        key = elems[-1]
        
        if key not in self.server.cache:
            resp = '$-1\r\n'
            return resp.encode('utf-8')
            
        val, expiry = self.server.cache.get(key, (None, 0))
        current_time = datetime.now(timezone.utc).timestamp()
        print(f'get expiry {expiry} {current_time}')
        
        # If expiry is set (not 0) and current time is greater than expiry
        if expiry and current_time > expiry:
            del self.server.cache[key]
            resp = '$-1\r\n'
            return resp.encode('utf-8')

        if val:
            resp = f'${len(val)}{self.server.CRLF}{val}{self.server.CRLF}'
        else:
            resp = '$-1\r\n'

        print(resp)
        return resp.encode('utf-8')
    
    def handle_config(self, elems):
        """Handle CONFIG command - get or set server configuration"""
        print(f'config {elems=}')
        opr = elems[2]
        if opr.lower() == 'get':
            print('get config')
            key = elems[-1]
            val = self.server.config.get(key, '')

            if val:
                resp = f'*2{self.server.CRLF}${len(key)}{self.server.CRLF}{key}{self.server.CRLF}${len(val)}{self.server.CRLF}{val}{self.server.CRLF}'
            else:
                resp = '$-1\r\n'

            print(resp.encode('utf-8'))
            return resp.encode('utf-8')
        
        # Default response for unknown config operations
        return b'-ERR unknown config operation\r\n'


def main():
    """Main entry point"""
    print("Inside main")
    server = RedisServer()
    server.initialize()
    server.start()


if __name__ == "__main__":
    print("Inside main")
    main()