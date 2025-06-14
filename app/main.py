import os
import socket  # noqa: F401
import threading
from datetime import datetime, timezone
import argparse

# Global constants
CRLF = '\r\n'  # CarriageReturn \r followed by LineFeed \n


class RedisServer:
    """Main Redis server class that manages configuration and server lifecycle"""

    def __init__(self):
        self.config = {}
        self.cache = {}
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
                for key, value, expiry in keys_values:
                    if key:  # Skip empty keys
                        # Store with no expiry (0)
                        self.cache[key] = (value, expiry)
                print(f"Loaded {len(self.cache)} keys from RDB file")
        except Exception as e:
            print(f"Error loading cache from RDB: {e}")

    def read_keys_from_rdb(self):
        """Read keys and values from RDB file
    
        Returns:
            list: List of (key, value, expiry) tuples
        """
        try:
            with open(self.config['dbpath'], 'rb') as fd:
                content = fd.read()
            
            # Find DB section (between FB and FF markers)
            start_marker = b'\xfb'
            end_marker = b'\xff'
            
            start_idx = content.find(start_marker)
            end_idx = content.find(end_marker, start_idx) if start_idx != -1 else -1
            
            if start_idx == -1 or end_idx == -1:
                print("Could not find database section in RDB file")
                return []
            
            # Extract DB section (skip FB marker but include everything up to FF marker)
            db_section = content[start_idx+1:end_idx]
            
            # First byte represents the number of keys
            num_keys = db_section[0]
            print(f"RDB header indicates {num_keys} keys")
            
            print(f"DB section length: {len(db_section)} bytes")
            print(f"DB section hex: {db_section.hex()}")
            print(f"DB section : {db_section}")
            
            # Parse key-value pairs
            keys_values = []
            i = 1  # Start after the key count byte
            
            while i < len(db_section):
                try:
                    # Check for the expiry marker (0xFC)
                    if i < len(db_section) and db_section[i] == 0xFC:
                        print(f"Current byte: 0x{db_section[i]:x}")
                        i += 1  # Skip the FC marker
                        
                        # Parse expiry timestamp (milliseconds since epoch)
                        # Extract a 4-byte timestamp from the next 8 bytes
                        expiry_bytes = db_section[i:i+8]
                        # Use bytes 1-4 (little endian) which seems to contain the actual timestamp
                        # The byte at index 0 is likely a type flag
                        expiry_ms = int.from_bytes(expiry_bytes[1:5], byteorder='little')
                        
                        # Convert to seconds for easier comparison with current time
                        # Redis stores expiry as milliseconds since epoch
                        expiry = expiry_ms / 1000.0
                        i += 8
                        
                        print(f"expiry={expiry}")
                    else:
                        # No expiry
                        expiry = 0
                    
                    # Get key length
                    if i >= len(db_section):
                        break
                    key_len = db_section[i]
                    i += 1
                    
                    # Skip zero-length keys
                    if key_len == 0:
                        continue
                    
                    # Extract key
                    if i + key_len > len(db_section):
                        break
                    key = db_section[i:i+key_len].decode('utf-8', errors='replace')
                    i += key_len
                    
                    # Get value length
                    if i >= len(db_section):
                        break
                    val_len = db_section[i]
                    i += 1
                    
                    # Extract value
                    if i + val_len > len(db_section):
                        break
                    value = db_section[i:i+val_len].decode('utf-8', errors='replace')
                    i += val_len
                    
                    print(f"Extracted key='{key}', value='{value}' expiry={expiry}")
                    keys_values.append((key, value, expiry))
                    
                except Exception as e:
                    print(f"Error parsing key-value pair at position {i}: {e}")
                    i += 1  # Skip this byte and continue
            
            print(f"Successfully extracted {len(keys_values)} key-value pairs: {keys_values}")
            return keys_values
            
        except Exception as e:
            print(f"Error reading RDB file: {e}")
            import traceback
            traceback.print_exc()
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
                        self.connection.sendall(b'-ERR invalid command format' + CRLF.encode())
                        continue
                        
                    cmd = elems[2].lower()
                    print(f'{cmd=}')
                    
                    resp = self.server.command_handler.process_command(cmd, elems[2:])
                    self.connection.sendall(resp)
                except UnicodeDecodeError:
                    self.connection.sendall(b'-ERR invalid encoding' + CRLF.encode())
                except Exception as e:
                    print(f"Command processing error: {e}")
                    self.connection.sendall(b'-ERR internal server error' + CRLF.encode())

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
            return f'-ERR unknown command{CRLF}'.encode()
        return handler(args)

    def handle_keys(self, elems):
        """
        Handle KEYS command - return keys from the cache that was loaded from RDB
        
        This implementation:
        1. Uses the server's cache directly rather than re-reading the RDB file
        2. Properly supports returning multiple keys
        3. Handles pattern matching if implemented in the future
        """
        try:
            # Get all keys from the server's cache
            keys = list(self.server.cache.keys())
            print(f'keys={keys}')
            print(f" {self.server.cache}")
            
            # Filter out empty keys
            keys = [k for k in keys if k]
            
            if not keys:
                # Return empty array if no keys found
                return f'*0{CRLF}'.encode()
            
            # Format response with all keys according to Redis protocol
            # Start with array length
            resp = f'*{len(keys)}{CRLF}'
            
            # Add each key as a bulk string
            for key in keys:
                resp += f'${len(key)}{CRLF}{key}{CRLF}'
            
            print(f"Returning {len(keys)} keys")
            return resp.encode('utf-8')
            
        except Exception as e:
            print(f"Error handling KEYS command: {e}")
            return f'-ERR internal error{CRLF}'.encode()

    def handle_ping(self, elems):
        """Handle PING command"""
        return f'+PONG{CRLF}'.encode()
    
    def handle_echo(self, elems):
        """Handle ECHO command"""
        inp = elems[-1]
        return f'+{inp}{CRLF}'.encode()
    
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
        return f'+OK{CRLF}'.encode()

    def handle_get(self, elems):
        """Handle GET command - retrieve a value by key"""
        print(f'get {elems=}')
        key = elems[-1]
        
        if key not in self.server.cache:
            resp = f'$-1{CRLF}'
            return resp.encode('utf-8')
            
        val, expiry = self.server.cache.get(key, (None, 0))
        current_time = int(datetime.now(timezone.utc).timestamp())
        print(f'get expiry {expiry=} {current_time=}')
        
        # If expiry is set (not 0) and current time is greater than expiry
        if expiry and current_time > expiry:
            del self.server.cache[key]
            resp = f'$-1{CRLF}'
            return resp.encode('utf-8')

        if val:
            resp = f'${len(val)}{CRLF}{val}{CRLF}'
        else:
            resp = f'$-1{CRLF}'

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
                resp = f'*2{CRLF}${len(key)}{CRLF}{key}{CRLF}${len(val)}{CRLF}{val}{CRLF}'
            else:
                resp = f'$-1{CRLF}'

            print(resp.encode('utf-8'))
            return resp.encode('utf-8')
        
        # Default response for unknown config operations
        return f'-ERR unknown config operation{CRLF}'.encode()


def main():
    """Main entry point"""
    print("Inside main")
    server = RedisServer()
    server.initialize()
    server.start()


if __name__ == "__main__":
    main()
