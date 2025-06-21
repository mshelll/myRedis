import socket
import threading
from typing import Dict, Any
import random

from .config import ServerConfig
from .rdb_parser import RDBParser
from .command_handler import RedisCommandHandler
from .client_handler import ClientHandler

class ReplicationInfo:
    def __init__(self, role: str = 'master', connected_slaves: int = 0, master_replid: str = None, master_repl_offset: int = 0):
        self.role = role
        self.connected_slaves = connected_slaves
        self.master_replid = master_replid or self.generate_replid()
        self.master_repl_offset = master_repl_offset

    @staticmethod
    def generate_replid():
        # Generate a random 40-character hex string
        return ''.join(random.choices('0123456789abcdef', k=40))

class RedisServer:
    """Main Redis server class that manages configuration and server lifecycle"""

    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.cache: Dict[str, tuple] = {}
        self.config_manager = ServerConfig()
        self.command_handler = RedisCommandHandler(self)
        self.replication_info = None
    
    def initialize(self, args=None):
        """Initialize server configuration from command line args"""
        self.config = self.config_manager.initialize(args)
        # Set up replication info
        self.replication_info = ReplicationInfo(
            role=self.config.get('role', 'master'),
            connected_slaves=0,
            master_replid=None,
            master_repl_offset=0
        )
        # Initialize cache from RDB file if it exists
        self.load_cache_from_rdb()

    def load_cache_from_rdb(self):
        """Load keys and values from RDB file into cache"""
        try:
            rdb_parser = RDBParser(self.config['dbpath'])
            keys_values = rdb_parser.load_keys_from_rdb()
            
            if keys_values:
                for key, value, expiry in keys_values:
                    if key:  # Skip empty keys
                        # Store with no expiry (0)
                        self.cache[key] = (value, expiry)
        except Exception as e:
            print(f"Error loading cache from RDB: {e}")

    def send_ping_to_master(self):
        """If this server is a slave, connect to the master and send a PING command."""
        if self.config.get('role') != 'slave':
            return
        host = self.config.get('replica_host')
        port = self.config.get('replica_port')
        if not host or not port:
            print("Replica host/port not set, cannot ping master.")
            return
        try:
            with socket.create_connection((host, port), timeout=2) as sock:
                # Send RESP2 PING command: *1\r\n$4\r\nPING\r\n
                ping_cmd = b'*1\r\n$4\r\nPING\r\n'
                sock.sendall(ping_cmd)
                resp = sock.recv(1024)
                print(f"Pinged master at {host}:{port}, got response: {resp!r}")
        except Exception as e:
            print(f"Failed to ping master at {host}:{port}: {e}")

    def start(self):
        """Start the Redis server and listen for connections"""
        print("Logs from your program will appear here!")

        # Ping master if this is a slave
        self.send_ping_to_master()

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