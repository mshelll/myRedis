import socket
import threading
from typing import Dict, Any

from .config import ServerConfig
from .rdb_parser import RDBParser
from .command_handler import RedisCommandHandler
from .client_handler import ClientHandler


class RedisServer:
    """Main Redis server class that manages configuration and server lifecycle"""

    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.cache: Dict[str, tuple] = {}
        self.config_manager = ServerConfig()
        self.command_handler = RedisCommandHandler(self)
    
    def initialize(self, args=None):
        """Initialize server configuration from command line args"""
        self.config = self.config_manager.initialize(args)
        
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

    def start(self):
        """Start the Redis server and listen for connections"""
        print("Logs from your program will appear here!")

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