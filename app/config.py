import os
import argparse
from typing import Dict, Any
from .constants import DEFAULT_PORT


class ServerConfig:
    """Handles server configuration and argument parsing"""
    
    def __init__(self):
        self.config: Dict[str, Any] = {}
    
    def parse_args(self):
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(description='Redis server implementation')
        parser.add_argument('--dir', type=str, default='/tmp',
                            help='Directory for Redis data files (default: /tmp)')
        parser.add_argument('--dbfilename', type=str, default='dump.rdb',
                            help='Filename for the Redis database (default: dump.rdb)')
        parser.add_argument('--port', type=int, default=DEFAULT_PORT,
                            help=f'Port to listen on (default: {DEFAULT_PORT})')
    
        return parser.parse_args()
    
    def initialize(self, args=None):
        """Initialize server configuration from command line args"""
        if args is None:
            args = self.parse_args()
    
        # Store the data directory and filename
        data_dir = args.dir
        db_filename = args.dbfilename
        port = args.port

        print(f" port: {port}")
    
        # Full path to the database file
        db_path = os.path.join(data_dir, db_filename)

        self.config = {
            'dir': data_dir,
            'dbfilename': db_filename,
            'dbpath': db_path,
            'port': port,
        }
    
        print(f"Redis server starting. Data will be stored at: {db_path}")
        print(f"Server will listen on port: {port}")
        return self.config 