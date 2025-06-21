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
        parser.add_argument('--replicaof', type=str,
                            help='Replicaof host and port as "HOST PORT" (if set, role is slave)')
    
        return parser.parse_args()
    
    def initialize(self, args=None):
        """Initialize server configuration from command line args"""
        if args is None:
            args = self.parse_args()
    
        # Store the data directory and filename
        data_dir = args.dir
        db_filename = args.dbfilename
        port = args.port
        replicaof = args.replicaof

        # Determine role and replica host/port
        if replicaof:
            role = 'slave'
            # Parse "HOST PORT" string into separate host and port
            try:
                replica_host, replica_port = replicaof.split()
                replica_port = int(replica_port)  # Convert port to integer
            except ValueError:
                print(f"Error: --replicaof must be in format 'HOST PORT' (got: {replicaof})")
                replica_host, replica_port = None, None
        else:
            role = 'master'
            replica_host, replica_port = None, None

        print(f" port: {port}")
    
        # Full path to the database file
        db_path = os.path.join(data_dir, db_filename)

        self.config = {
            'dir': data_dir,
            'dbfilename': db_filename,
            'dbpath': db_path,
            'port': port,
            'role': role,
            'replica_host': replica_host,
            'replica_port': replica_port,
        }
    
        print(f"Redis server starting. Data will be stored at: {db_path}")
        print(f"Server will listen on port: {port}")
        print(f"Server role: {role}")
        if replicaof:
            print(f"Replica of: {replica_host}:{replica_port}")
        return self.config 