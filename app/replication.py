import socket
import random
from typing import List


class Replication:
    """Handles Redis replication functionality including info storage and replica management"""
    
    def __init__(self, role: str = 'master', connected_slaves: int = 0, master_replid: str = None, master_repl_offset: int = 0):
        self.role = role
        self.connected_slaves = connected_slaves
        self.master_replid = master_replid or self.generate_replid()
        self.master_repl_offset = master_repl_offset
        # Track replica connections
        self.replica_connections: List[socket.socket] = []

    @staticmethod
    def generate_replid():
        """Generate a random 40-character hex string for replication ID"""
        return ''.join(random.choices('0123456789abcdef', k=40))

    def add_replica_connection(self, connection: socket.socket):
        """Add a replica connection to the tracking list"""
        if self.role == 'master':
            self.replica_connections.append(connection)
            self.connected_slaves = len(self.replica_connections)
            print(f"Added replica connection. Total replicas: {self.connected_slaves}")

    def propagate_to_replicas(self, payload: bytes):
        """Propagate a command payload to all connected replicas"""
        if self.role != 'master':
            return
            
        # Send to all tracked replica connections
        for replica_conn in self.replica_connections:
            try:
                replica_conn.sendall(payload)
                print(f"Propagated command to replica connection")
            except Exception as e:
                print(f"Failed to propagate to replica connection: {e}")
                # Remove failed connection
                self.replica_connections.remove(replica_conn)
                self.connected_slaves = len(self.replica_connections)

    def get_info_lines(self) -> List[str]:
        """Get replication info lines for INFO command"""
        return [
            f'role:{self.role}',
            f'connected_slaves:{self.connected_slaves}',
            f'master_replid:{self.master_replid}',
            f'master_repl_offset:{self.master_repl_offset}'
        ] 