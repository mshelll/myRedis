import socket
import random
import time
import threading
from typing import List, Dict, Set
from .constants import DEFAULT_REPLICATION_ID_LENGTH
import select


class Replication:
    """Handles Redis replication functionality including info storage and replica management"""
    
    def __init__(self, role: str = 'master', connected_slaves: int = 0, master_replid: str = None, master_repl_offset: int = 0):
        self.role = role
        self.connected_slaves = connected_slaves
        self.master_replid = master_replid or self.generate_replid()
        self.master_repl_offset = master_repl_offset
        # Track replica connections
        self.replica_connections: List[socket.socket] = []
        
        # Write sequence tracking for WAIT command
        self.write_sequence = 0
        self.pending_acks: Dict[int, Set[socket.socket]] = {}  # sequence -> set of replicas that haven't acked
        self.ack_lock = threading.Lock()
        
        # Track replica acknowledgment offsets
        self.replica_offsets: Dict[socket.socket, int] = {}

    @staticmethod
    def generate_replid() -> str:
        """Generate a random 40-character hex string for replication ID"""
        return ''.join(random.choices('0123456789abcdef', k=DEFAULT_REPLICATION_ID_LENGTH))

    def add_replica_connection(self, connection: socket.socket) -> None:
        """Add a replica connection to the tracking list"""
        self.replica_connections.append(connection)
        self.connected_slaves = len(self.replica_connections)
        self.replica_offsets[connection] = 0
        print(f"Added replica connection. Total replicas: {self.connected_slaves}")

    def propagate_to_replicas(self, payload: bytes) -> None:
        """Propagate a command payload to all connected replicas"""
        if self.role != 'master':
            return
        
        # Increment write sequence for this write operation
        self.write_sequence += 1
        current_sequence = self.write_sequence
        
        # Track which replicas need to acknowledge this write
        with self.ack_lock:
            self.pending_acks[current_sequence] = set(self.replica_connections)
            print(f"Write sequence {current_sequence} - waiting for {len(self.replica_connections)} replicas to ack")
            
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
                # Remove from pending acks
                with self.ack_lock:
                    for seq in self.pending_acks:
                        self.pending_acks[seq].discard(replica_conn)
                    if replica_conn in self.replica_offsets:
                        del self.replica_offsets[replica_conn]

    def request_replica_acks(self) -> None:
        """Send REPLCONF GETACK * to all replicas to request acknowledgments"""
        if self.role != 'master':
            return
        
        getack_command = b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n'
        
        for replica_conn in list(self.replica_connections):
            try:
                replica_conn.sendall(getack_command)
                print(f"Sent REPLCONF GETACK * to replica")
            except Exception as e:
                print(f"Failed to send GETACK to replica: {e}")
                # Remove failed connection
                self.replica_connections.remove(replica_conn)
                self.connected_slaves = len(self.replica_connections)
                if replica_conn in self.replica_offsets:
                    del self.replica_offsets[replica_conn]

    def handle_replica_ack(self, replica_conn: socket.socket, offset: int) -> None:
        """Handle acknowledgment from a replica"""
        with self.ack_lock:
            # Update replica's offset
            self.replica_offsets[replica_conn] = offset
            
            # For simplicity, assume the replica has acknowledged all sequences up to the current write_sequence
            # This is a reasonable assumption since replicas process commands sequentially
            for seq in list(self.pending_acks.keys()):
                if replica_conn in self.pending_acks[seq]:
                    self.pending_acks[seq].discard(replica_conn)
                    print(f"Replica acked sequence {seq}")
                    # Clean up empty sets
                    if not self.pending_acks[seq]:
                        del self.pending_acks[seq]

    def handle_replconf_ack(self, replica_conn: socket.socket, ack_offset: int) -> None:
        """Handle REPLCONF ACK response from a replica"""
        print(f"Received REPLCONF ACK {ack_offset} from replica")
        self.handle_replica_ack(replica_conn, ack_offset)

    def handle_client_ack(self, ack_offset: int) -> None:
        """Handle REPLCONF ACK response that came through a client connection"""
        print(f"Received REPLCONF ACK {ack_offset} from client connection")
        # Since we don't know which specific replica sent this,
        # we'll assume it's from one of the replicas that hasn't acked yet
        with self.ack_lock:
            # Find any pending sequence and mark one replica as acked
            for seq in list(self.pending_acks.keys()):
                if self.pending_acks[seq]:
                    # Remove one replica from the pending list
                    replica_conn = next(iter(self.pending_acks[seq]))
                    self.pending_acks[seq].discard(replica_conn)
                    print(f"Marked replica as acked for sequence {seq}")
                    # Clean up empty sets
                    if not self.pending_acks[seq]:
                        del self.pending_acks[seq]
                    break

    def read_replica_responses(self) -> None:
        """Read responses from replica connections to handle ACKs"""
        if self.role != 'master':
            return
        
        # Check each replica connection for available data
        for replica_conn in list(self.replica_connections):
            try:
                # Use select to check if data is available without blocking
                ready, _, _ = select.select([replica_conn], [], [], 0.001)  # 1ms timeout
                
                if ready:
                    data = replica_conn.recv(1024)
                    if data:
                        print(f"Received data from replica: {data!r}")
                        # Parse the response
                        self.parse_replica_response(replica_conn, data)
            except Exception as e:
                print(f"Error reading from replica connection: {e}")
                # Remove failed connection
                self.replica_connections.remove(replica_conn)
                self.connected_slaves = len(self.replica_connections)
                if replica_conn in self.replica_offsets:
                    del self.replica_offsets[replica_conn]

    def parse_replica_response(self, replica_conn: socket.socket, data: bytes) -> None:
        """Parse response from replica connection"""
        try:
            text = data.decode('utf-8')
            lines = text.strip().split('\r\n')
            
            # Look for REPLCONF ACK responses
            # Format: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n31\r\n
            if len(lines) >= 7 and lines[0] == '*3' and lines[1] == '$8' and lines[2] == 'REPLCONF':
                if lines[3] == '$3' and lines[4] == 'ACK':
                    try:
                        # The offset is the last element
                        ack_offset = int(lines[6])
                        self.handle_replconf_ack(replica_conn, ack_offset)
                    except (ValueError, IndexError) as e:
                        print(f"Error parsing ACK offset: {e}")
        except Exception as e:
            print(f"Error parsing replica response: {e}")

    def wait_for_acks(self, num_replicas: int, timeout_ms: int) -> int:
        """Wait for at least num_replicas to acknowledge the last write operation"""
        if self.role != 'master' or not self.replica_connections:
            return 0
        
        if self.write_sequence == 0:
            # No writes have been made yet, return the number of connected replicas
            return len(self.replica_connections)
        
        target_sequence = self.write_sequence
        timeout_seconds = timeout_ms / 1000.0
        start_time = time.time()
        
        print(f"WAIT: waiting for {num_replicas} replicas to ack sequence {target_sequence}")
        
        # Request acknowledgments from all replicas
        self.request_replica_acks()
        
        while time.time() - start_time < timeout_seconds:
            # Check for replica responses
            self.read_replica_responses()
            
            with self.ack_lock:
                # Check if target sequence has been acknowledged by enough replicas
                if target_sequence in self.pending_acks:
                    acked_count = len(self.replica_connections) - len(self.pending_acks[target_sequence])
                else:
                    # All replicas have acknowledged this sequence
                    acked_count = len(self.replica_connections)
                
                if acked_count >= num_replicas:
                    print(f"WAIT: {acked_count} replicas acked sequence {target_sequence}")
                    return acked_count
            
            # Sleep briefly before checking again
            time.sleep(0.001)  # 1ms sleep
        
        # Timeout reached - do one final check for responses
        self.read_replica_responses()
        
        with self.ack_lock:
            if target_sequence in self.pending_acks:
                acked_count = len(self.replica_connections) - len(self.pending_acks[target_sequence])
            else:
                acked_count = len(self.replica_connections)
        
        print(f"WAIT: timeout reached, {acked_count} replicas acked sequence {target_sequence}")
        return acked_count

    def get_info_lines(self) -> List[str]:
        """Get replication info lines for INFO command"""
        return [
            f"role:{self.role}",
            f"connected_slaves:{self.connected_slaves}",
            f"master_replid:{self.master_replid}",
            f"master_repl_offset:{self.master_repl_offset}"
        ] 