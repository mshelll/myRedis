import time
from typing import Dict, Any, Tuple, Optional
from .constants import CRLF


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
            'info': self.handle_info,
            'replconf': self.handle_replconf,
            'psync': self.handle_psync,
        }

    def handle_info(self, elems: list, connection) -> None:
        """Handle INFO command - return server replication info"""
        info = self.server.replication_info
        lines = [
            f'role:{info.role}',
            f'connected_slaves:{info.connected_slaves}',
            f'master_replid:{info.master_replid}',
            f'master_repl_offset:{info.master_repl_offset}'
        ]
        content = '\r\n'.join(lines) + '\r\n'
        response = f'${len(content)}{CRLF}{content}{CRLF}'.encode('utf-8')
        connection.sendall(response)

    def process_command(self, cmd: str, args: list, connection) -> None:
        """Process a command and send the response via connection"""
        handler = self.cmd_handlers.get(cmd)
        if not handler:
            response = f'-ERR unknown command{CRLF}'.encode()
            connection.sendall(response)
            return
        handler(args, connection)

    def handle_keys(self, elems: list, connection) -> None:
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
                response = f'*0{CRLF}'.encode()
                connection.sendall(response)
                return
            
            # Format response with all keys according to Redis protocol
            # Start with array length
            resp = f'*{len(keys)}{CRLF}'
            
            # Add each key as a bulk string
            for key in keys:
                resp += f'${len(key)}{CRLF}{key}{CRLF}'
            
            print(f"Returning {len(keys)} keys")
            response = resp.encode('utf-8')
            connection.sendall(response)
            
        except Exception as e:
            print(f"Error handling KEYS command: {e}")
            response = f'-ERR internal error{CRLF}'.encode()
            connection.sendall(response)

    def handle_ping(self, elems: list, connection) -> None:
        """Handle PING command"""
        response = f'+PONG{CRLF}'.encode()
        connection.sendall(response)
    
    def handle_echo(self, elems: list, connection) -> None:
        """Handle ECHO command"""
        inp = elems[-1]
        response = f'+{inp}{CRLF}'.encode()
        connection.sendall(response)
    
    def handle_set(self, elems: list, connection) -> None:
        """Handle SET command - store a key-value pair with optional expiry"""
        print(f'set {elems=}')
        key = elems[2]
        value = elems[4]

        expiry = None
        if len(elems) > 5 and elems[6] == 'px':
            px = int(elems[8])
            # Calculate expiry in milliseconds
            expiry = time.time()*1000 + px
            print(f'expiry {expiry}')

        self.server.cache[key] = (value, expiry)
        response = f'+OK{CRLF}'.encode()
        connection.sendall(response)

        # propagate to replicas
        payload = f'*3{CRLF}$3{CRLF}SET{CRLF}${len(key)}{CRLF}{key}{CRLF}${len(value)}{CRLF}{value}{CRLF}'
        self.server.propagate_to_replicas(payload.encode('utf-8'))

    def handle_get(self, elems: list, connection) -> None:
        """Handle GET command - retrieve a value by key"""
        print(f'get {elems=}')
        print(f'{self.server.cache=}')
        key = elems[-1]
        
        if key not in self.server.cache:
            resp = f'$-1{CRLF}'
            connection.sendall(resp.encode('utf-8'))
            return
            
        val, expiry = self.server.cache.get(key, (None, None))
        current_time = time.time() * 1000
        print(f'get expiry {expiry=} {current_time=}')
        
        # If expiry is set (not 0) and current time is greater than expiry
        if expiry and current_time > int(expiry):
            del self.server.cache[key]
            resp = f'$-1{CRLF}'
            connection.sendall(resp.encode('utf-8'))
            return

        if val:
            resp = f'${len(val)}{CRLF}{val}{CRLF}'
        else:
            resp = f'$-1{CRLF}'

        print(resp)
        connection.sendall(resp.encode('utf-8'))
    
    def handle_config(self, elems: list, connection) -> None:
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
            connection.sendall(resp.encode('utf-8'))
            return
        
        # Default response for unknown config operations
        response = f'-ERR unknown config operation{CRLF}'.encode()
        connection.sendall(response)

    def handle_replconf(self, elems: list, connection) -> None:
        """Handle REPLCONF command (including REPLCONF capa psync2) by responding with +OK"""
        response = f'+OK{CRLF}'.encode()
        connection.sendall(response)

    def handle_psync(self, elems: list, connection) -> None:
        """Handle PSYNC command: respond with +FULLRESYNC <REPL_ID> 0 if master."""
        if self.server.replication_info.role == 'master':
            # Add this connection as a replica
            self.server.add_replica_connection(connection)
            
            replid = self.server.replication_info.master_replid
            resp1 = f'+FULLRESYNC {replid} 0{CRLF}'.encode()
            empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
            empty_rdb_bytes = bytes.fromhex(empty_rdb_hex)
            resp2 = f'${len(empty_rdb_bytes)}{CRLF}'.encode() + bytes.fromhex(empty_rdb_hex)
            
            # Send both responses
            connection.sendall(resp1)
            connection.sendall(resp2)
            return
            
        response = f'-ERR PSYNC not supported in this role{CRLF}'.encode()
        connection.sendall(response) 