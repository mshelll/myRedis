import time
from typing import Dict, Any, Tuple, Optional
from .constants import CRLF


class RedisCommandHandler:
    """Handles Redis commands processing for a specific client connection"""
    
    def __init__(self, server, connection):
        self.server = server
        self.connection = connection
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

    def handle_info(self, elems: list) -> None:
        """Handle INFO command - return server replication info"""
        info_lines = self.server.replication.get_info_lines()
        content = '\r\n'.join(info_lines) + '\r\n'
        response = f'${len(content)}{CRLF}{content}{CRLF}'.encode('utf-8')
        self.connection.sendall(response)

    def handle_replconf(self, elems: list) -> None:
        """Handle REPLCONF command - respond with +OK"""
        response = f'+OK{CRLF}'.encode()
        self.connection.sendall(response)

    def handle_psync(self, elems: list) -> None:
        """Handle PSYNC command: respond with +FULLRESYNC <REPL_ID> 0 if master."""
        if self.server.replication.role == 'master':
            # Add this connection as a replica
            self.server.replication.add_replica_connection(self.connection)
            
            replid = self.server.replication.master_replid
            resp1 = f'+FULLRESYNC {replid} 0{CRLF}'.encode()
            empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
            empty_rdb_bytes = bytes.fromhex(empty_rdb_hex)
            resp2 = f'${len(empty_rdb_bytes)}{CRLF}'.encode() + bytes.fromhex(empty_rdb_hex)
            
            # Send both responses
            self.connection.sendall(resp1)
            self.connection.sendall(resp2)
            return
            
        response = f'-ERR PSYNC not supported in this role{CRLF}'.encode()
        self.connection.sendall(response)

    def handle_ping(self, elems: list) -> None:
        """Handle PING command - respond with +PONG"""
        response = f'+PONG{CRLF}'.encode()
        self.connection.sendall(response)

    def handle_echo(self, elems: list) -> None:
        """Handle ECHO command - echo back the message"""
        if len(elems) < 2:
            response = f'-ERR wrong number of arguments for ECHO command{CRLF}'.encode()
            self.connection.sendall(response)
            return
        
        message = elems[1]
        response = f'+{message}{CRLF}'.encode()
        self.connection.sendall(response)

    def handle_set(self, elems: list) -> None:
        """Handle SET command - set key-value pair with optional expiry"""
        if len(elems) < 3:
            response = f'-ERR wrong number of arguments for SET command{CRLF}'.encode()
            self.connection.sendall(response)
            return
        
        key = elems[1]
        value = elems[2]
        expiry = None
        
        # Check for expiry options (PX for milliseconds)
        if len(elems) >= 5 and elems[3].upper() == 'PX':
            try:
                px_milliseconds = int(elems[4])
                expiry = time.time() * 1000 + px_milliseconds
                print(f"expiry {expiry}")
            except ValueError:
                response = f'-ERR value is not an integer or out of range{CRLF}'.encode()
                self.connection.sendall(response)
                return
        
        self.server.cache[key] = (value, expiry)
        response = f'+OK{CRLF}'.encode()
        self.connection.sendall(response)

        # propagate to replicas
        payload = f'*3{CRLF}$3{CRLF}SET{CRLF}${len(key)}{CRLF}{key}{CRLF}${len(value)}{CRLF}{value}{CRLF}'
        self.server.replication.propagate_to_replicas(payload.encode('utf-8'))

    def handle_get(self, elems: list) -> None:
        """Handle GET command - get value for key"""
        if len(elems) < 2:
            response = f'-ERR wrong number of arguments for GET command{CRLF}'.encode()
            self.connection.sendall(response)
            return
        
        key = elems[1]
        
        if key not in self.server.cache:
            response = f'$-1{CRLF}'.encode()
            self.connection.sendall(response)
            return
        
        value, expiry = self.server.cache[key]
        
        # Check if key has expired
        if expiry is not None:
            current_time = time.time() * 1000
            # Convert expiry to float if it's a string
            expiry_time = float(expiry) if isinstance(expiry, str) else expiry
            print(f"get expiry expiry={expiry_time} current_time={current_time}")
            if current_time > expiry_time:
                # Remove expired key
                del self.server.cache[key]
                response = f'$-1{CRLF}'.encode()
                self.connection.sendall(response)
                return
        
        response = f'${len(value)}{CRLF}{value}{CRLF}'.encode()
        self.connection.sendall(response)

    def handle_keys(self, elems: list) -> None:
        """Handle KEYS command - return all keys matching pattern"""
        if len(elems) < 2:
            response = f'-ERR wrong number of arguments for KEYS command{CRLF}'.encode()
            self.connection.sendall(response)
            return
        
        pattern = elems[1]
        
        # For now, just return all keys (simple implementation)
        if pattern == '*':
            keys = list(self.server.cache.keys())
            print(f"keys={keys}")
            print(f" {self.server.cache}")
            print(f"Returning {len(keys)} keys")
            
            # Build RESP array response
            response_parts = [f'*{len(keys)}']
            for key in keys:
                response_parts.append(f'${len(key)}')
                response_parts.append(key)
            
            response = f'{CRLF}'.join(response_parts) + f'{CRLF}'
            self.connection.sendall(response.encode())
        else:
            # For other patterns, return empty array for now
            response = f'*0{CRLF}'.encode()
            self.connection.sendall(response)

    def handle_config(self, elems: list) -> None:
        """Handle CONFIG command - return configuration values"""
        if len(elems) < 3:
            response = f'-ERR wrong number of arguments for CONFIG command{CRLF}'.encode()
            self.connection.sendall(response)
            return
        
        subcommand = elems[1].upper()
        parameter = elems[2]
        
        if subcommand == 'GET':
            if parameter == 'dir':
                dir_path = self.server.config.get('dir', '/tmp')
                response = f'*2{CRLF}$3{CRLF}dir{CRLF}${len(dir_path)}{CRLF}{dir_path}{CRLF}'.encode()
                self.connection.sendall(response)
            else:
                response = f'*0{CRLF}'.encode()
                self.connection.sendall(response)
        else:
            response = f'-ERR Unknown subcommand or wrong number of arguments for CONFIG{CRLF}'.encode()
            self.connection.sendall(response)

    def process_command(self, elems: list) -> None:
        """Process a command by calling the appropriate handler"""
        if not elems:
            return
        
        cmd = elems[0].lower()
        
        if cmd in self.cmd_handlers:
            handler = self.cmd_handlers[cmd]
            handler(elems)
        else:
            response = f'-ERR unknown command \'{cmd}\'{CRLF}'.encode()
            self.connection.sendall(response) 