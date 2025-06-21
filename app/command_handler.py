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
        }

    def handle_info(self, elems: list) -> bytes:
        print(f'set {elems=}')
        request = elems[-1]
        if request == 'replication':
            resp = f'$11{CRLF}role:master{CRLF}'
            return resp.encode()

    def process_command(self, cmd: str, args: list) -> bytes:
        """Process a command and return the response"""
        handler = self.cmd_handlers.get(cmd)
        if not handler:
            return f'-ERR unknown command{CRLF}'.encode()
        return handler(args)

    def handle_keys(self, elems: list) -> bytes:
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

    def handle_ping(self, elems: list) -> bytes:
        """Handle PING command"""
        return f'+PONG{CRLF}'.encode()
    
    def handle_echo(self, elems: list) -> bytes:
        """Handle ECHO command"""
        inp = elems[-1]
        return f'+{inp}{CRLF}'.encode()
    
    def handle_set(self, elems: list) -> bytes:
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
        return f'+OK{CRLF}'.encode()

    def handle_get(self, elems: list) -> bytes:
        """Handle GET command - retrieve a value by key"""
        print(f'get {elems=}')
        print(f'{self.server.cache=}')
        key = elems[-1]
        
        if key not in self.server.cache:
            resp = f'$-1{CRLF}'
            return resp.encode('utf-8')
            
        val, expiry = self.server.cache.get(key, (None, None))
        current_time = time.time() * 1000
        print(f'get expiry {expiry=} {current_time=}')
        
        # If expiry is set (not 0) and current time is greater than expiry
        if expiry and current_time > int(expiry):
            del self.server.cache[key]
            resp = f'$-1{CRLF}'
            return resp.encode('utf-8')

        if val:
            resp = f'${len(val)}{CRLF}{val}{CRLF}'
        else:
            resp = f'$-1{CRLF}'

        print(resp)
        return resp.encode('utf-8')
    
    def handle_config(self, elems: list) -> bytes:
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