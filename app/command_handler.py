import time
from typing import Dict, Any, Tuple, Optional
from .constants import CRLF, ERROR_MESSAGES, MILLISECONDS_PER_SECOND, EMPTY_RDB_HEX
from .resp_protocol import RESPProtocol


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
        response = RESPProtocol.encode_ok()
        self.connection.sendall(response)

    def handle_psync(self, elems: list) -> None:
        """Handle PSYNC command: respond with +FULLRESYNC <REPL_ID> 0 if master."""
        if self.server.replication.role == 'master':
            # Add this connection as a replica
            self.server.replication.add_replica_connection(self.connection)
            
            replid = self.server.replication.master_replid
            resp1 = RESPProtocol.encode_fullresync(replid, 0)
            resp2 = RESPProtocol.encode_rdb_file(EMPTY_RDB_HEX)
            
            # Send both responses
            self.connection.sendall(resp1)
            self.connection.sendall(resp2)
            return
            
        response = RESPProtocol.encode_error(ERROR_MESSAGES["PSYNC_NOT_SUPPORTED"])
        self.connection.sendall(response)

    def handle_ping(self, elems: list) -> None:
        """Handle PING command - respond with +PONG"""
        response = RESPProtocol.encode_pong()
        self.connection.sendall(response)

    def handle_echo(self, elems: list) -> None:
        """Handle ECHO command - echo back the message"""
        if len(elems) < 2:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="ECHO")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        
        message = elems[1]
        response = RESPProtocol.encode_simple_string(message)
        self.connection.sendall(response)

    def handle_set(self, elems: list) -> None:
        """Handle SET command - set key-value pair with optional expiry"""
        if len(elems) < 3:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="SET")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        
        key = elems[1]
        value = elems[2]
        expiry = None
        
        # Check for expiry options (PX for milliseconds)
        if len(elems) >= 5 and elems[3].upper() == 'PX':
            try:
                px_milliseconds = int(elems[4])
                expiry = time.time() * MILLISECONDS_PER_SECOND + px_milliseconds
                print(f"expiry {expiry}")
            except ValueError:
                response = RESPProtocol.encode_error(ERROR_MESSAGES["VALUE_NOT_INTEGER"])
                self.connection.sendall(response)
                return
        
        self.server.storage.set(key, value, expiry)
        response = RESPProtocol.encode_ok()
        self.connection.sendall(response)

        # propagate to replicas
        payload = f'*3{CRLF}$3{CRLF}SET{CRLF}${len(key)}{CRLF}{key}{CRLF}${len(value)}{CRLF}{value}{CRLF}'
        self.server.replication.propagate_to_replicas(payload.encode('utf-8'))

    def handle_get(self, elems: list) -> None:
        """Handle GET command - get value for key"""
        if len(elems) < 2:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="GET")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        
        key = elems[1]
        value = self.server.storage.get(key)
        response = RESPProtocol.encode_bulk_string(value)
        self.connection.sendall(response)

    def handle_keys(self, elems: list) -> None:
        """Handle KEYS command - return all keys matching pattern"""
        if len(elems) < 2:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="KEYS")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        
        pattern = elems[1]
        keys = self.server.storage.keys(pattern)
        
        print(f"keys={keys}")
        print(f" {self.server.storage.get_all()}")
        print(f"Returning {len(keys)} keys")
        
        response = RESPProtocol.encode_array(keys)
        self.connection.sendall(response)

    def handle_config(self, elems: list) -> None:
        """Handle CONFIG command - return configuration values"""
        if len(elems) < 3:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="CONFIG")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        
        subcommand = elems[1].upper()
        parameter = elems[2]
        
        if subcommand == 'GET':
            if parameter == 'dir':
                dir_path = self.server.config.get('dir', '/tmp')
                response = RESPProtocol.encode_config_array('dir', dir_path)
                self.connection.sendall(response)
            else:
                response = RESPProtocol.encode_empty_array()
                self.connection.sendall(response)
        else:
            response = RESPProtocol.encode_error(ERROR_MESSAGES["UNKNOWN_SUBCOMMAND"])
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
            error_msg = ERROR_MESSAGES["UNKNOWN_COMMAND"].format(command=cmd)
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response) 