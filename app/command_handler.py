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
            'wait': self.handle_wait,
            'rpush': self.handle_rpush,
            'lrange': self.handle_lrange,
            'lpush': self.handle_lpush,
            'llen': self.handle_llen,
        }

    def handle_info(self, elems: list) -> None:
        """Handle INFO command - return server replication info"""
        info_lines = self.server.replication.get_info_lines()
        content = '\r\n'.join(info_lines) + '\r\n'
        response = f'${len(content)}{CRLF}{content}{CRLF}'.encode('utf-8')
        self.connection.sendall(response)

    def handle_replconf(self, elems: list) -> None:
        """Handle REPLCONF command - respond with +OK or handle GETACK"""
        if len(elems) >= 2:
            subcommand = elems[1].upper()
            
            # Handle GETACK subcommand
            if subcommand == 'GETACK':
                # Respond with REPLCONF ACK 0
                ack_response = f'*3{CRLF}$8{CRLF}REPLCONF{CRLF}$3{CRLF}ACK{CRLF}$1{CRLF}0{CRLF}'
                self.connection.sendall(ack_response.encode('utf-8'))
                return
            
            # Handle ACK subcommand (from replicas)
            elif subcommand == 'ACK' and len(elems) >= 3:
                try:
                    ack_offset = int(elems[2])
                    # This is an acknowledgment from a replica
                    # Find which replica connection this corresponds to
                    # For now, we'll assume this is from a replica and update tracking
                    if hasattr(self.server.replication, 'handle_client_ack'):
                        # Handle ACK that came through client connection
                        self.server.replication.handle_client_ack(ack_offset)
                except ValueError:
                    pass
        
        # Default response for other REPLCONF commands
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

    def handle_rpush(self, elems: list) -> None:
        """Handle RPUSH command - append one or more values to a list and return new length"""
        if len(elems) < 3:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="RPUSH")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        key = elems[1]
        values = elems[2:]
        new_length = self.server.storage.rpush(key, *values)
        response = f":{new_length}{CRLF}".encode('utf-8')
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

    def handle_wait(self, elems: list) -> None:
        """Handle WAIT command - wait for replicas to acknowledge the last write operation"""
        if len(elems) < 3:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="WAIT")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        
        try:
            num_replicas = int(elems[1])
            timeout_ms = int(elems[2])
        except ValueError:
            response = RESPProtocol.encode_error(ERROR_MESSAGES["VALUE_NOT_INTEGER"])
            self.connection.sendall(response)
            return
        
        # Wait for replicas to acknowledge the last write operation
        acked_count = self.server.replication.wait_for_acks(num_replicas, timeout_ms)
        response = f":{acked_count}{CRLF}".encode('utf-8')
        self.connection.sendall(response)

    def handle_rpush(self, elems: list) -> None:
        """Handle RPUSH command - append one or more values to a list and return new length"""
        if len(elems) < 3:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="RPUSH")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        key = elems[1]
        values = elems[2:]
        new_length = self.server.storage.rpush(key, *values)
        response = f":{new_length}{CRLF}".encode('utf-8')
        self.connection.sendall(response)

    def handle_lrange(self, elems: list) -> None:
        """Handle LRANGE command - return a range of elements from a list"""
        if len(elems) < 4:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="LRANGE")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        key = elems[1]
        start = int(elems[2])
        stop = int(elems[3])
        elements = self.server.storage.lrange(key, start, stop)
        response = RESPProtocol.encode_array(elements)
        self.connection.sendall(response)

    def handle_lpush(self, elems: list) -> None:
        """Handle LPUSH command - append one or more values to a list and return new length"""
        if len(elems) < 3:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="LPUSH")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        key = elems[1]
        values = elems[2:]
        new_length = self.server.storage.lpush(key, *values)
        response = f":{new_length}{CRLF}".encode('utf-8')
        self.connection.sendall(response)

    def handle_llen(self, elems: list) -> None:
        """Handle LLEN command - return the length of a list"""
        if len(elems) < 2:
            error_msg = ERROR_MESSAGES["WRONG_NUMBER_OF_ARGS"].format(command="LLEN")
            response = RESPProtocol.encode_error(error_msg)
            self.connection.sendall(response)
            return
        key = elems[1]
        length = self.server.storage.llen(key)
        response = f":{length}{CRLF}".encode('utf-8')
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