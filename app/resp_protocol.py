from typing import List, Optional, Union
from .constants import CRLF


class RESPProtocol:
    """Handles RESP (Redis Serialization Protocol) encoding and decoding"""
    
    @staticmethod
    def encode_simple_string(message: str) -> bytes:
        """Encode a simple string response"""
        return f"+{message}{CRLF}".encode()
    
    @staticmethod
    def encode_bulk_string(value: Optional[str]) -> bytes:
        """Encode a bulk string response"""
        if value is None:
            return b"$-1\r\n"
        return f"${len(value)}{CRLF}{value}{CRLF}".encode()
    
    @staticmethod
    def encode_array(items: List[str]) -> bytes:
        """Encode an array response"""
        if not items:
            return b"*0\r\n"
        
        parts = [f"*{len(items)}"]
        for item in items:
            parts.append(f"${len(item)}")
            parts.append(item)
        response = f"{CRLF}".join(parts) + f"{CRLF}"
        return response.encode()
    
    @staticmethod
    def encode_error(message: str) -> bytes:
        """Encode an error response"""
        return f"-ERR {message}{CRLF}".encode()
    
    @staticmethod
    def encode_ok() -> bytes:
        """Encode an OK response"""
        return RESPProtocol.encode_simple_string("OK")
    
    @staticmethod
    def encode_pong() -> bytes:
        """Encode a PONG response"""
        return RESPProtocol.encode_simple_string("PONG")
    
    @staticmethod
    def encode_config_array(key: str, value: str) -> bytes:
        """Encode a CONFIG GET response array"""
        return RESPProtocol.encode_array([key, value])
    
    @staticmethod
    def encode_empty_array() -> bytes:
        """Encode an empty array response"""
        return b"*0\r\n"
    
    @staticmethod
    def encode_fullresync(replid: str, offset: int = 0) -> bytes:
        """Encode a FULLRESYNC response"""
        return f"+FULLRESYNC {replid} {offset}{CRLF}".encode()
    
    @staticmethod
    def encode_rdb_file(rdb_hex: str) -> bytes:
        """Encode an RDB file response"""
        rdb_bytes = bytes.fromhex(rdb_hex)
        return f"${len(rdb_bytes)}{CRLF}".encode() + rdb_bytes 