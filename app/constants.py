"""Redis constants and configuration values"""

# RESP protocol constants
CRLF = "\r\n"

# Network constants
DEFAULT_PORT = 6379
DEFAULT_HOST = "localhost"
DEFAULT_DB_PATH = "/tmp/dump.rdb"

# RESP types
RESP_SIMPLE_STRING = "+"
RESP_ERROR = "-"
RESP_INTEGER = ":"
RESP_BULK_STRING = "$"
RESP_ARRAY = "*"

# Redis commands
COMMANDS = {
    "PING", "ECHO", "SET", "GET", "KEYS", 
    "CONFIG", "INFO", "REPLCONF", "PSYNC"
}

# Replication constants
DEFAULT_REPLICATION_ID_LENGTH = 40
EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

# Configuration keys
CONFIG_KEYS = {
    "dir", "dbfilename", "port", "role", "replicaof"
}

# Error messages
ERROR_MESSAGES = {
    "WRONG_NUMBER_OF_ARGS": "wrong number of arguments for {command} command",
    "UNKNOWN_COMMAND": "unknown command '{command}'",
    "VALUE_NOT_INTEGER": "value is not an integer or out of range",
    "PSYNC_NOT_SUPPORTED": "PSYNC not supported in this role",
    "UNKNOWN_SUBCOMMAND": "Unknown subcommand or wrong number of arguments for CONFIG"
}

# Time constants
MILLISECONDS_PER_SECOND = 1000 