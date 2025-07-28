# Redis Server - Modular Architecture

This Redis server implementation has been refactored into a modular architecture for better maintainability and separation of concerns.

## File Structure

```
app/
├── __init__.py              # Package initialization
├── main.py                  # Entry point - imports and runs RedisServer
├── server.py                # Main RedisServer class - orchestrates all components
├── constants.py             # Global constants (CRLF, DEFAULT_PORT, etc.)
├── config.py                # Server configuration and argument parsing
├── rdb_parser.py            # Redis Database file parsing
├── command_handler.py       # Redis command processing
├── client_handler.py        # Client connection management
└── README.md                # This file
```

## Module Responsibilities

### `main.py`
- Entry point for the application
- Creates and starts the RedisServer instance

### `server.py`
- Main RedisServer class that orchestrates all components
- Manages server lifecycle and initialization
- Coordinates between different modules

### `constants.py`
- Contains global constants used across the application
- Currently includes CRLF for Redis protocol compliance and DEFAULT_PORT

### `config.py`
- Handles server configuration management
- Parses command line arguments
- Manages data directory, database file paths, and server port

### `rdb_parser.py`
- Dedicated module for parsing Redis Database (RDB) files
- Extracts key-value pairs and expiry information
- Handles binary file format parsing

### `command_handler.py`
- Processes Redis commands (PING, ECHO, SET, GET, KEYS, CONFIG)
- Implements Redis protocol responses
- Manages cache operations and expiry logic

### `client_handler.py`
- Manages individual client connections
- Handles data reception and command routing
- Implements connection lifecycle management

## Benefits of This Architecture

1. **Separation of Concerns**: Each module has a single, well-defined responsibility
2. **Maintainability**: Easier to locate and modify specific functionality
3. **Testability**: Individual modules can be tested in isolation
4. **Reusability**: Components can be reused or replaced independently
5. **Type Safety**: Added type hints for better code documentation and IDE support

## Usage

Run the server with default settings:
```bash
python3 -m app.main
```

Run the server with custom configuration:
```bash
python3 -m app.main --port 6380 --dir /custom/path --dbfilename custom.rdb
```

### Available Arguments

- `--port PORT`: Port to listen on (default: 6379)
- `--dir DIR`: Directory for Redis data files (default: /tmp)
- `--dbfilename DBFILENAME`: Filename for the Redis database (default: dump.rdb)
- `--help`: Show help message

The functionality remains exactly the same, but the code is now better organized and more maintainable. 