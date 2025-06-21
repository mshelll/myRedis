import argparse
from .server import RedisServer
from .constants import DEFAULT_PORT


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Redis server implementation')
    parser.add_argument('--dir', type=str, default='/tmp',
                        help='Directory for Redis data files (default: /tmp)')
    parser.add_argument('--dbfilename', type=str, default='dump.rdb',
                        help='Filename for the Redis database (default: dump.rdb)')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT,
                        help=f'Port to listen on (default: {DEFAULT_PORT})')

    return parser.parse_args()


def main():
    """Main entry point"""
    print("Inside main")
    
    # Parse command line arguments
    args = parse_args()
    
    # Create and start server
    server = RedisServer()
    server.initialize(args)
    server.start()


if __name__ == "__main__":
    main()
