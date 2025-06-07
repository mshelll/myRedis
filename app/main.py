import socket  # noqa: F401
import threading
from datetime import datetime, timezone

cache = {}
CRLF = '\r\n' # CarrageReturn \r followed by  LineFeed \n


def handle_ping(elems):
    return b'+PONG\r\n'


def handle_echo(elems):
    inp = elems[-1]
    return b'+' + inp.encode('utf-8') + b'\r\n'


def handle_set(elems):
    print(f'set {elems=}')
    key = elems[2]
    value = elems[4]

    expiry = 0
    if len(elems) > 5 and elems[6] == 'px':
        px = int(elems[8])
        expiry = datetime.now(timezone.utc).timestamp() + px/1000

    cache[key] = (value, expiry)
    return b'+OK\r\n'


def handle_get(elems):
    print(f'get {elems=}')
    key = elems[-1]
    val, expiry = cache.get(key)
    expired = datetime.now(timezone.utc).timestamp() > expiry if expiry else False
    if expired:
        del cache[key]
        resp = '$-1\r\n'
        return resp.encode('utf-8')

    if val and not expired:
        resp = f'${len(val)}{CRLF}{val}{CRLF}'
    else:
        resp = '$-1\r\n'

    return resp.encode('utf-8')


cmd_handler = {
    'echo': handle_echo,
    'ping': handle_ping,
    'set': handle_set,
    'get': handle_get,
}


def handle_client(connection):
    try:
        while True:
            # Read data from the client
            data = connection.recv(1024)  # Read up to 1024 bytes

            # If client closed connection or no data received, break the loop
            if not data:
                print("Client disconnected")
                break

            # Convert bytes to string and count PING occurrences
            print(data)
            data_str = data.decode('utf-8')
            elems = data_str.split()
            print(elems)
            cmd = elems[2].lower()
            print(f'{cmd=}')
            hander = cmd_handler.get(cmd)
            resp = hander(elems[2:])
            # ping_count = data_str.count("PING")

            # print(f"Received: {data_str}")
            # print(f"Number of PING commands: {ping_count}")

            # For now, just respond with PONG
            connection.sendall(resp)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up the connection
        connection.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Create server socket
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    try:
        while True:
            # Wait for a client to connect
            connection, client_address = server_socket.accept()
            print(f"New connection from {client_address}")

            # Create a new thread to handle this client
            client_thread = threading.Thread(target=handle_client, args=(connection,))
            client_thread.daemon = True  # Set as daemon so it exits when main thread exits
            client_thread.start()

    except KeyboardInterrupt:
        print("Server shutting down")
    except Exception as e:
        import traceback
        traceback.print_stack()
        print(e)
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
    main()