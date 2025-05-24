import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept()  # wait for client
    
    try:
        while True:
            # Read data from the client
            data = connection.recv(1024)  # Read up to 1024 bytes
            
            # If client closed connection or no data received, break the loop
            if not data:
                print("Client disconnected")
                break
            
            # Convert bytes to string and count PING occurrences
            data_str = data.decode('utf-8')
            ping_count = data_str.count("PING")
            
            print(f"Received: {data_str}")
            print(f"Number of PING commands: {ping_count}")
            
            # For now, just respond with PONG
            connection.sendall(b'+PONG\r\n')
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up the connection
        connection.close()
        server_socket.close()


if __name__ == "__main__":
    main()