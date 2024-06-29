import socket


def main():
    print("Redis server started successfully!")
    
    PONG = "+PONG\r\n"
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, addr = server_socket.accept()  # wait for client
    
    while connection:
        connection.recv(1024)
        connection.send(PONG.encode())
        
    


if __name__ == "__main__":
    main()
