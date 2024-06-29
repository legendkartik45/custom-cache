import socket
import threading
import time

def start_server(host='localhost', port=6379):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Server listening on {host}:{port}...")

    threading.Thread(target=expire_keys, daemon=True).start()

    while True:
        client_socket, client_address = server_socket.accept()
        handle_client(client_socket)

def handle_client(client_socket):
    with client_socket:
        request = client_socket.recv(1024).decode('utf-8').strip()
        response = handle_request(request)
        client_socket.sendall(response.encode('utf-8'))

def handle_request(request):
    parts = request.split()
    command = parts[0].upper()
    if command == 'SET':
        return handle_set(parts)
    elif command == 'GET':
        return handle_get(parts)
    elif command == 'DELETE':
        return handle_delete(parts)
    else:
        return "ERROR: Unknown command"

cache = {}
ttl = {}

def handle_set(parts):
    if len(parts) < 3:
        return "ERROR: SET command requires at least 2 arguments"
    key, value = parts[1], parts[2]
    cache[key] = value

    if len(parts) == 4 and parts[3].isdigit():
        ttl[key] = time.time() + int(parts[3])
    elif key in ttl:
        del ttl[key]

    return "OK"

def handle_get(parts):
    if len(parts) != 2:
        return "ERROR: GET command requires 1 argument"
    key = parts[1]

    if key in ttl and time.time() > ttl[key]:
        del cache[key]
        del ttl[key]
        return "nil"

    return cache.get(key, "nil")

def handle_delete(parts):
    if len(parts) != 2:
        return "ERROR: DELETE command requires 1 argument"
    key = parts[1]
    if key in cache:
        del cache[key]
        if key in ttl:
            del ttl[key]
        return "OK"
    else:
        return "nil"

def expire_keys():
    while True:
        current_time = time.time()
        keys_to_delete = [key for key, expiry in ttl.items() if current_time > expiry]
        for key in keys_to_delete:
            del cache[key]
            del ttl[key]
        time.sleep(1)

if __name__ == "__main__":
    start_server()
