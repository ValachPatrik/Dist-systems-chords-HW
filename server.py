import hashlib
import http
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import sys
import threading
import json

class Node:
    def __init__(self, node_name, node_port, initialization_list):
        self.M = 256  # sha256
        self.node_name = node_name
        self.node_port = node_port
        self.node_id = self.hashing(f"{node_name}:{node_port}")
        self.finger_table = []
        self.key_val = {}
        self.succ = None
        self.pred = None

        self.initialization_list = initialization_list
        self.hashed_map = {self.hashing(node): node for node in self.initialization_list}
        self.hashed_list = sorted([self.hashing(node) for node in self.initialization_list])
        self.setup_succ_pred() 
        self.setup_finger_table()
        self.initialization_list = []  # Drop the list after organizing the ring and table
        self.hashed_map = {}
        self.hashed_list = []

    def hashing(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2 ** self.M)

    def setup_succ_pred(self):
        index = self.hashed_list.index(self.node_id)
        self.pred = self.hashed_map[self.hashed_list[index - 1]]
        self.succ = self.hashed_map[self.hashed_list[(index + 1) % len(self.hashed_list)]]

    def setup_finger_table(self):
        for i in range(self.M):
            start = (self.node_id + 2**i) % (2**self.M)
            successor = next((node for node in self.hashed_list if node >= start), self.hashed_list[0])
            self.finger_table.append(self.hashed_map[successor])

    def get_value(self, key):
        hashed_key = self.hashing(key)
        if self.is_responsible(hashed_key):
            value = self.key_val.get(key)
            if value:
                return value, 200
            else:
                return "Key not found", 404
        else:
            return self.forward(key, f"/storage/{key}")

    def put_value(self, key, value):
        hashed_key = self.hashing(key)
        if self.is_responsible(hashed_key):
            self.key_val[key] = value
            return "Stored", 200
        else:
            return self.forward(key, f"/storage/{key}", method="PUT", data=value)

    def is_responsible(self, hashed_key):
        if self.hashing(self.pred) == self.node_id:
            return True
        return self.hashing(self.pred) < hashed_key <= self.node_id
    
    def find_forward_address(self, hashed_key):
        for i in range(self.M):
            if self.hashing(self.finger_table[i]) >= hashed_key:
                return self.finger_table[i]
        return self.finger_table[0]
    
    def forward(self, key, url, method="GET", data=None):
        forward_host, forward_port = self.find_forward_address(self.hashing(key)).split(":")
        conn = http.client.HTTPConnection(forward_host, int(forward_port))
        try:
            if method == "GET":
                conn.request("GET", url)
            elif method == "PUT":
                headers = {"Content-type": "text/plain"}
                conn.request("PUT", url, body=data, headers=headers)
            response = conn.getresponse()
            response_text = response.read().decode()
            return response_text, response.status
        except Exception as e:
            return f"Forwarding failed: {e}", 500
        finally:
            conn.close()

class ServerHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/helloworld':
            response = f"{node_instance.node_name}:{node_instance.node_port}"
            
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
        elif self.path.startswith('/storage/'):
            key = self.path[len('/storage/'):]
            response, status = node_instance.get_value(key)
            
            self.send_response(status)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
            
        elif self.path == '/network':
            response = json.dumps(list(set([
                str(node_instance.succ),
                str(node_instance.pred),
            ] + [str(n) for n in node_instance.finger_table])))
            
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
        
        elif self.path == '/node':
            response = json.dumps({
                "node_name": node_instance.node_name,
                "node_port": node_instance.node_port,
                "successor": node_instance.succ,
                "predecessor": node_instance.pred,
                "finger_table": node_instance.finger_table,
                "key_value_store": node_instance.key_val,
                "node_id": node_instance.node_id
            })
            
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("Not found".encode())
            
    def do_PUT(self):
        if self.path.startswith('/storage/'):
            key = self.path[len('/storage/'):]
            value = self.rfile.read().decode('utf-8')
            response, status = node_instance.put_value(key, value)
            
            self.send_response(status)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("Not found".encode())

def main():
    global node_instance
    if len(sys.argv) < 4:
        print("Usage: python3 server.py <node_name> <port> <initialization_list>")
        sys.exit(1)
    
    node_name = sys.argv[1]
    node_port = int(sys.argv[2])
    initialization_list = sys.argv[3].split(',')
    
    if not initialization_list or len(initialization_list) < 1:
        print("Initialization list must have at least one element.")
        sys.exit(1)
    
    node_instance = Node(node_name, node_port, initialization_list)

    def run_app():
        httpd = HTTPServer((node_name, node_port), ServerHandler)
        httpd.serve_forever()

    threading.Thread(target=run_app).start()
    threading.Timer(300, lambda: os._exit(0)).start()  # Shutdown after 5 minutes

if __name__ == '__main__':
    main()
