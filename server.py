import hashlib
import http
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import sys
import threading
import json

class Node:
    def __init__(self, node_name, node_port, initialization_list):
        self.M = 10 # 160
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
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** self.M)

    def setup_succ_pred(self):
        index = self.hashed_list.index(self.node_id)
        self.pred = self.hashed_map[self.hashed_list[index - 1]]
        self.succ = self.hashed_map[self.hashed_list[(index + 1) % len(self.hashed_list)]]

    def setup_finger_table(self):
        for i in range(self.M):
            start = (self.node_id + 2**i) % (2**self.M)
            #print(f"{self.node_port} finger_table[i={i}], {self.node_id} + {2**i} start: {start}, hashed_map: {self.hashed_map}")
            successor = next((node for node in self.hashed_list if node >= start), self.hashed_list[0])
            self.finger_table.append(self.hashed_map[successor])
        #print(f"{self.node_port} finger_table: {self.finger_table}")

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
        #print(f"hashed_key: {hashed_key}, I am {self.node_id} port {self.node_port}, pred {self.pred.split(':')}, succ {self.succ.split(':')}")
        #print(f"finger_table: {self.finger_table}")
        if self.is_responsible(hashed_key):
            #print(f"PUT port{self.node_port}: is responsible TRUE")
            self.key_val[key] = value
            return "Stored", 200
        else:
            #print(f"PUT port{self.node_port}: is responsible FALSE")
            return self.forward(key, f"/storage/{key}", method="PUT", data=value)

    def is_responsible(self, hashed_key):
        #print(f"{self.hashing(self.pred)} < {hashed_key} <= {self.node_id}, {self.hashing(self.pred) < hashed_key <= self.node_id} ")
        if self.node_id == hashed_key:
            return True
        pred_id = self.hashing(self.pred)
        if pred_id == self.node_id:  # single node only
            return True
        if pred_id < self.node_id:
            # Normal case: predecessor is less than current node ID
            return pred_id < hashed_key <= self.node_id
        else:
            if hashed_key > pred_id:
                return True
            elif hashed_key <= self.node_id:
                return True
        return False
    def find_forward_address(self, hashed_key):
        #print(f"finger_table: {[self.hashing(self.finger_table[i]) for i in range(self.M)]}")
        for i in range(self.M):
            #print(f"Forwarding? {self.finger_table[i]} >= {hashed_key}")
            if self.hashing(self.finger_table[i]) >= hashed_key:
                if i == 0:
                    #print(f"Forwarding to finger_table[i=0]{self.finger_table[i]}")
                    return self.finger_table[i]
                #print(f"Forwarding to finger_table[i={i-1}]{self.finger_table[i-1]}")
                return self.finger_table[i-1]
        #print(f"Forwarding to finger_table[0]{self.finger_table[0]}")
        return self.finger_table[0]
    
    def forward(self, key, url, method="GET", data=None):
        forward_host, forward_port = self.find_forward_address(self.hashing(key)).split(":")
        conn = http.client.HTTPConnection(forward_host, int(forward_port))
        #print(f"Forwarding to {forward_host}:{forward_port}")
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
    def __init__(self, *args, node_instance=None, **kwargs):
        self.node_instance = node_instance
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == '/helloworld':
            response = f"{self.node_instance.node_name}:{self.node_instance.node_port}"
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
        elif self.path.startswith('/storage/'):
            key = self.path[len('/storage/'):]
            response, status = self.node_instance.get_value(key)
            self.send_response(status)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
        elif self.path == '/network':
            response = json.dumps(list(set([
                str(self.node_instance.succ),
                str(self.node_instance.pred),
            ] + [str(n) for n in self.node_instance.finger_table])))
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.encode())
        elif self.path == '/node':
            response = json.dumps({
                "node_name": self.node_instance.node_name,
                "node_port": self.node_instance.node_port,
                "successor": self.node_instance.succ,
                "predecessor": self.node_instance.pred,
                "finger_table": self.node_instance.finger_table,
                "key_value_store": self.node_instance.key_val,
                "node_id": self.node_instance.node_id
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
            content_length = int(self.headers['Content-Length'])
            value = self.rfile.read(content_length).decode('utf-8')
            response, status = self.node_instance.put_value(key, value)
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
    if len(sys.argv) < 4:
        print("Usage: python3 server.py <node_name> <port> <initialization_list>")
        #sys.exit(1)
    
    try:
        node_name = sys.argv[1]
        node_port = int(sys.argv[2])
        initialization_list = sys.argv[3].split(',')
        if not initialization_list or len(initialization_list) < 1:
            print("Initialization list must have at least one element.")
            #sys.exit(1)
    except Exception as e:
        print(f"no args")
    

    def run_server(port, node_instance):
        httpd = HTTPServer(("localhost", port), lambda *args, **kwargs: ServerHandler(*args, node_instance=node_instance, **kwargs))
        httpd.serve_forever()
    

    def run_app():
        # start local server
        if False:
            node_instance1 = Node("localhost", 65123, ["localhost:65123", "localhost:65124", "localhost:65125"])
            node_instance2 = Node("localhost", 65124, ["localhost:65123", "localhost:65124", "localhost:65125"])
            node_instance3 = Node("localhost", 65125, ["localhost:65123", "localhost:65124", "localhost:65125"])
            threading.Thread(target=run_server, args=(65123, node_instance1)).start()
            print("started 1")
            threading.Thread(target=run_server, args=(65124, node_instance2)).start()
            print("started 2")
            threading.Thread(target=run_server, args=(65125, node_instance3)).start()
            print("started 3")
        if True:
            # start it on cluster   
            node_instance = Node(node_name, node_port, initialization_list) 
            httpd = HTTPServer((node_name, node_port), lambda *args, **kwargs: ServerHandler(*args, node_instance=node_instance, **kwargs))
            httpd.serve_forever()

    threading.Thread(target=run_app).start()
    threading.Timer(600, lambda: os._exit(0)).start()  # Shutdown after 10 minutes

if __name__ == '__main__':
    main()
