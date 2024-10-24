import sys
import time
import http.client
import json

def join_nodes(node_addresses):
    start_time = time.time()
    for address in node_addresses[1:]:
        conn = http.client.HTTPConnection(address.split(":")[0], int(address.split(":")[1]))    
        conn.request("PUT", f"/join?nprime={node_addresses[0]}")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to join {address}")
        conn.close()
    end_time = time.time()
    return end_time - start_time

def leave_nodes(node_addresses):
    start_time = time.time()
    for address in node_addresses[:len(node_addresses)//2]:
        conn = http.client.HTTPConnection(address.split(":")[0], int(address.split(":")[1]))
        conn.request("PUT", "/leave")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to leave {address}")
        conn.close()
    end_time = time.time()
    return end_time - start_time

def reset_nodes(node_addresses):
    start_time = time.time()
    for address in node_addresses:
        conn = http.client.HTTPConnection(address.split(":")[0], int(address.split(":")[1]))
        conn.request("PUT", "/sim-recover")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to recover {address}")
        conn.request("PUT", "/leave")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to leave {address}")
        conn.close()
    end_time = time.time()
    return end_time - start_time

def simulate_crashes(node_addresses, crash_count):
    for address in node_addresses[:crash_count]:
        conn = http.client.HTTPConnection(address.split(":")[0], int(address.split(":")[1]))
        conn.request("PUT", "/sim-crash")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to crash {address}")
        conn.close()
    
    time.sleep(20) # wait for stabilization call and stabilization of network
    
    initial_address = node_addresses[-1]
    current_address = initial_address
    stabilized = True
    for i in range(32):
        conn = http.client.HTTPConnection(current_address.split(":")[0], int(current_address.split(":")[1]))
        conn.request("GET", "/node-info")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to get node info from {current_address}")
            stabilized = False
            break
        data = json.loads(response.read().decode())
        if data["successor"] == initial_address:
            break
        current_address = data["successor"]
        conn.close()
        if i == 31:
            stabilized = False
    return stabilized

def main():

    node_addresses = sys.argv[1].split(",")
    print(node_addresses)

    #print("Experiment 1: grow network")
    #time_to_join = join_nodes(node_addresses)
    #print(f"Time taken to join nodes: {time_to_join:.5f} seconds")

    #print("Experiment 2: Time to shrink network to half")
    #time_to_leave = leave_nodes(node_addresses)
    #print(f"Time taken to leave nodes: {time_to_leave:.5f} seconds")

    #reset_nodes(node_addresses)
    
    
    print("Experiment 3: Network tolerance to bursts of node crashes")
    list_stabilized = []
    for crash_count in range(1, 32):
        reset_nodes(node_addresses)
        join_nodes(node_addresses)
        print(f"primed for {crash_count}")
        stabilized = simulate_crashes(node_addresses, crash_count)
        print(f"Simulate {crash_count} crashes: Has stabilized? {stabilized}")
        list_stabilized.append(stabilized)
        if not stabilized:
            break
    print(list_stabilized)

if __name__ == "__main__":
    main()
