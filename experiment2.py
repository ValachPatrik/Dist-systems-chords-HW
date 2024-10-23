import sys
import time
import http.client

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
        conn.request("PUT", "/leave")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to leave {address}")
        conn.close()
    end_time = time.time()
    return end_time - start_time

def simulate_crashes(node_addresses, crash_count):
    start_time = time.time()
    for i in range(crash_count):
        address = node_addresses[i % len(node_addresses)]
        conn = http.client.HTTPConnection(address.split(":")[0], int(address.split(":")[1]))
        conn.request("PUT", "/simulate-crash")
        response = conn.getresponse()
        if response.status != 200:
            print(f"Failed to crash {address}")
        conn.close()
    end_time = time.time()
    return end_time - start_time

def main():

    node_addresses = sys.argv[1].split(",")
    print(node_addresses)

    print("Experiment 1: grow network")
    time_to_join = join_nodes(node_addresses)
    print(f"Time taken to join nodes: {time_to_join:.5f} seconds")

    print("Experiment 2: Time to shrink network to half")
    time_to_leave = leave_nodes(node_addresses)
    print(f"Time taken to leave nodes: {time_to_leave:.5f} seconds")

    reset_nodes(node_addresses)
    #join_nodes(node_addresses)

    #print("Experiment 3: Network tolerance to bursts of node crashes")
    #for crash_count in range(1, 6):  # Testing with 1 to 5 crashes
    #    time_to_crash = simulate_crashes(node_addresses, crash_count)
    #    print(f"Time taken to simulate {crash_count} crashes: {time_to_crash:.2f} seconds")

if __name__ == "__main__":
    main()
