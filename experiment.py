import http
import subprocess
import time
import matplotlib.pyplot as plt
import numpy as np
import random
import http.client
from matplotlib.backends.backend_pdf import PdfPages

def run_experiment(num_nodes, num_operations, addresses):
    # Measure PUT throughput
    put_times = []
    for _ in range(3):  # Run 3 trials
        print(f"Running PUT operation for {num_nodes} nodes")
        start_time = time.time()
        for i in range(num_operations):
            key = f"key_{i}"
            value = f"value_{i}"
            put_value(random.choice(addresses), key, value)
        end_time = time.time()
        put_times.append(end_time - start_time)
    
    # Measure GET throughput
    get_times = []
    for _ in range(3):  # Run 3 trials
        print(f"Running GET operation for {num_nodes} nodes")
        start_time = time.time()
        for i in range(num_operations):
            key = f"key_{i}"
            get_value(random.choice(addresses), key)
        end_time = time.time()
        get_times.append(end_time - start_time)
    
    return put_times, get_times

def put_value(node, key, value):
    conn = None
    try:
        conn = http.client.HTTPConnection(node)
        conn.request("PUT", "/storage/"+key, value)
        conn.getresponse()
    finally:
        if conn:
            conn.close()

def get_value(node, key):
    conn = None
    try:
        conn = http.client.HTTPConnection(node)
        conn.request("GET", "/storage/"+key)
        resp = conn.getresponse()
        value = resp.read().decode('utf-8')
        return value
    finally:
        if conn:
            conn.close()

def plot_results(results):
    num_nodes = [1, 2, 4, 8, 16]
    put_means = [np.mean(results[n]['put']) for n in num_nodes]
    put_stds = [np.std(results[n]['put']) for n in num_nodes]
    get_means = [np.mean(results[n]['get']) for n in num_nodes]
    get_stds = [np.std(results[n]['get']) for n in num_nodes]

    with PdfPages('experiment_results.pdf') as pdf:
        plt.figure()
        plt.errorbar(num_nodes, put_means, yerr=put_stds, label='PUT', fmt='-o')
        plt.errorbar(num_nodes, get_means, yerr=get_stds, label='GET', fmt='-o')
        plt.xlabel('Number of nodes in network')
        plt.ylabel('Time (seconds)')
        plt.title('Time to PUT and GET 100 different values in DHT (n=3)')
        plt.legend()
        pdf.savefig()  # saves the current figure into a pdf page
        plt.close()

if __name__ == "__main__":
    num_operations = 100
    results = {}
    addresses = ["c6-4:63156", "c7-26:54178", "c6-5:50310", "c7-21:65307", "c11-2:63864", "c11-3:57565", "c11-10:51093", "c11-4:64515", "c7-18:61041", "c7-8:56050", "c11-7:58204", "c7-6:64592", "c11-0:58784", "c6-6:63270", "c7-14:61034", "c11-15:49339"]
    num_nodes = len(addresses)
    print(f"Running experiment for {num_nodes} nodes")
    put_times, get_times = run_experiment(num_nodes, num_operations, addresses)
    results[num_nodes] = {'put': put_times, 'get': get_times}
    print(results)