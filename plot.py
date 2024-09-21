import http
import subprocess
import time
import matplotlib.pyplot as plt
import numpy as np
import random
import http.client
from matplotlib.backends.backend_pdf import PdfPages


def plot_results(results):
    num_nodes = [1, 2, 4, 8, 16]
    put_means = [np.mean(results[n]["put"]) for n in num_nodes]
    put_stds = [np.std(results[n]["put"]) for n in num_nodes]
    get_means = [np.mean(results[n]["get"]) for n in num_nodes]
    get_stds = [np.std(results[n]["get"]) for n in num_nodes]

    with PdfPages("experiment_results.pdf") as pdf:
        plt.figure()
        plt.errorbar(num_nodes, put_means, yerr=put_stds, label="PUT", fmt="-o")
        plt.errorbar(num_nodes, get_means, yerr=get_stds, label="GET", fmt="-o")
        plt.xlabel("Number of nodes in network")
        plt.ylabel("Time (seconds)")
        plt.title("Time to PUT and GET 100 different values in DHT (n=100)")
        plt.legend()
        pdf.savefig()  # saves the current figure into a pdf page
        plt.close()


if __name__ == "__main__":
    plot_results(
        {
            1: {'put': [0.04081535339355469, 0.038034677505493164, 0.03774380683898926], 'get': [0.039659976959228516, 0.034574031829833984, 0.03490447998046875]},
            2: {'put': [0.10348367691040039, 0.09232711791992188, 0.0971219539642334], 'get': [0.0980384349822998, 0.10206460952758789, 0.10654044151306152]},
            4: {'put': [0.31058526039123535, 0.3766343593597412, 0.8102521896362305], 'get': [0.1789853572845459, 0.1687772274017334, 0.16334247589111328]},
            8: {'put': [0.7988860607147217, 0.6718454360961914, 0.5190091133117676], 'get': [0.49247050285339355, 0.49208664894104004, 0.4807400703430176]},
            16: {'put': [1.0985281467437744, 0.9119656085968018, 0.8069441318511963], 'get': [0.839228630065918, 0.8278074264526367, 0.9630928039550781]}
        }
    )
