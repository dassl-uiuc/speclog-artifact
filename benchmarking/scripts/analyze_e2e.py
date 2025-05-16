import numpy as np
import glob
import os

def get_append_metrics(path):
    file_pattern = path + "append_metrics*.csv"

    total_throughput = 0
    latency_values = []

    for file in glob.glob(file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]
            for line in lines:
                parts = line.strip().split(',')
                gsn, latency, throughput = int(parts[0]), float(parts[1]), float(parts[2])
                latency_values.append(latency)
            
            total_throughput += throughput

    latency_array = np.array(latency_values)

    mean_latency = np.mean(latency_array)
    p50_latency = np.percentile(latency_array, 50)
    p99_latency = np.percentile(latency_array, 99)

    print("results for computation time " + path.split("_")[-1].split("/")[0] + " us")
    print("statistic/metric, latency (us)")
    print(f"mean, {mean_latency:.2f}")
    print(f"p50, {p50_latency:.2f}")
    print(f"p99, {p99_latency:.2f}")
    print(f"total throughput, {total_throughput:.2f}")

    return int(path.split("_")[-1].split("/")[0]), mean_latency, total_throughput 

def get_e2e_metrics(path):
    file_pattern = path + "e2e_metrics*.csv"

    delivery_latency_values = []
    compute_latency_values = []
    confirmation_latency_values = []
    e2e_latency_values = []
    queuing_delay_values = []

    for file in glob.glob(file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]
            for line in lines:
                parts = line.strip().split(',')
                delivery, confirm, compute, e2e, queuing_delay = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])
                delivery_latency_values.append(delivery)
                compute_latency_values.append(compute)
                confirmation_latency_values.append(confirm)
                e2e_latency_values.append(e2e)
                queuing_delay_values.append(queuing_delay)

    
    delivery_latency_array = np.array(delivery_latency_values)
    compute_latency_array = np.array(compute_latency_values)
    confirmation_latency_array = np.array(confirmation_latency_values)
    e2e_latency_array = np.array(e2e_latency_values)
    queuing_delay_array = np.array([x for x in queuing_delay_values if x > 0])

    print("statistic/metric, delivery latency (us), compute latency (us), confirm latency (us), e2e latency (us), queuing delay (us)")
    print(f"mean, {np.mean(delivery_latency_array):.2f}, {np.mean(compute_latency_array):.2f}, {np.mean(confirmation_latency_array):.2f}, {np.mean(e2e_latency_array):.2f}, {np.mean(queuing_delay_array):.2f}")
    print(f"std, {np.std(delivery_latency_array):.2f}, {np.std(compute_latency_array):.2f}, {np.std(confirmation_latency_array):.2f}, {np.std(e2e_latency_array):.2f}, {np.std(queuing_delay_array):.2f}")
    print(f"p50, {np.percentile(delivery_latency_array, 50):.2f}, {np.percentile(compute_latency_array, 50):.2f}, {np.percentile(confirmation_latency_array, 50):.2f}, {np.percentile(e2e_latency_array, 50):.2f}, {np.percentile(queuing_delay_array, 50):.2f}")
    print(f"p99, {np.percentile(delivery_latency_array, 99):.2f}, {np.percentile(compute_latency_array, 99):.2f}, {np.percentile(confirmation_latency_array, 99):.2f}, {np.percentile(e2e_latency_array, 99):.2f}, {np.percentile(queuing_delay_array, 99):.2f}")

    return int(path.split("_")[-1].split("/")[0]), np.mean(delivery_latency_array), np.mean(compute_latency_array), np.mean(confirmation_latency_array), np.mean(e2e_latency_array), np.mean(queuing_delay_array)


def get_splits(path):
    file_pattern = path + "e2e_metrics*.csv"

    delivery_latency_values = []
    compute_latency_values = []
    confirmation_latency_values = []
    e2e_latency_values = []
    queuing_delay_values = []

    for file in glob.glob(file_pattern):
        with open(file, 'r') as f:
            lines = f.readlines()[1:]
            for line in lines:
                parts = line.strip().split(',')
                delivery, confirm, compute, e2e, queuing_delay = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])
                delivery_latency_values.append(delivery)
                compute_latency_values.append(compute)
                confirmation_latency_values.append(confirm)
                e2e_latency_values.append(e2e)
                queuing_delay_values.append(queuing_delay)

    delivery_latency_array = np.array(delivery_latency_values)
    compute_latency_array = np.array(compute_latency_values)
    confirmation_latency_array = np.array(confirmation_latency_values)
    e2e_latency_array = np.array(e2e_latency_values)
    queuing_delay_array = np.array([x for x in queuing_delay_values if x > 0])

    min_size = min(len(compute_latency_array), len(delivery_latency_array), 
                len(confirmation_latency_array), len(queuing_delay_array))

    compute_latency_array = compute_latency_array[:min_size]
    delivery_latency_array = delivery_latency_array[:min_size]
    confirmation_latency_array = confirmation_latency_array[:min_size]
    queuing_delay_array = queuing_delay_array[:min_size]

    compute = compute_latency_array - delivery_latency_array - queuing_delay_array
    wait_for_confirmation = confirmation_latency_array - compute_latency_array
    for i in range(len(wait_for_confirmation)):
        if wait_for_confirmation[i] < 0:
            wait_for_confirmation[i] = 0
        else:
            wait_for_confirmation[i] = wait_for_confirmation[i]


    print("results for computation time " + path.split("_")[-1].split("/")[0] + " us")
    print("statistic/metric, delivery latency (us), queueing latency (us), computation latency (us), wait for confirmation (us)")
    print(f"mean, {np.mean(delivery_latency_array):.2f}, {np.mean(queuing_delay_array):.2f}, {np.mean(compute):.2f}, {np.mean(wait_for_confirmation):.2f}")
    print(f"p50, {np.percentile(delivery_latency_array, 50):.2f}, {np.percentile(queuing_delay_array, 50):.2f}, {np.percentile(compute, 50):.2f}, {np.percentile(wait_for_confirmation, 50):.2f}")
    print(f"p99, {np.percentile(delivery_latency_array, 99):.2f}, {np.percentile(queuing_delay_array, 99):.2f}, {np.percentile(compute, 99):.2f}, {np.percentile(wait_for_confirmation, 99):.2f}")
 
path =  "../results/"

for dir in glob.glob(path + "*/"):
    get_append_metrics(dir)
    get_e2e_metrics(dir)