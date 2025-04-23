import matplotlib.pyplot as plt
import sys
import os 
import numpy as np

num_replicas = 2
num_append_clients_per_replica = 10
num_read_clients_per_replica = 1
num_trials = 3

def largest_common_key(*maps):
    # Find the intersection of all keys across the maps
    common_keys = set(maps[0].keys())
    for m in maps[1:]:
        common_keys.intersection_update(m.keys())
    
    return max(common_keys) if common_keys else None


root_path = sys.argv[1]


def analyze_scalog(root_path):
    analyzing_trial = 1
    list_lat = []
    while analyzing_trial <= num_trials:
        append_throughput_file_path = root_path + str(analyzing_trial) + "/data/append_throughput_"
        append_start_timestamps_file_path = root_path + str(analyzing_trial) + "/data/append_start_timestamps_"
        compute_e2e_end_times_file_path = root_path + str(analyzing_trial) + "/data/compute_e2e_end_times_"
        delivery_latencies_file_path = root_path + str(analyzing_trial) + "/data/delivery_latencies_"
        read_throughput_file_path = root_path + str(analyzing_trial) + "/data/read_throughput_"
        append_records_produced_file_path = root_path + str(analyzing_trial) + "/data/append_records_produced_"
        records_received_file_path = root_path + str(analyzing_trial) + "/data/records_received_"
        start_compute_times_file_path = root_path + str(analyzing_trial) + "/data/start_compute_times_"
        avg_batch_size_file_path = root_path + str(analyzing_trial) + "/data/batch_sizes_"

        records_produced = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_records_produced_file_path_i = append_records_produced_file_path + str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(append_records_produced_file_path_i, 'r') as file:
                    for line in file:
                        records_produced += int(line)

        records_consumed = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                records_received_file_path_i = records_received_file_path + str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(records_received_file_path_i, 'r') as file:
                    for line in file:
                        records_consumed += int(line)

        append_throughput = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_throughput_file_path_i = append_throughput_file_path + str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(append_throughput_file_path_i, "r") as append_num_ops_file:
                    for line in append_num_ops_file:
                        append_throughput += float(line)

        read_throughput = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                read_throughput_file_path_i = read_throughput_file_path + str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(read_throughput_file_path_i, 'r') as file:
                    for line in file:
                        read_throughput += float(line)

        # Start calculating the latencies
        append_start_timestamps = {}
        num_append_timestamps = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_start_timestamps_file_path_i = f"{append_start_timestamps_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(append_start_timestamps_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        append_start_timestamps[int(gsn)] = int(timestamp)
                        num_append_timestamps += 1

        threshold_key = 250000

        compute_e2e_latency = 0
        compute_e2e_latencies_list = []
        compute_e2e_latencies_map = {}
        num_compute_e2e_latencies = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                compute_e2e_end_times_file_path_i = f"{compute_e2e_end_times_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(compute_e2e_end_times_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) < threshold_key:
                            continue
                        compute_e2e_latency += int(timestamp) - append_start_timestamps[int(gsn)]
                        compute_e2e_latencies_list.append(int(timestamp) - append_start_timestamps[int(gsn)])
                        compute_e2e_latencies_map[int(gsn)] = int(timestamp) - append_start_timestamps[int(gsn)]
                        num_compute_e2e_latencies += 1

        avg_compute_e2e_latency = compute_e2e_latency / num_compute_e2e_latencies / 1000

        delivery_e2e_latencies_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                delivery_latencies_file_path_i = f"{delivery_latencies_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(delivery_latencies_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) < threshold_key:
                            continue
                        delivery_e2e_latencies_map[int(gsn)] = int(timestamp)
        
        delivery_e2e_latencies = 0
        delivery_e2e_latencies_list = []
        num_delivery_e2e_latencies = 0

        # largest_key = largest_common_key(append_start_timestamps, delivery_e2e_latencies_map, compute_e2e_latencies_map)
        # threshold = 0.2
        # threshold_key = largest_key*threshold

        for gsn, timestamp in append_start_timestamps.items():
            if gsn < threshold_key:
                continue
            if gsn in delivery_e2e_latencies_map and gsn in compute_e2e_latencies_map:
                delivery_e2e_latencies += delivery_e2e_latencies_map[gsn] - timestamp
                delivery_e2e_latencies_list.append(delivery_e2e_latencies_map[gsn] - timestamp)
                num_delivery_e2e_latencies += 1

        avg_delivery_e2e_latency = delivery_e2e_latencies / num_delivery_e2e_latencies / 1000
        avg_total_e2e_latency = avg_compute_e2e_latency

        # Queuing delay
        compute_start_times_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                start_compute_times_file_path_i = f"{start_compute_times_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(start_compute_times_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) < threshold_key:
                            continue
                        compute_start_times_map[int(gsn)] = int(timestamp)

        queuing_delay = 0
        queuing_delays_list = []
        num_queuing_delays = 0
        for gsn, timestamp in compute_start_times_map.items():
            if gsn in delivery_e2e_latencies_map and gsn in compute_e2e_latencies_map:
                queuing_delay += timestamp - delivery_e2e_latencies_map[gsn]
                queuing_delays_list.append(timestamp - delivery_e2e_latencies_map[gsn])
                num_queuing_delays += 1

        avg_queuing_delay = queuing_delay / num_queuing_delays / 1000

        # calculate std, p50, p99, and p99.99 for compute_e2e_latency,delivery_e2e_latency, and queuing_delay
        import numpy as np
        compute_e2e_latency_std = np.std(compute_e2e_latencies_list) / 1000
        delivery_e2e_latency_std = np.std(delivery_e2e_latencies_list) / 1000
        queuing_delay_std = np.std(queuing_delays_list) / 1000

        p50_compute_e2e_latency = np.percentile(compute_e2e_latencies_list, 50) / 1000
        p99_compute_e2e_latency = np.percentile(compute_e2e_latencies_list, 99) / 1000
        p99_99_compute_e2e_latency = np.percentile(compute_e2e_latencies_list, 99.99) / 1000

        p50_delivery_e2e_latency = np.percentile(delivery_e2e_latencies_list, 50) / 1000
        p99_delivery_e2e_latency = np.percentile(delivery_e2e_latencies_list, 99) / 1000
        p99_99_delivery_e2e_latency = np.percentile(delivery_e2e_latencies_list, 99.99) / 1000

        p50_queuing_delay = np.percentile(queuing_delays_list, 50) / 1000
        p99_queuing_delay = np.percentile(queuing_delays_list, 99) / 1000
        p99_99_queuing_delay = np.percentile(queuing_delays_list, 99.99) / 1000

        # Batch size
        batch_size = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                avg_batch_size_file_path_i = avg_batch_size_file_path + str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(avg_batch_size_file_path_i, 'r') as file:
                    for line in file:
                        batch_size += float(line)

        avg_batch_size = batch_size / num_replicas

        # write compute_e2e_latency_list to a file
        # with open("analytics/compute_e2e_latencies.txt", 'w') as file:
        #     for latency in compute_e2e_latencies_list:
        #         file.write(str(latency) + "\n")

        list_lat.append(avg_total_e2e_latency)

        analyzing_trial += 1

    return list_lat


threshold_key = 250000
def analyze_speclog(root_path):
    analyzing_trial = 1
    list_lat = []
    while analyzing_trial <= num_trials:
        append_throughput_file_path = root_path + str(analyzing_trial) + "/data/append_throughput_"
        append_start_timestamps_file_path = root_path + str(analyzing_trial) + "/data/append_start_timestamps_"
        compute_e2e_end_times_file_path = root_path + str(analyzing_trial) + "/data/compute_e2e_end_times_"
        delivery_latencies_file_path = root_path + str(analyzing_trial) + "/data/delivery_latencies_"
        confirm_latencies_file_path = root_path + str(analyzing_trial) + "/data/confirm_latencies_"
        read_throughput_file_path = root_path + str(analyzing_trial) + "/data/read_throughput_"
        append_records_produced_file_path = root_path + str(analyzing_trial) + "/data/append_records_produced_"
        records_received_file_path = root_path + str(analyzing_trial) + "/data/records_received_"
        start_compute_times_file_path = root_path + str(analyzing_trial) + "/data/start_compute_times_"
        avg_batch_size_file_path = root_path + str(analyzing_trial) + "/data/batch_sizes_"

        records_produced = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_records_produced_file_path_i = append_records_produced_file_path + str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(append_records_produced_file_path_i, 'r') as file:
                    for line in file:
                        records_produced += int(line)

        records_consumed = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                records_received_file_path_i = records_received_file_path +  str(i*2) + "_" + str(j) + ".txt"
                with open(records_received_file_path_i, 'r') as file:
                    for line in file:
                        records_consumed += int(line)

        append_throughput = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_throughput_file_path_i = append_throughput_file_path +  str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(append_throughput_file_path_i, "r") as append_num_ops_file:
                    for line in append_num_ops_file:
                        append_throughput += float(line)

        read_throughput = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                read_throughput_file_path_i = read_throughput_file_path +  str(i*2) + "_" + str(j) + ".txt"
                with open(read_throughput_file_path_i, 'r') as file:
                    for line in file:
                        read_throughput += float(line)

        # Start calculating the latencies
        append_start_timestamps = {}

        gsn_node_map = {}

        num_append_timestamps = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_start_timestamps_file_path_i = f"{append_start_timestamps_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(append_start_timestamps_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        append_start_timestamps[int(gsn)] = int(timestamp)
                        gsn_node_map[int(gsn)] = i
                        num_append_timestamps += 1

        compute_e2e_latency = 0
        compute_e2e_latencies_list = []
        compute_e2e_latencies_map = {}
        num_compute_e2e_latencies = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                compute_e2e_end_times_file_path_i = f"{compute_e2e_end_times_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(compute_e2e_end_times_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) < threshold_key:
                            continue
                        compute_e2e_latency += int(timestamp) - append_start_timestamps[int(gsn)]
                        compute_e2e_latencies_list.append(int(timestamp) - append_start_timestamps[int(gsn)])
                        compute_e2e_latencies_map[int(gsn)] = int(timestamp) - append_start_timestamps[int(gsn)]
                        num_compute_e2e_latencies += 1

        avg_compute_e2e_latency = compute_e2e_latency / num_compute_e2e_latencies / 1000

        delivery_e2e_latencies_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                delivery_latencies_file_path_i = f"{delivery_latencies_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(delivery_latencies_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) < threshold_key:
                            continue
                        delivery_e2e_latencies_map[int(gsn)] = int(timestamp)

        confirm_e2e_latencies_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                confirm_latencies_file_path_i = f"{confirm_latencies_file_path}{i*2}_{j}.txt"
                with open(confirm_latencies_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) < threshold_key:
                            continue
                        if int(gsn) in gsn_node_map and gsn_node_map[int(gsn)] == i:
                            confirm_e2e_latencies_map[int(gsn)] = int(timestamp)
        
        confirm_e2e_duration_map = {}
        confirm_e2e_latencies = 0
        confirm_e2e_latencies_list = []
        num_confirm_e2e_latencies = 0
        delivery_e2e_latencies = 0
        delivery_e2e_latencies_list = []
        num_delivery_e2e_latencies = 0
        total_e2e_latencies = 0
        total_e2e_latencies_list = []
        num_total_e2e_latencies = 0

        for gsn, timestamp in append_start_timestamps.items():
            if gsn < threshold_key:
                continue
            if gsn in delivery_e2e_latencies_map and gsn in confirm_e2e_latencies_map and gsn in compute_e2e_latencies_map:
                delivery_e2e_latencies += delivery_e2e_latencies_map[gsn] - timestamp
                delivery_e2e_latencies_list.append(delivery_e2e_latencies_map[gsn] - timestamp)
                confirm_e2e_latencies += confirm_e2e_latencies_map[gsn] - timestamp
                confirm_e2e_duration_map[gsn] = confirm_e2e_latencies_map[gsn] - timestamp
                confirm_e2e_latencies_list.append(confirm_e2e_latencies_map[gsn] - timestamp)
                num_delivery_e2e_latencies += 1
                num_confirm_e2e_latencies += 1

                total_e2e_latencies += max((confirm_e2e_latencies_map[gsn] - timestamp), compute_e2e_latencies_map[gsn])
                total_e2e_latencies_list.append(max((confirm_e2e_latencies_map[gsn] - timestamp), compute_e2e_latencies_map[gsn]))
                num_total_e2e_latencies += 1

        avg_delivery_e2e_latency = delivery_e2e_latencies / num_delivery_e2e_latencies / 1000
        avg_confirm_e2e_latency = confirm_e2e_latencies / num_confirm_e2e_latencies / 1000
        avg_total_e2e_latency = total_e2e_latencies / num_total_e2e_latencies / 1000

        # Queuing delay
        compute_start_times_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                start_compute_times_file_path_i = f"{start_compute_times_file_path}{i*2 + int(j/5)}_{j}.txt"
                with open(start_compute_times_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) < threshold_key:
                            continue
                        compute_start_times_map[int(gsn)] = int(timestamp)

        queuing_delay = 0
        queuing_delays_list = []
        num_queuing_delays = 0
        for gsn, timestamp in compute_start_times_map.items():
            if gsn in delivery_e2e_latencies_map and gsn in compute_e2e_latencies_map:
                queuing_delay += timestamp - delivery_e2e_latencies_map[gsn]
                queuing_delays_list.append(timestamp - delivery_e2e_latencies_map[gsn])
                num_queuing_delays += 1

        avg_queuing_delay = queuing_delay / num_queuing_delays / 1000

        # calculate std, p50, p99, and p99.99 for compute_e2e_latency,delivery_e2e_latency, and queuing_delay
        import numpy as np
        compute_e2e_latency_std = np.std(compute_e2e_latencies_list) / 1000
        delivery_e2e_latency_std = np.std(delivery_e2e_latencies_list) / 1000
        queuing_delay_std = np.std(queuing_delays_list) / 1000
        confirm_e2e_latency_std = np.std(confirm_e2e_latencies_list) / 1000
        total_e2e_latency_std = np.std(total_e2e_latencies_list) / 1000

        p50_compute_e2e_latency = np.percentile(compute_e2e_latencies_list, 50) / 1000
        p99_compute_e2e_latency = np.percentile(compute_e2e_latencies_list, 99) / 1000
        p99_99_compute_e2e_latency = np.percentile(compute_e2e_latencies_list, 99.99) / 1000

        p50_delivery_e2e_latency = np.percentile(delivery_e2e_latencies_list, 50) / 1000
        p99_delivery_e2e_latency = np.percentile(delivery_e2e_latencies_list, 99) / 1000
        p99_99_delivery_e2e_latency = np.percentile(delivery_e2e_latencies_list, 99.99) / 1000

        p50_queuing_delay = np.percentile(queuing_delays_list, 50) / 1000
        p99_queuing_delay = np.percentile(queuing_delays_list, 99) / 1000
        p99_99_queuing_delay = np.percentile(queuing_delays_list, 99.99) / 1000

        p50_confirm_e2e_latency = np.percentile(confirm_e2e_latencies_list, 50) / 1000
        p99_confirm_e2e_latency = np.percentile(confirm_e2e_latencies_list, 99) / 1000
        p99_99_confirm_e2e_latency = np.percentile(confirm_e2e_latencies_list, 99.99) / 1000

        p50_total_e2e_latency = np.percentile(total_e2e_latencies_list, 50) / 1000
        p99_total_e2e_latency = np.percentile(total_e2e_latencies_list, 99) / 1000
        p99_99_total_e2e_latency = np.percentile(total_e2e_latencies_list, 99.99) / 1000

        # Batch size
        batch_size = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                avg_batch_size_file_path_i = avg_batch_size_file_path + str(i*2 + int(j/5)) + "_" + str(j) + ".txt"
                with open(avg_batch_size_file_path_i, 'r') as file:
                    for line in file:
                        batch_size += float(line)

        avg_batch_size = batch_size / num_replicas

        # write compute_e2e_latency_list to a file
        # with open("analytics/compute_e2e_latencies.txt", 'w') as file:
        #     for latency in compute_e2e_latencies_list:
        #         file.write(str(latency) + "\n")

        list_lat.append(avg_total_e2e_latency)

        analyzing_trial += 1

    return list_lat

scalog_path = os.path.join(root_path, "scalog_")
speclog_path = os.path.join(root_path, "speclog_")
scalog_latencies = analyze_scalog(scalog_path)
speclog_latencies = analyze_speclog(speclog_path)

with open("scalog", "a") as f:
    f.write(f"\"High-Freq \\nTrade\"\t{np.mean(scalog_latencies)}\n")

lines = 0 
if os.path.exists("speclog"):
    with open("speclog", "r") as f:
        lines = len(f.readlines())

with open("speclog", "a") as f:
    f.write(f"\"High-Freq \\nTrade\"\t{np.mean(speclog_latencies):.2f}\t{lines}\t{(np.mean(scalog_latencies)/np.mean(speclog_latencies)):.2f}\n")