num_replicas = 2
num_append_clients_per_replica = 10
num_read_clients_per_replica = 1
num_trials = 5

def largest_common_key(*maps):
    # Find the intersection of all keys across the maps
    common_keys = set(maps[0].keys())
    for m in maps[1:]:
        common_keys.intersection_update(m.keys())
    
    return max(common_keys) if common_keys else None

def analyze():
    analyzing_trial = 1
    while analyzing_trial <= num_trials:
        append_throughput_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/append_throughput_"
        append_start_timestamps_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/append_start_timestamps_"
        compute_e2e_end_times_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/compute_e2e_end_times_"
        delivery_latencies_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/delivery_latencies_"
        read_throughput_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/read_throughput_"
        append_records_produced_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/append_records_produced_"
        records_received_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/records_received_"
        stats_file_path = "analytics/stats_trial_" + str(analyzing_trial) + ".txt"
        start_compute_times_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/start_compute_times_"
        avg_batch_size_file_path = "analytics/hft_run_" + str(analyzing_trial) + "/batch_sizes_"

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

        with open(stats_file_path, 'w') as file:
            file.write("compute_e2e_latency in microseconds: " + str(avg_compute_e2e_latency) + "\n")
            file.write("shared_log_e2e_latency in microseconds: " + str(avg_delivery_e2e_latency) + "\n")
            file.write("total_e2e_latency in microseconds: " + str(avg_total_e2e_latency) + "\n")
            file.write("queuing_delay in microseconds: " + str(avg_queuing_delay) + "\n")
            file.write("append_throughput (ops/s): " + str(append_throughput) + "\n")
            file.write("read_throughput (ops/s): " + str(read_throughput) + "\n")
            file.write("records_produced: " + str(records_produced) + "\n")
            file.write("records_consumed: " + str(records_consumed) + "\n")
            file.write("num append timestamps: " + str(num_append_timestamps) + "\n")
            file.write("num delivery e2e latencies: " + str(num_delivery_e2e_latencies) + "\n")
            file.write("num queuing delays: " + str(num_queuing_delays) + "\n")
            file.write("num compute e2e latencies: " + str(num_compute_e2e_latencies) + "\n")
            file.write("std compute_e2e_latency: " + str(compute_e2e_latency_std) + "\n")
            file.write("std delivery_e2e_latency: " + str(delivery_e2e_latency_std) + "\n")
            file.write("std queuing_delay: " + str(queuing_delay_std) + "\n")
            file.write("p50 compute_e2e_latency: " + str(p50_compute_e2e_latency) + "\n")
            file.write("p99 compute_e2e_latency: " + str(p99_compute_e2e_latency) + "\n")
            file.write("p99.99 compute_e2e_latency: " + str(p99_99_compute_e2e_latency) + "\n")
            file.write("p50 delivery_e2e_latency: " + str(p50_delivery_e2e_latency) + "\n")
            file.write("p99 delivery_e2e_latency: " + str(p99_delivery_e2e_latency) + "\n")
            file.write("p99.99 delivery_e2e_latency: " + str(p99_99_delivery_e2e_latency) + "\n")
            file.write("p50 queuing_delay: " + str(p50_queuing_delay) + "\n")
            file.write("p99 queuing_delay: " + str(p99_queuing_delay) + "\n")
            file.write("p99.99 queuing_delay: " + str(p99_99_queuing_delay) + "\n")
            file.write("avg_batch_size: " + str(avg_batch_size) + "\n")

        stats = {
            "Mean": [
                avg_delivery_e2e_latency,  # Delivery latency mean
                avg_compute_e2e_latency,  # E2E latency mean
                avg_queuing_delay,        # Queuing delay mean
            ],
            "Std": [
                delivery_e2e_latency_std,  # Delivery latency std
                compute_e2e_latency_std,  # E2E latency std
                queuing_delay_std,        # Queuing delay std
            ],
            "P50": [
                p50_delivery_e2e_latency,  # Delivery latency p50
                p50_compute_e2e_latency,  # E2E latency p50
                p50_queuing_delay,        # Queuing delay p50
            ],
            "P99": [
                p99_delivery_e2e_latency,  # Delivery latency p99
                p99_compute_e2e_latency,  # E2E latency p99
                p99_queuing_delay,        # Queuing delay p99
            ],
            "P99.99": [
                p99_99_delivery_e2e_latency,  # Delivery latency p99.99
                p99_99_compute_e2e_latency,  # E2E latency p99.99
                p99_99_queuing_delay,        # Queuing delay p99.99
            ],
        }

        # Column headers
        columns = [
            "Statistic/Metric",
            "Delivery Latency (us)",
            "E2E Latency (us)",
            "Queuing Delay (us)",
        ]

        # Print header
        print(",".join(columns))

        # Print each row of statistics
        for stat_name, values in stats.items():
            row = [str(value) for value in values]
            print(",".join(row))
        print(" ")

        analyzing_trial += 1

analyze()