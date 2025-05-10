import matplotlib.pyplot as plt

num_replicas = 4
num_append_clients_per_replica = 10
num_read_clients_per_replica = 1
num_trials = 1
gsn_threshold = 250000
def analyze():
    analyzing_trial = 1
    while analyzing_trial <= num_trials:
        append_throughput_file_path = "data/append_throughput_"
        append_start_timestamps_file_path = "data/append_start_timestamps_"
        compute_e2e_end_times_file_path = "data/compute_e2e_end_times_"
        delivery_latencies_file_path = "data/delivery_latencies_"
        confirm_latencies_file_path = "data/confirm_latencies_"
        read_throughput_file_path = "data/read_throughput_"
        append_records_produced_file_path = "data/append_records_produced_"
        records_received_file_path = "data/records_received_"
        stats_file_path = "analytics/stats_trial_" + str(analyzing_trial) + ".txt"
        start_compute_times_file_path = "data/start_compute_times_"
        avg_batch_size_file_path = "data/batch_sizes_"

        records_produced = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_records_produced_file_path_i = append_records_produced_file_path + str(i) + "_" + str(j) + ".txt"
                with open(append_records_produced_file_path_i, 'r') as file:
                    for line in file:
                        records_produced += int(line)

        records_consumed = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                records_received_file_path_i = records_received_file_path + str(i) + "_" + str(j) + ".txt"
                with open(records_received_file_path_i, 'r') as file:
                    for line in file:
                        records_consumed += int(line)

        append_throughput = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_throughput_file_path_i = append_throughput_file_path + str(i) + "_" + str(j) + ".txt"
                with open(append_throughput_file_path_i, "r") as append_num_ops_file:
                    for line in append_num_ops_file:
                        append_throughput += float(line)

        read_throughput = 0
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                read_throughput_file_path_i = read_throughput_file_path + str(i) + "_" + str(j) + ".txt"
                with open(read_throughput_file_path_i, 'r') as file:
                    for line in file:
                        read_throughput += float(line)

        # Start calculating the latencies
        append_start_timestamps = {}
        gsn_node_map = {}
        num_append_timestamps = 0
        for i in range(num_replicas):
            for j in range(num_append_clients_per_replica):
                append_start_timestamps_file_path_i = f"{append_start_timestamps_file_path}{i}_{j}.txt"
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
                compute_e2e_end_times_file_path_i = f"{compute_e2e_end_times_file_path}{i}_{j}.txt"
                with open(compute_e2e_end_times_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) > gsn_threshold:
                            compute_e2e_latency += int(timestamp) - append_start_timestamps[int(gsn)]
                            compute_e2e_latencies_list.append(int(timestamp) - append_start_timestamps[int(gsn)])
                            compute_e2e_latencies_map[int(gsn)] = int(timestamp) - append_start_timestamps[int(gsn)]
                            num_compute_e2e_latencies += 1

        avg_compute_e2e_latency = compute_e2e_latency / num_compute_e2e_latencies / 1000

        delivery_e2e_latencies_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                delivery_latencies_file_path_i = f"{delivery_latencies_file_path}{i}_{j}.txt"
                with open(delivery_latencies_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) > gsn_threshold:
                            delivery_e2e_latencies_map[int(gsn)] = int(timestamp)
        
        delivery_e2e_latencies_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                delivery_latencies_file_path_i = f"{delivery_latencies_file_path}{i}_{j}.txt"
                with open(delivery_latencies_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) > gsn_threshold:
                            delivery_e2e_latencies_map[int(gsn)] = int(timestamp)

        confirm_e2e_latencies_map = {}
        for i in range(num_replicas):
            for j in range(num_read_clients_per_replica):
                confirm_latencies_file_path_i = f"{confirm_latencies_file_path}{i}_{j}.txt"
                with open(confirm_latencies_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) > gsn_threshold:
                            if int(gsn) in gsn_node_map and gsn_node_map[int(gsn)] == i:
                                confirm_e2e_latencies_map[int(gsn)] = int(timestamp)
        
        confirm_e2e_latencies = 0
        confirm_e2e_latencies_list = []
        confirm_e2e_duration_map = {}
        num_confirm_e2e_latencies = 0
        delivery_e2e_latencies = 0
        delivery_e2e_latencies_list = []
        delivery_e2e_duration_map = {}
        num_delivery_e2e_latencies = 0
        total_e2e_latencies = 0
        total_e2e_latencies_list = []
        total_e2e_duration_map = {}
        num_total_e2e_latencies = 0
        for gsn, timestamp in append_start_timestamps.items():
            if gsn in delivery_e2e_latencies_map and gsn in confirm_e2e_latencies_map and gsn in compute_e2e_latencies_map:
                delivery_e2e_latencies += delivery_e2e_latencies_map[gsn] - timestamp
                delivery_e2e_latencies_list.append(delivery_e2e_latencies_map[gsn] - timestamp)
                delivery_e2e_duration_map[gsn] = delivery_e2e_latencies_map[gsn] - timestamp
                confirm_e2e_latencies += confirm_e2e_latencies_map[gsn] - timestamp
                confirm_e2e_latencies_list.append(confirm_e2e_latencies_map[gsn] - timestamp)
                confirm_e2e_duration_map[gsn] = confirm_e2e_latencies_map[gsn] - timestamp
                num_delivery_e2e_latencies += 1
                num_confirm_e2e_latencies += 1

                total_e2e_latencies += max((confirm_e2e_latencies_map[gsn] - timestamp), compute_e2e_latencies_map[gsn])
                total_e2e_latencies_list.append(max((confirm_e2e_latencies_map[gsn] - timestamp), compute_e2e_latencies_map[gsn]))
                total_e2e_duration_map[gsn] = max((confirm_e2e_latencies_map[gsn] - timestamp), compute_e2e_latencies_map[gsn])
                num_total_e2e_latencies += 1

        avg_delivery_e2e_latency = delivery_e2e_latencies / num_delivery_e2e_latencies / 1000
        avg_confirm_e2e_latency = confirm_e2e_latencies / num_confirm_e2e_latencies / 1000
        avg_total_e2e_latency = total_e2e_latencies / num_total_e2e_latencies / 1000

        # Queuing delay
        compute_start_times_map = {}
        for i in range(num_replicas): 
            for j in range(num_read_clients_per_replica):
                start_compute_times_file_path_i = f"{start_compute_times_file_path}{i}_{j}.txt"
                with open(start_compute_times_file_path_i, 'r') as file:
                    for line in file:
                        gsn, timestamp = line.strip().split(",")
                        if int(gsn) > gsn_threshold:
                            compute_start_times_map[int(gsn)] = int(timestamp)

        queuing_delay = 0
        queuing_delays_list = []
        queueing_delays_map = {}
        num_queuing_delays = 0
        for gsn, timestamp in compute_start_times_map.items():
            if gsn in delivery_e2e_latencies_map and gsn in compute_e2e_latencies_map:
                queuing_delay += timestamp - delivery_e2e_latencies_map[gsn]
                queuing_delays_list.append(timestamp - delivery_e2e_latencies_map[gsn])
                queueing_delays_map[gsn] = timestamp - delivery_e2e_latencies_map[gsn]
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
                avg_batch_size_file_path_i = avg_batch_size_file_path + str(i) + "_" + str(j) + ".txt"
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
            file.write("confirm_e2e_latency in microseconds: " + str(avg_confirm_e2e_latency) + "\n")
            file.write("total_e2e_latency in microseconds: " + str(avg_total_e2e_latency) + "\n")
            file.write("queuing_delay in microseconds: " + str(avg_queuing_delay) + "\n")
            file.write("append_throughput (ops/s): " + str(append_throughput) + "\n")
            file.write("read_throughput (ops/s): " + str(read_throughput) + "\n")
            file.write("records_produced: " + str(records_produced) + "\n")
            file.write("records_consumed: " + str(records_consumed) + "\n")
            file.write("num append timestamps: " + str(num_append_timestamps) + "\n")
            file.write("num delivery e2e latencies: " + str(num_delivery_e2e_latencies) + "\n")
            file.write("num confirm e2e latencies: " + str(num_confirm_e2e_latencies) + "\n")
            file.write("num total e2e latencies: " + str(num_total_e2e_latencies) + "\n")
            file.write("num queuing delays: " + str(num_queuing_delays) + "\n")
            file.write("num compute e2e latencies: " + str(num_compute_e2e_latencies) + "\n")
            file.write("std compute_e2e_latency: " + str(compute_e2e_latency_std) + "\n")
            file.write("std delivery_e2e_latency: " + str(delivery_e2e_latency_std) + "\n")
            file.write("std queuing_delay: " + str(queuing_delay_std) + "\n")
            file.write("std confirm_e2e_latency: " + str(confirm_e2e_latency_std) + "\n")
            file.write("std total_e2e_latency: " + str(total_e2e_latency_std) + "\n")
            file.write("p50 compute_e2e_latency: " + str(p50_compute_e2e_latency) + "\n")
            file.write("p99 compute_e2e_latency: " + str(p99_compute_e2e_latency) + "\n")
            file.write("p99.99 compute_e2e_latency: " + str(p99_99_compute_e2e_latency) + "\n")
            file.write("p50 delivery_e2e_latency: " + str(p50_delivery_e2e_latency) + "\n")
            file.write("p99 delivery_e2e_latency: " + str(p99_delivery_e2e_latency) + "\n")
            file.write("p99.99 delivery_e2e_latency: " + str(p99_99_delivery_e2e_latency) + "\n")
            file.write("p50 queuing_delay: " + str(p50_queuing_delay) + "\n")
            file.write("p99 queuing_delay: " + str(p99_queuing_delay) + "\n")
            file.write("p99.99 queuing_delay: " + str(p99_99_queuing_delay) + "\n")
            file.write("p50 confirm_e2e_latency: " + str(p50_confirm_e2e_latency) + "\n")
            file.write("p99 confirm_e2e_latency: " + str(p99_confirm_e2e_latency) + "\n")
            file.write("p99.99 confirm_e2e_latency: " + str(p99_99_confirm_e2e_latency) + "\n")
            file.write("p50 total_e2e_latency: " + str(p50_total_e2e_latency) + "\n")
            file.write("p99 total_e2e_latency: " + str(p99_total_e2e_latency) + "\n")
            file.write("p99.99 total_e2e_latency: " + str(p99_99_total_e2e_latency) + "\n")
            file.write("avg_batch_size: " + str(avg_batch_size) + "\n")

            sorted_confirm_items = sorted(confirm_e2e_duration_map.items())

            # Extracting the sorted keys and values
            confirm_keys = [item[0] for item in sorted_confirm_items]
            confirm_values = [item[1] for item in sorted_confirm_items]

            # Plotting the sorted data
            plt.figure(figsize=(8, 6))
            plt.plot(confirm_keys, confirm_values, marker='o', linestyle='-', color='b')
            plt.title('Confirm E2E Duration Map (Sorted by GSN)')
            plt.xlabel('GSN (Key)')
            plt.ylabel('Duration (Value)')
            plt.grid(True)
            plt.savefig('confirm_e2e_duration_map_' + str(analyzing_trial) + '.png')

            sorted_delivery_items = sorted(delivery_e2e_duration_map.items())

            # Extracting the sorted keys and values
            delivery_keys = [item[0] for item in sorted_delivery_items]
            delivery_values = [item[1] for item in sorted_delivery_items]

            # Plotting the sorted data
            plt.figure(figsize=(8, 6))
            plt.plot(delivery_keys, delivery_values, marker='o', linestyle='-', color='b')
            plt.title('Delivery E2E Duration Map (Sorted by GSN)')
            plt.xlabel('GSN (Key)')
            plt.ylabel('Duration (Value)')
            plt.grid(True)
            plt.savefig('delivery_e2e_duration_map_' + str(analyzing_trial) + '.png')

            sorted_total_items = sorted(total_e2e_duration_map.items())

            # Extracting the sorted keys and values
            total_keys = [item[0] for item in sorted_total_items]
            total_values = [item[1] for item in sorted_total_items]

            # Plotting the sorted data
            plt.figure(figsize=(8, 6))
            plt.plot(total_keys, total_values, marker='o', linestyle='-', color='b')
            plt.title('Total E2E Duration Map (Sorted by GSN)')
            plt.xlabel('GSN (Key)')
            plt.ylabel('Duration (Value)')
            plt.grid(True)
            plt.savefig('total_e2e_duration_map_' + str(analyzing_trial) + '.png')

            sorted_compute_items = sorted(compute_e2e_latencies_map.items())

            # Extracting the sorted keys and values
            compute_keys = [item[0] for item in sorted_compute_items]
            compute_values = [item[1] for item in sorted_compute_items]

            # Plotting the sorted data
            plt.figure(figsize=(8, 6))
            plt.plot(compute_keys, compute_values, marker='o', linestyle='-', color='b')
            plt.title('Compute E2E Latency Map (Sorted by GSN)')
            plt.xlabel('GSN (Key)')
            plt.ylabel('Latency (Value)')
            plt.grid(True)
            plt.savefig('compute_e2e_latency_map_' + str(analyzing_trial) + '.png')

            sorted_queueing_items = sorted(queueing_delays_map.items())

            # Extracting the sorted keys and values
            queueing_keys = [item[0] for item in sorted_queueing_items]
            queueing_values = [item[1] for item in sorted_queueing_items]

            # Plotting the sorted data
            plt.figure(figsize=(8, 6))
            plt.plot(queueing_keys, queueing_values, marker='o', linestyle='-', color='b')
            plt.title('Queuing Delay Map (Sorted by GSN)')
            plt.xlabel('GSN (Key)')
            plt.ylabel('Delay (Value)')
            plt.grid(True)
            plt.savefig('queuing_delay_map_' + str(analyzing_trial) + '.png')

        analyzing_trial += 1

analyze()