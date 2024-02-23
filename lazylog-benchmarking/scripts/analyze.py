import os
import csv

def calculate_avg_throughput_latency(directory, num_bytes_per_op):
    total_bytes_sum = 0
    max_total_time = 0
    total_time_sum = 0
    num_ops_total = 0

    # Iterate through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)

            # Read the first line from each CSV file
            with open(file_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)
                first_line = next(csv_reader, None)

            if first_line:
                # Extract relevant information
                total_bytes = int(first_line[5])
                total_time = int(first_line[6])

                # Update sum of totalBytes and max_total_time
                total_bytes_sum += total_bytes
                max_total_time = max(max_total_time, total_time)
                total_time_sum += total_time
                num_ops_total += total_bytes/num_bytes_per_op

    # Calculate average throughput
    if max_total_time > 0:
        avg_throughput = total_bytes_sum / max_total_time * 1e7
        avg_latency = total_time_sum * 1e-6 / num_ops_total
        return (avg_throughput, avg_latency)
    else:
        return 0

clients = [1, 2, 4, 6, 8, 10, 12, 16, 20]
throughput = []
latency = []

for n in clients:
    # Specify the directory path
    directory_path = "../append_bench_" + str(n)

    # Calculate and print the average throughput
    average_throughput, average_latency = calculate_avg_throughput_latency(directory_path, 100)
    throughput.append(average_throughput)
    latency.append(average_latency)
    print(f"{n} clients\n\taverage throughput: {average_throughput} ops/sec\n\taverage latency: {average_latency} msec/op")

import matplotlib.pyplot as plt

# Given data
clients = [1, 2, 4, 6, 8, 10, 12, 16, 20]

# Plotting latency vs. throughput
plt.figure(figsize=(8, 6))
plt.plot(latency, throughput, marker='o')
plt.title('Latency vs. Throughput')
plt.xlabel('Average Latency (msec/op)')
plt.ylabel('Average Throughput (ops/sec)')

# Display the plots
plt.tight_layout()
# Save the plots to a file
plt.savefig('throughput_latency_plot.png')