import os
import re
import fnmatch

def average_emulation_metrics(directory):
    """
    Calculates the average of the `mean`, `p50`, and `p99` metrics across all files in a directory matching the pattern `data*.log`.

    Args:
        directory (str): Path to the directory containing log files.

    Returns:
        dict: A dictionary containing the averages of `mean`, `p50`, and `p99` metrics.
    """
    data_regex = re.compile(r"(mean|p50|p99),\s*([\d.e+-]+),\s*([\d.e+-]+)")
    
    append_metrics = {"mean": [], "p50": [], "p99": []}
    delivery_metrics = {"mean": [], "p50": [], "p99": []}
    tput_metrics = {"mean": []}

    for filename in os.listdir(directory):
        if fnmatch.fnmatch(filename, 'data*.log'):  
            filepath = os.path.join(directory, filename)
            
            # Temporary dictionaries to track the last occurrence in the current file
            last_append_metrics = {}
            last_delivery_metrics = {}
            last_tput = {}

            with open(filepath, 'r') as file:
                for line in file:
                    match = data_regex.search(line)
                    if match:
                        metric, append_latency, delivery_latency = match.groups()
                        # Update the last seen values for the current file
                        last_append_metrics[metric] = float(append_latency)
                        last_delivery_metrics[metric] = float(delivery_latency)
                    if "tput:" in line:
                        tput = float(line.split()[-1])
                        last_tput["mean"] = tput

            # Append the last occurrence values from the current file to the global lists
            for metric in append_metrics.keys():
                if metric in last_append_metrics:
                    append_metrics[metric].append(last_append_metrics[metric])
                if metric in last_delivery_metrics:
                    delivery_metrics[metric].append(last_delivery_metrics[metric])
            if "mean" in last_tput:
                tput_metrics["mean"].append(last_tput["mean"])


    # Compute averages across all files for the last occurrences
    averages = {
        "append": {
            metric: sum(values) / len(values) if values else None
            for metric, values in append_metrics.items()
        },
        "delivery": {
            metric: sum(values) / len(values) if values else None
            for metric, values in delivery_metrics.items()
        },
        "tput": {
            metric: sum(values) if values else None
            for metric, values in tput_metrics.items()
        }
    }
    print("found values across " + str(len(append_metrics["mean"])) + " files")
    
    return averages

for num_shards in [8]:
    # Define input directory
    input_directory = "../results/emulation_" + str(num_shards) + "/"

    print(f"\nAverage metrics for {num_shards} shards:")
    # Calculate averages
    averages = average_emulation_metrics(input_directory)

    # Print the results
    print("append/confirmation latency (us):")
    print(f"mean: {averages['append']['mean']}")
    print(f"p50: {averages['append']['p50']}")
    print(f"p99: {averages['append']['p99']}")

    print("delivery latency (us):")
    print(f"mean: {averages['delivery']['mean']}")
    print(f"p50: {averages['delivery']['p50']}")
    print(f"p99: {averages['delivery']['p99']}")

    print("throughput (ops/sec):")
    print(f"mean: {averages['tput']['mean']}")