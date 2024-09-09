import re
import numpy as np
from scipy import stats as st
from collections import Counter


def parse_log_file(log_file):
    startGSN_values = []
    cuts = {}

    pattern = r'startGSN:(\d+).*?cut:<(.*?)>'

    with open(log_file, 'r') as file:
        for line in file:
            startGSN_match = re.search(r'startGSN:(\d+)', line)
            if startGSN_match:
                startGSN = int(startGSN_match.group(1))
                startGSN_values.append(startGSN)

            cut_match = re.findall(r'cut:<key:(\d+) value:(\d+) >', line)
            if cut_match:
                for key, value in cut_match:
                    if int(key) not in cuts:
                        cuts[int(key)] = []
                    cuts[int(key)].append(int(value))

    return startGSN_values, cuts

def get_metrics(log_file):
    latencies = []
    cuts = []
    gctime = []
    queuelen = []
    with open(log_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if "replication latency:" in line:
                lat = line.split()[-1]
                latencies.append(int(lat))
            if "records sent:" in line:
                cut = line.split()[-1]
                cuts.append(int(cut))
            if "time since last committed cut: " in line:
                gc = line.split()[-1]
                gctime.append(int(gc))
            if "queue length: " in line:
                ql = line.split()[-1]
                queuelen.append(int(ql))

    return np.array(latencies), np.array(cuts), np.array(queuelen), np.array(gctime)


log_file = '../speclog_40K_100c/data-0-0.log'  
startGSN_values, cuts = parse_log_file(log_file)

data1 = cuts[0]
data2 = cuts[1]

differences = [np.diff(data1), np.diff(data2)]

for i, diff in enumerate(differences):
    print(f'Cut for {i} mean: {np.mean(diff)}')
    print(f'Cut for {i} std: {np.std(diff)}')
    print(f'Cut for {i} max: {np.max(diff)}')
    print(f'Cut for {i} min: {np.min(diff)}')
    print(f'Cut for {i} 99th percentile: {np.percentile(diff, 99)}')
    print(f'Cut for {i} 50th percentile: {np.percentile(diff, 50)}')
    print(f'Cut for {i} frequent entries: {Counter(diff).most_common(4)}')

    print('---')


latencies, cuts, queuelen, gctime = get_metrics(log_file)

print(f'mean latency ns: {np.mean(latencies)}')
print(f'std latency ns: {np.std(latencies)}')
print(f'p99 latency ns: {np.max(latencies)}')

print(f'mean local cut: {np.mean(cuts)}')
print(f'std local cut: {np.std(cuts)}')
print(f'max local cut: {np.max(cuts)}')
print(f'p99 local cut: {np.percentile(cuts, 99)}')

print(f'mean gctime: {np.mean(gctime)}')
print(f'std gctime: {np.std(gctime)}')
print(f'max gctime: {np.max(gctime)}')
print(f'p99 gctime: {np.percentile(gctime, 99)}')

print(f'mean queuelen: {np.mean(queuelen)}')
print(f'std queuelen: {np.std(queuelen)}')
print(f'max queuelen: {np.max(queuelen)}')
print(f'p99 queuelen: {np.percentile(queuelen, 99)}')