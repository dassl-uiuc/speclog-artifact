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

log_file = '../logs/order-0.log'  
startGSN_values, cuts = parse_log_file(log_file)

data1 = cuts[0][200000:]
data2 = cuts[1][200000:]
data3 = cuts[2][200000:]
data4 = cuts[3][200000:]

differences = [np.diff(data1), np.diff(data2), np.diff(data3), np.diff(data4)]

for i, diff in enumerate(differences):
    print(f'Cut for {i} mean: {np.mean(diff)}')
    print(f'Cut for {i} std: {np.std(diff)}')
    print(f'Cut for {i} max: {np.max(diff)}')
    print(f'Cut for {i} min: {np.min(diff)}')
    print(f'Cut for {i} 99th percentile: {np.percentile(diff, 99)}')
    print(f'Cut for {i} 50th percentile: {np.percentile(diff, 50)}')
    print(f'Cut for {i} frequent entries: {Counter(diff).most_common(4)}')

    print('---')


