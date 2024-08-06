# python script to go over the data logs and get the throughput of holes generated per second

import re
import os
import sys

runtime_in_secs = int(sys.argv[1])
def find_last_hole_num(file_path, pattern):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    for line in reversed(lines):
        if re.search(pattern, line):
            return line.split()[-1]
    
    return None


total = 0
for file in os.listdir('../logs/'):
    if file.startswith('data-'):
        pattern = r'num holes generated: \d+'
        result = find_last_hole_num('../logs/'+file, pattern)
        if result:
            total += int(result)


print(total/runtime_in_secs)

