from datetime import datetime, timedelta
import glob
import csv
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

results_dir = os.getenv("results_dir")

failure_ev : datetime = None
with open('{0}/app_failure_mask/e2e_1500/data-0-1.log'.format(results_dir), 'r') as f:
    for line in f.readlines():
        if 'killing backup for shard 0' in line and failure_ev is None:
            failure_ev = datetime.strptime(line.split('/')[2].split(' ')[1], '%H:%M:%S.%f')
            print(failure_ev)



path='{0}/app_failure_mask/e2e_1500/e2e_metrics_*.csv'.format(results_dir)

all_e2e_metric : list[tuple[int, int, datetime]] = []
for file in glob.glob(path):
    print(f'processing {file}')
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                all_e2e_metric.append((int(row.get('gsn')), max(int(row.get('e2e latency (us)')), int(row.get('recompute latency (us)'))), datetime.strptime(row.get('start time'), '%H:%M:%S.%f')))
            except Exception as e:
                print(f'Invalid value in row: {row}, {e}')

path='{0}/app_failure_mask/e2e_1500/append_metrics_*.csv'.format(results_dir)
all_append_metric : list[tuple[int, int]] = []
for file in glob.glob(path):
    print(f'processing {file}')
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                all_append_metric.append((int(row.get('gsn')), int(row.get('latency (us)'))))
            except Exception as e:
                print(f'Invalid value in row: {row}, {e}')

all_e2e_metric.sort(key=lambda x: x[0])
all_append_metric.sort(key=lambda x: x[0])
all_time = [l[2] for l in all_e2e_metric]
all_e2e_lat = [l[1] for l in all_e2e_metric]
all_append_lat = [l[1] for l in all_append_metric]

df = pd.DataFrame({
    'e2e_latency': all_e2e_lat,
    'app_latency': all_append_lat,
    'time': all_time,
})
df = df.sort_values(by='time')


df['e2e_mov_avg'] = df['e2e_latency'].rolling(window=10, min_periods=1).mean()/1e3
df['app_mov_avg'] = df['app_latency'].rolling(window=10, min_periods=1).mean()/1e3
min_time = df['time'].min()
df['relative_time_s'] = (df['time'] - min_time).dt.total_seconds()

zoomstart = int((failure_ev - min_time).total_seconds())
zoom = (zoomstart-1, zoomstart+2)

start_time = all_time[0] + timedelta(seconds=zoom[0])
end_time = all_time[0] + timedelta(seconds=zoom[1])

df_zoomed = df[(df['time'] >= start_time) & (df['time'] <= end_time)]



with open("e2elat_mask", 'w') as f:
    for key in df_zoomed['relative_time_s'].keys():
        f.write(str(df_zoomed['relative_time_s'][key]) + "\t" + str(df_zoomed['e2e_mov_avg'][key]) + "\n")

print("replica failure:" + str((failure_ev - min_time).total_seconds()))

tput = []
time : 'list[datetime]' = []
event : 'dict[str, tuple[datetime, str]]' = {}
with open('{0}/app_failure_mask/e2e_1500/order-0.log'.format(results_dir), 'r') as f:
    for line in f.readlines():
        if '[real-time tput]' in line:
            tput.append(int(line.split(' ')[-2]))
            time.append(datetime.strptime(line.split(' ')[-5], '%H:%M:%S.%f'))

start_time = min_time
time_in_s = [(t - start_time).total_seconds() for t in time]

assert len(time_in_s) == len(tput)

with open("tput_mask", 'w') as f:
    for i in range(0, len(tput)):
        if time_in_s[i] >= zoom[0] and time_in_s[i] <= zoom[1]:
            f.write(str(time_in_s[i]) + "\t" + str(tput[i]) + "\n")