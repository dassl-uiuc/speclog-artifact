from datetime import datetime, timedelta
import glob
import csv
import pandas as pd
import matplotlib.pyplot as plt

path='../results/1ms/append_bench_40/<hpnode*>_1m_4096_*.csv'

# change this to draw different time range
draw_start = 25
draw_end = 35

all_metric : list[tuple[int, int, datetime]] = []
for file in glob.glob(path):
    print(f'processing {file}')
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                all_metric.append((int(row.get('gsn')), int(row.get('latency(ns)')), datetime.strptime(row.get('runEndTime'), '%H:%M:%S.%f')))
            except e:
                print(f'Invalid value in row: {row}, {e}')
print('sorting..')
all_metric.sort(key=lambda x: x[0])
all_time = [l[2] for l in all_metric]
all_lat = [l[1] for l in all_metric]

df = pd.DataFrame({
    'latency': all_lat,
    'time': all_time,
})
df = df.sort_values(by='time')

start_time = all_time[0] + timedelta(seconds=draw_start-5)
end_time = all_time[0] + timedelta(seconds=draw_end+5)

df['mov_avg'] = df['latency'].rolling(window=10, min_periods=1).mean()/1e6
min_time = df['time'].min()
print(f'min_time: {min_time}')
df['relative_time_s'] = (df['time'] - min_time).dt.total_seconds()

df_zoomed = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

failure_ev : datetime = None
viewchange_ev : datetime = None
with open('../results/logs/1ms/append_bench_40_400/client_node11.log', 'r') as f:
    for line in f.readlines():
        if 'rpc error:' in line and failure_ev is None:
            failure_ev = datetime.strptime(line.split(' ')[2], '%H:%M:%S.%f')
            print(failure_ev)
        elif 'viewID changed to 3' in line and viewchange_ev is None:
            viewchange_ev = datetime.strptime(line.split(' ')[2], '%H:%M:%S.%f')
            print(viewchange_ev)

plot, ax = plt.subplots(figsize=(10,6))
ax.plot(df_zoomed['relative_time_s'], df_zoomed['mov_avg'])
ax.axvline((failure_ev - min_time).total_seconds(), color='brown', alpha=0.7, linestyle='--', label="shard fail")
ax.axvline((viewchange_ev - min_time).total_seconds(), color='crimson', alpha=0.7, linestyle='--', label="view change notified")
ax.set_xlim(left=draw_start, right=draw_end)
ax.set_xlabel('Time (seconds)')
ax.set_ylabel('Append latency (ms)')
ax.set_title('Append Latency change (moving avg, window=10)')
ax.grid()

plot.legend()
plot.tight_layout()
plt.savefig('lat_change.pdf')

tput = []
time : 'list[datetime]' = []
event : 'dict[str, tuple[datetime, str]]' = {}
with open('../results/logs/1ms/append_bench_40_400/order-0.log', 'r') as f:
    for line in f.readlines():
        if '[real-time tput]' in line:
            tput.append(int(line.split(' ')[-2]))
            time.append(datetime.strptime(line.split(' ')[-5], '%H:%M:%S.%f'))
        if '[last cut]'  in line:
            event['last cut report'] = (datetime.strptime(line.split(' ')[-1].rstrip(), '%H:%M:%S.%f'), 'red')
        elif '[lag detected]' in line:
            event['significant lag detected'] = (datetime.strptime(line.split(' ')[-1].rstrip(), '%H:%M:%S.%f'), 'green')
        elif '[fail detected]' in line:
            print(line.split(' ')[-1])
            event['shard failure detected'] = (datetime.strptime(line.split(' ')[-1].rstrip(), '%H:%M:%S.%f'), 'purple')
        elif '[cut commit]' in line:
            event['notify data servers'] = (datetime.strptime(line.split(' ')[-1].rstrip(), '%H:%M:%S.%f'), 'orange')

start_time = min_time
time_in_s = [(t - start_time).total_seconds() for t in time]

plot, ax = plt.subplots(figsize=(10, 6))
ax.plot(time_in_s, tput)
ax.set_xlim(left=draw_start, right=draw_end)
ax.grid(True)
ax.set_xlabel("Time (seconds)")
ax.set_ylabel("Throughput (ops/sec)")
ax.set_title("Throughput change")


for e, t in sorted(event.items(), key=lambda it: it[1][0]):  # sort by time
    print(e, t[0])
    ax.axvline((t[0]-start_time).total_seconds(), color=t[1], alpha=0.7, linestyle='--', label=e)


plot.legend()
plot.tight_layout()
plot.savefig('tput_change.pdf')