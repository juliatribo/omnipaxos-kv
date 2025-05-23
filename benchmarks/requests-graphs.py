import pandas as pd
import matplotlib.pyplot as plt
import os
import matplotlib.dates as mdates

#file_names = ['./logs/example-experiment/MajorityQuorum/client-1.csv', './logs/example-experiment/MajorityQuorum/client-2.csv', './logs/example-experiment/MajorityQuorum/client-3.csv','./logs/example-experiment/MajorityQuorum/client-4.csv','./logs/example-experiment/MajorityQuorum/client-5.csv']
folder = "../build_scripts/logs"
file_names = [folder+'/client-1.csv', folder+'/client-2.csv', folder+'/client-3.csv',folder+'/client-4.csv',folder+'/client-5.csv',folder+'/client-6.csv',folder+'/client-7.csv']
dfs = []

for file_name in file_names:
    try:
        if os.path.exists(file_name):
            df = pd.read_csv(file_name)
            df['request_time'] = pd.to_datetime(df['request_time'], unit='ms')
            df['response_time'] = pd.to_datetime(df['response_time'], unit='ms')
            dfs.append(df)
        else:
            print(f"File not found: {file_name}")
    except Exception as e:
        print(f"Error reading {file_name}: {e}")

combined_df = pd.concat(dfs, ignore_index=True)
combined_df = combined_df.sort_values('request_time')

# delete the rows with NaN values
replied = combined_df.dropna().copy()
replied['latency'] = (replied['response_time'] - replied['request_time']).dt.total_seconds() * 1000

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

ax1.plot(replied['request_time'], replied['latency'])
ax1.set_ylabel('Latency (ms)')
ax1.set_title('Latency and Operations per Second')
ax1.grid(True)

requests_per_second = combined_df.set_index('request_time').resample('1s').size()
combined_df = combined_df.sort_values('response_time')
responses_per_second = combined_df.set_index('response_time').resample('1s').size()

latest_request_time = combined_df['request_time'].max()

ax2.plot(requests_per_second.index, requests_per_second.values, marker='.', label='Requests')
ax2.plot(responses_per_second.index, responses_per_second.values, marker='.', label='Responses')
ax2.set_xlabel('Time (seconds)')
ax2.set_ylabel('Operations per Second')
ax2.grid(True)
ax2.legend()

ax1.set_xlim(left=combined_df['request_time'].min(), right=latest_request_time)
ax2.set_xlim(left=combined_df['request_time'].min(), right=latest_request_time)

ax1.xaxis.set_major_locator(mdates.SecondLocator(interval=10))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
ax2.xaxis.set_major_locator(mdates.SecondLocator(interval=10))
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

plt.xticks(rotation=45)

# Improve layout and show the plot
plt.tight_layout()
#output_dir = "./logs/example-experiment/MajorityQuorum/graphs"
output_dir = "../build_scripts/logs/graphs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "fast-req-rep2")
plt.savefig(output_path)
plt.show()