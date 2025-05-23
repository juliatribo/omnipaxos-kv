import pandas as pd
import matplotlib.pyplot as plt
import os

folder_o = "../build_scripts/logs/omni-rl"
folder_s = "../build_scripts/logs/spaxos-rl"
#folder_f = "../build_scripts/logs/fast-rl-2"
omni_names = [folder_o+'/client-1.csv', folder_o+'/client-2.csv', folder_o+'/client-3.csv',
              folder_o+'/client-4.csv', folder_o+'/client-5.csv', folder_o+'/client-6.csv', folder_o+'/client-7.csv']
spax_names = [folder_s+'/client-1.csv', folder_s+'/client-2.csv', folder_s+'/client-3.csv',
              folder_s+'/client-4.csv', folder_s+'/client-5.csv', folder_s+'/client-6.csv', folder_s+'/client-7.csv']
#fast_names = [folder_f+'/client-1.csv', folder_f+'/client-2.csv', folder_f+'/client-3.csv',
#              folder_f+'/client-4.csv', folder_f+'/client-5.csv', folder_f+'/client-6.csv', folder_f+'/client-7.csv']

# Read OmniPaxos data
dfs_omni = []
for file_name in omni_names:
    try:
        if os.path.exists(file_name):
            df = pd.read_csv(file_name)
            df['request_time'] = pd.to_datetime(df['request_time'], unit='ms')
            df['response_time'] = pd.to_datetime(df['response_time'], unit='ms')
            dfs_omni.append(df)
        else:
            print(f"File not found: {file_name}")
    except Exception as e:
        print(f"Error reading {file_name}: {e}")

combined_df_omni = pd.concat(dfs_omni, ignore_index=True)

# Read SPaxos data
dfs_spax = []
for file_name in spax_names:
    try:
        if os.path.exists(file_name):
            df = pd.read_csv(file_name)
            df['request_time'] = pd.to_datetime(df['request_time'], unit='ms')
            df['response_time'] = pd.to_datetime(df['response_time'], unit='ms')
            dfs_spax.append(df)
        else:
            print(f"File not found: {file_name}")
    except Exception as e:
        print(f"Error reading {file_name}: {e}")

combined_df_spax = pd.concat(dfs_spax, ignore_index=True)

# Read FastPaxos data
#dfs_fast = []
#for file_name in fast_names:
#    try:
#        if os.path.exists(file_name):
#            df = pd.read_csv(file_name)
#            df['request_time'] = pd.to_datetime(df['request_time'], unit='ms')
#            df['response_time'] = pd.to_datetime(df['response_time'], unit='ms')
#            dfs_fast.append(df)
#        else:
#            print(f"File not found: {file_name}")
#    except Exception as e:
#        print(f"Error reading {file_name}: {e}")
#combined_df_fast = pd.concat(dfs_fast, ignore_index=True)

# Calculate requests and responses per second for SPaxos
requests_per_second_spax = combined_df_spax.set_index('request_time').resample('1s').size()
responses_per_second_spax = combined_df_spax.set_index('response_time').resample('1s').size()

# Calculate responses per second for OmniPaxos
responses_per_second_omni = combined_df_omni.set_index('response_time').resample('1s').size()

# Calculate responses per second for FastPaxos
#responses_per_second_fast = combined_df_fast.set_index('response_time').resample('1s').size()

# Plotting
plt.figure(figsize=(12, 6))
plt.title("OmniPaxos vs SPaxos with rate limiter")
plt.plot(requests_per_second_spax.index - combined_df_spax['request_time'].min(), requests_per_second_spax.values, marker='.', label='Requests')
plt.plot(responses_per_second_spax.index - combined_df_spax['request_time'].min(), responses_per_second_spax.values, marker=',', label='Responses SPaxos')
plt.plot(responses_per_second_omni.index - combined_df_omni['request_time'].min(), responses_per_second_omni.values, marker=',', label='Responses OmniPaxos')
#plt.plot(responses_per_second_fast.index - combined_df_fast['request_time'].min(), responses_per_second_fast.values, marker=',', label='Responses FastPaxos')
plt.grid(True)
plt.legend()

plt.xticks(rotation=45)

output_dir = "../build_scripts/logs/graphs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "omni-vs-spax-rl.png")
plt.savefig(output_path)
plt.show()
