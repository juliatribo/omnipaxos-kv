from pathlib import Path
import glob
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os



def plot_workload_volume(df: pd.DataFrame, output_folder: str, filename: str):
    # Process the request times and response times
    df_req = pd.DataFrame()
    df_req["req_second"] = df['request_time'] // 1000
    df_req = df_req.iloc[1:-1]
    beginning_spike = df_req.groupby("req_second").size().iloc[0]

    df_req = df_req.groupby("req_second").size() / beginning_spike
    df_req.index = df_req.index.map(lambda x: datetime.fromtimestamp(x).strftime('%H:%M:%S'))

    df_resp = pd.DataFrame()
    df_resp["rep_second"] = df['response_time'] // 1000
    df_resp = df_resp.iloc[1:-1]
    df_resp = df_resp.groupby("rep_second").size() / beginning_spike
    df_resp.index = df_resp.index.map(lambda x: datetime.fromtimestamp(x).strftime('%H:%M:%S'))

    os.makedirs(output_folder, exist_ok=True)
    plt.figure(figsize=(10, 8))
    plt.plot(df_req.index, df_req, label='Requests')
    plt.plot(df_resp.index, df_resp, label='Replies')
    plt.xticks(rotation=45)
    plt.xlabel("Time (HH:MM:SS)")
    plt.ylabel("Relative Workload")
    plt.title(f"Workload Volume for Client {filename} Normalized to the Start of the Spike")
    plt.legend()

    plt.savefig(os.path.join(output_folder, f'Client_{Path(filename).stem}_workload.png'))
    plt.show()
    plt.close()

def plot_files(path, output_folder):
    files = glob.glob(path)
    for i, file in enumerate(files):
        data = pd.read_csv(file)
        plot_workload_volume(data, output_folder, str(i+1))

def main():
    folder_path = "./logs/local-run/"
    file_pattern = "client-*.csv"
    output_folder = "logs/local-run/graphs"

    plot_files(folder_path + file_pattern, output_folder)



if __name__ == "__main__":
    main()
