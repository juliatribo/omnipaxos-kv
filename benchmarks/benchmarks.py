from pathlib import Path

from omnipaxos_cluster import OmnipaxosClusterBuilder
from omnipaxos_configs import FlexibleQuorum, RequestInterval


def example_workload() -> dict[int, list[RequestInterval]]:
    experiment_duration = 10
    read_ratio = 0.50
    high_load = RequestInterval(experiment_duration, 100, read_ratio)
    low_load = RequestInterval(experiment_duration, 10, read_ratio)

    nodes = [1, 2, 3, 4, 5]
    us_nodes = [1, 2, 3]
    workload = {}
    for node in nodes:
        if node in us_nodes:
            requests = [high_load, low_load]
        else:
            requests = [low_load, high_load]
        workload[node] = requests
    return workload

def different_workloads(workload:int) -> list[RequestInterval]:
    workload_worldcup = [
        RequestInterval(duration_sec=2, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=11, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=12, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=13, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=19, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=24, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=32, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=36, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=38, read_ratio=0),
    ]
    workload_eecs = [
        RequestInterval(duration_sec=2, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=20, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=16, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=13, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=15, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=12, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=11, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=16, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=11, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=25, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=27, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=32, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=37, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=28, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=36, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=38, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=36, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=27, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=35, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=27, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=25, read_ratio=0),
    ]

    workload_eccs_above_the_clouds = [
        RequestInterval(duration_sec=3, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=12, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=14, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=12, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=8, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=17, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=14, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=12, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=11, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=13, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=11, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=12, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=11, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=12, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=18, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=11, read_ratio=0),
    ]

    workload_ebates = [
        RequestInterval(duration_sec=1, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=9, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=10, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=8, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=7, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=8, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=7, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=6, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=7, read_ratio=0),
        RequestInterval(duration_sec=3, requests_per_sec=8, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=7, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=6, read_ratio=0),
        RequestInterval(duration_sec=3, requests_per_sec=7, read_ratio=0),
        RequestInterval(duration_sec=1, requests_per_sec=6, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=8, read_ratio=0),
        RequestInterval(duration_sec=2, requests_per_sec=9, read_ratio=0),
    ]

    workload_michael_jackson = [
        RequestInterval(duration_sec=5, requests_per_sec=40, read_ratio=0),
        RequestInterval(duration_sec=5, requests_per_sec=40, read_ratio=0),
    ]
    workloads = [workload_worldcup, workload_eecs, workload_eccs_above_the_clouds, workload_ebates, workload_michael_jackson]
    return workloads[workload]


def example_benchmark():
    # Define workload and cluster
    # workload = example_workload()
    workload = different_workloads(0)

    cluster = (
        OmnipaxosClusterBuilder("cluster-5-1")
        .initial_leader(5)
        .server(1, "us-west2-a")
        .server(2, "us-south1-a")
        .server(3, "us-east4-a")
        .server(4, "europe-southwest1-a")
        .server(5, "europe-west4-a")
        .client(1, "us-west2-a", requests=workload)
        .client(2, "us-south1-a", requests=workload)
        .client(3, "us-east4-a", requests=workload)
        .client(4, "europe-southwest1-a", requests=workload)
        .client(5, "europe-west4-a", requests=workload)
    ).build()
    experiment_log_dir = Path(f"./logs/example-experiment")

    # Run cluster
    iteration_dir = Path.joinpath(experiment_log_dir, "MajorityQuorum")
    cluster.run(iteration_dir)

    # Run same cluster again but with different flexible quorum config
    #flex_quorum = FlexibleQuorum(read_quorum_size=4, write_quorum_size=2)
    #cluster.change_cluster_config(initial_flexible_quorum=flex_quorum)
    #iteration_dir = Path.joinpath(experiment_log_dir, "FlexQuorum")
    #cluster.run(iteration_dir)

    # Shutdown GCP instances (or not if you want to reuse instances in another benchmark)
    cluster.shutdown()


def main():
    example_benchmark()
    pass


if __name__ == "__main__":
    main()
