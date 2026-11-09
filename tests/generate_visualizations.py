
import csv
from pathlib import Path

import matplotlib.pyplot as plt

from communication.p2p_client import send_request


RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

BENCHMARK_FILE = RESULTS_DIR / "performance_benchmark.csv"


def generate_performance_graphs():
    if not BENCHMARK_FILE.exists():
        print("[ERROR] Performance benchmark CSV not found.")
        return False

    batch_sizes = []
    average_times = []
    minimum_times = []
    maximum_times = []

    with open(BENCHMARK_FILE, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            batch_sizes.append(int(row["batch_size"]))
            average_times.append(float(row["average_time"]))
            minimum_times.append(float(row["minimum_time"]))
            maximum_times.append(float(row["maximum_time"]))

    if not batch_sizes:
        print("[ERROR] No benchmark data found.")
        return False

    # Graph 1: Average execution time
    plt.figure(figsize=(9, 5))
    plt.plot(
        batch_sizes,
        average_times,
        marker="o",
        linewidth=2
    )
    plt.title("Cluster Performance: Average Execution Time")
    plt.xlabel("Number of Tasks")
    plt.ylabel("Average Execution Time (seconds)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(
        RESULTS_DIR / "average_execution_time.png",
        dpi=300
    )
    plt.close()

    # Graph 2: Minimum, average and maximum execution time
    plt.figure(figsize=(9, 5))
    plt.plot(
        batch_sizes,
        minimum_times,
        marker="o",
        label="Minimum"
    )
    plt.plot(
        batch_sizes,
        average_times,
        marker="s",
        label="Average"
    )
    plt.plot(
        batch_sizes,
        maximum_times,
        marker="^",
        label="Maximum"
    )
    plt.title("Cluster Execution Time Comparison")
    plt.xlabel("Number of Tasks")
    plt.ylabel("Execution Time (seconds)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(
        RESULTS_DIR / "execution_time_comparison.png",
        dpi=300
    )
    plt.close()

    print("[SUCCESS] Performance graphs generated.")
    return True

def generate_workload_distribution_graph():
    tasks = []

    for i in range(12):
        tasks.append({
            "task_id": f"DIST-TASK-{i + 1:02d}",
            "task_type": "SUM",
            "data": list(range(1, 101))
        })

    request = {
        "command": "EXECUTE_BATCH",
        "tasks": tasks
    }

    response = send_request(
        "127.0.0.1",
        6000,
        request
    )

    if not response or not response.get("success"):
        print("[ERROR] Workload distribution test failed.")
        print(response)
        return False

    distribution = response.get(
        "successful_worker_distribution",
        {}
    )

    if not distribution:
        print("[ERROR] No successful worker distribution returned.")
        return False

    workers = sorted(distribution.keys())
    task_counts = [distribution[worker] for worker in workers]

    with open(
        RESULTS_DIR / "worker_distribution.csv",
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(["worker_id", "successful_tasks"])

        for worker, count in zip(workers, task_counts):
            writer.writerow([worker, count])

    plt.figure(figsize=(9, 5))
    plt.bar(workers, task_counts)
    plt.title("Cluster Workload Distribution")
    plt.xlabel("Worker")
    plt.ylabel("Number of Successfully Executed Tasks")
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig(
        RESULTS_DIR / "worker_distribution.png",
        dpi=300
    )
    plt.close()

    print("\nWORKLOAD DISTRIBUTION")
    for worker, count in zip(workers, task_counts):
        print(f"{worker}: {count} tasks")

    print("[SUCCESS] Workload distribution graph generated.")
    return True


def main():
    print("=" * 65)
    print("       DPCS PERFORMANCE VISUALIZATION")
    print("=" * 65)

    performance_ok = generate_performance_graphs()
    distribution_ok = generate_workload_distribution_graph()

    print("\nGENERATED FILES")
    print("-" * 65)

    for filename in [
        "average_execution_time.png",
        "execution_time_comparison.png",
        "worker_distribution.png",
        "worker_distribution.csv"
    ]:
        path = RESULTS_DIR / filename
        if path.exists():
            print(f"[CREATED] {path}")

    if performance_ok and distribution_ok:
        print("\nSUCCESS: ALL VISUALIZATIONS GENERATED!")
    else:
        print("\nWARNING: SOME VISUALIZATIONS COULD NOT BE GENERATED.")

    print("=" * 65)


if __name__ == "__main__":
    main()
