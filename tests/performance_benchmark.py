
import csv
import statistics
from pathlib import Path

from communication.p2p_client import send_request

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 6000

BATCH_SIZES = [6, 30, 60, 90]
REPETITIONS = 5

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

CSV_FILE = RESULTS_DIR / "performance_benchmark.csv"


def generate_tasks(batch_size, run_number):
    tasks = []

    for i in range(batch_size):
        tasks.append({
            "task_id": f"B{batch_size}-R{run_number}-T{i + 1}",
            "task_type": "SUM",
            "data": list(range(1, 101))
        })

    return tasks


def run_benchmark():
    benchmark_results = []

    print("=" * 75)
    print("          DPCS CLUSTER PERFORMANCE BENCHMARK")
    print("=" * 75)

    for batch_size in BATCH_SIZES:
        execution_times = []

        print(f"\nTesting batch size: {batch_size}")
        print("-" * 55)

        for run_number in range(1, REPETITIONS + 1):
            tasks = generate_tasks(batch_size, run_number)

            request = {
                "command": "EXECUTE_BATCH",
                "tasks": tasks
            }

            response = send_request(
                MASTER_HOST,
                MASTER_PORT,
                request
            )

            if not response or not response.get("success"):
                print(
                    f"Run {run_number}: FAILED - "
                    f"{response}"
                )
                continue

            if response.get("successful_tasks") != batch_size:
                print(f"Run {run_number}: INCOMPLETE")
                continue

            execution_time = response.get(
                "total_execution_time"
            )

            execution_times.append(execution_time)

            print(
                f"Run {run_number}: "
                f"{batch_size} tasks | "
                f"{execution_time:.6f} seconds"
            )

        if execution_times:
            benchmark_results.append({
                "batch_size": batch_size,
                "successful_runs": len(execution_times),
                "average_time": statistics.mean(execution_times),
                "minimum_time": min(execution_times),
                "maximum_time": max(execution_times)
            })

    print("\n" + "=" * 75)
    print("                    BENCHMARK SUMMARY")
    print("=" * 75)

    for result in benchmark_results:
        print(
            f"\nBatch Size: {result['batch_size']}"
            f"\nSuccessful Runs: {result['successful_runs']}"
            f"\nAverage Time: {result['average_time']:.6f} seconds"
            f"\nMinimum Time: {result['minimum_time']:.6f} seconds"
            f"\nMaximum Time: {result['maximum_time']:.6f} seconds"
        )

    if benchmark_results:
        with open(
            CSV_FILE,
            "w",
            newline="",
            encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "batch_size",
                    "successful_runs",
                    "average_time",
                    "minimum_time",
                    "maximum_time"
                ]
            )

            writer.writeheader()
            writer.writerows(benchmark_results)

        print("\nBenchmark data saved to:", CSV_FILE)

    print("\n" + "=" * 75)
    print("BENCHMARK COMPLETED")
    print("=" * 75)


if __name__ == "__main__":
    run_benchmark()
