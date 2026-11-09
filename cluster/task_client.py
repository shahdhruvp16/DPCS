import json
import socket


MASTER_HOST = "127.0.0.1"
MASTER_PORT = 6000



def submit_tasks(tasks):
    request = {
        "command": "EXECUTE_BATCH",
        "tasks": tasks
    }

    try:
        with socket.create_connection(
            (MASTER_HOST, MASTER_PORT), timeout=10
        ) as client:

            client.settimeout(120)

            client.sendall(
                (json.dumps(request) + "\n").encode("utf-8")
            )

            response_file = client.makefile("r", encoding="utf-8")
            response_line = response_file.readline()

            if not response_line:
                print("[ERROR] No response received from master.")
                return None

            response = json.loads(response_line)

            print("\nDEBUG - RAW MASTER RESPONSE:")
            print(json.dumps(response, indent=4))

            return response

    except json.JSONDecodeError:
        print("[ERROR] Master returned invalid JSON.")
        return None

    except Exception as error:
        print(f"[SUBMISSION ERROR] {error}")
        return None

def main():

    tasks = [
        {
            "task_id": "TASK-01",
            "task_type": "SUM",
            "data": [10, 20, 30, 40, 50]
        },
        {
            "task_id": "TASK-02",
            "task_type": "FACTORIAL",
            "data": 10
        },
        {
            "task_id": "TASK-03",
            "task_type": "PRIME_COUNT",
            "data": 100
        },
        {
            "task_id": "TASK-04",
            "task_type": "SUM",
            "data": [100, 200, 300, 400, 500]
        },
        {
            "task_id": "TASK-05",
            "task_type": "FACTORIAL",
            "data": 7
        },
        {
            "task_id": "TASK-06",
            "task_type": "PRIME_COUNT",
            "data": 1000
        }
    ]

    print("=" * 65)
    print("        DPCS DISTRIBUTED TASK SUBMISSION")
    print("=" * 65)

    print(f"Total tasks submitted: {len(tasks)}")

    for task in tasks:
        print(
            f"{task['task_id']} | "
            f"{task['task_type']} | "
            f"Input: {task['data']}"
        )

    print("\nSubmitting tasks to Cluster Master...")

    response = submit_tasks(tasks)

    if not response:
        print("Task submission failed: No valid response.")
        return

    if "total_tasks" not in response:
        print("\n[MASTER ERROR]")
        print(response.get("error", "Unexpected response format"))
        return

    print("\n" + "=" * 65)
    print("             CLUSTER EXECUTION REPORT")
    print("=" * 65)

    print(f"Total Tasks       : {response['total_tasks']}")
    print(f"Successful Tasks  : {response['successful_tasks']}")
    print(f"Failed Tasks      : {response['failed_tasks']}")
    print(
        f"Execution Time    : "
        f"{response['total_execution_time']:.6f} seconds"
    )

    print("\nWORKER DISTRIBUTION")
    print("-" * 45)

    for worker_id, count in response["worker_distribution"].items():
        print(f"{worker_id}: {count} tasks")

    print("\nINDIVIDUAL TASK RESULTS")
    print("-" * 65)

    for result in response["results"]:
        if result["success"]:
            print(
                f"{result['task_id']} | "
                f"Worker: {result['worker_id']} | "
                f"Type: {result['task_type']} | "
                f"Result: {result['result']} | "
                f"Time: {result['execution_time']:.6f}s"
            )
        else:
            print(
                f"{result['task_id']} | FAILED | "
                f"{result.get('error')}"
            )

    print("\n" + "=" * 65)

    if response["success"]:
        print("SUCCESS: All distributed tasks completed!")
    else:
        print("WARNING: Some tasks failed.")

    print("=" * 65)


if __name__ == "__main__":
    main()
