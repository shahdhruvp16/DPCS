
import json
import socket


# Request will be submitted through PEER-01.
PEER_HOST = "127.0.0.1"
PEER_PORT = 5001
PEER_ID = "PEER-01"


def submit_cluster_request(tasks):

    request = {
        "command": "EXECUTE_CLUSTER_TASKS",
        "tasks": tasks
    }

    try:
        with socket.create_connection(
            (PEER_HOST, PEER_PORT),
            timeout=10
        ) as client:

            client.settimeout(130)

            client.sendall(
                (json.dumps(request) + "\n").encode("utf-8")
            )

            response_file = client.makefile(
                "r",
                encoding="utf-8"
            )

            response_line = response_file.readline()

            if not response_line:
                print("[ERROR] No response received from peer.")
                return None

            return json.loads(response_line)

    except Exception as error:
        print(f"[INTEGRATION ERROR] {error}")
        return None


def main():

    tasks = [
        {
            "task_id": "INT-TASK-01",
            "task_type": "SUM",
            "data": [10, 20, 30, 40, 50]
        },
        {
            "task_id": "INT-TASK-02",
            "task_type": "FACTORIAL",
            "data": 10
        },
        {
            "task_id": "INT-TASK-03",
            "task_type": "PRIME_COUNT",
            "data": 100
        },
        {
            "task_id": "INT-TASK-04",
            "task_type": "SUM",
            "data": [100, 200, 300, 400, 500]
        },
        {
            "task_id": "INT-TASK-05",
            "task_type": "FACTORIAL",
            "data": 7
        },
        {
            "task_id": "INT-TASK-06",
            "task_type": "PRIME_COUNT",
            "data": 1000
        }
    ]

    print("=" * 70)
    print("       DPCS P2P + CLUSTER INTEGRATION")
    print("=" * 70)

    print(f"Requesting Peer : {PEER_ID}")
    print(f"Peer Address    : {PEER_HOST}:{PEER_PORT}")
    print(f"Tasks Submitted : {len(tasks)}")

    print("\nCOMPUTATIONAL REQUEST")
    print("-" * 50)

    for task in tasks:
        print(
            f"{task['task_id']} | "
            f"{task['task_type']} | "
            f"Input: {task['data']}"
        )

    print("\nSending request to PEER-01...")
    print("PEER-01 will forward tasks to the Cluster Master.")

    response = submit_cluster_request(tasks)

    if not response:
        print("\n[FAILED] No valid response from peer.")
        return

    if not response.get("success"):
        print("\n[FAILED] Cluster execution was unsuccessful.")
        print(response.get("error", response.get("cluster_response")))
        return

    cluster = response.get("cluster_response", {})

    print("\n" + "=" * 70)
    print("          INTEGRATED EXECUTION REPORT")
    print("=" * 70)

    print(f"Requesting Peer   : {response['peer_id']}")
    print(f"Total Tasks       : {cluster['total_tasks']}")
    print(f"Successful Tasks  : {cluster['successful_tasks']}")
    print(f"Failed Tasks      : {cluster['failed_tasks']}")
    print(
        f"Execution Time    : "
        f"{cluster['total_execution_time']:.6f} seconds"
    )

    print("\nWORKER DISTRIBUTION")
    print("-" * 50)

    for worker_id, count in cluster["worker_distribution"].items():
        print(f"{worker_id}: {count} tasks")

    print("\nRESULTS RETURNED TO REQUESTING PEER")
    print("-" * 70)

    for result in cluster["results"]:

        if result.get("success"):
            print(
                f"{result['task_id']} | "
                f"{result['worker_id']} | "
                f"{result['task_type']} | "
                f"Result: {result['result']}"
            )
        else:
            print(
                f"{result.get('task_id')} | FAILED | "
                f"{result.get('error')}"
            )

    print("\n" + "=" * 70)

    if cluster.get("success"):
        print("SUCCESS: P2P + CLUSTER INTEGRATION COMPLETED!")
        print("The requesting peer received the aggregated results.")
    else:
        print("WARNING: Some cluster tasks failed.")

    print("=" * 70)


if __name__ == "__main__":
    main()
