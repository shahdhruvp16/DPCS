
from communication.p2p_client import send_request

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 6000

tasks = [
    {
        "task_id": "TEST-01",
        "task_type": "SUM",
        "data": [1, 2, 3, 4]
    },
    {
        "task_id": "TEST-02",
        "task_type": "SUM",
        "data": [-5, 10, -2]
    },
    {
        "task_id": "TEST-03",
        "task_type": "FACTORIAL",
        "data": 0
    },
    {
        "task_id": "TEST-04",
        "task_type": "FACTORIAL",
        "data": 5
    },
    {
        "task_id": "TEST-05",
        "task_type": "PRIME_COUNT",
        "data": 0
    },
    {
        "task_id": "TEST-06",
        "task_type": "PRIME_COUNT",
        "data": 10
    },
    {
        "task_id": "TEST-07",
        "task_type": "PRIME_COUNT",
        "data": 100
    },
    {
        "task_id": "TEST-08",
        "task_type": "SUM",
        "data": [0, 0, 0]
    },
    {
        "task_id": "TEST-09",
        "task_type": "FACTORIAL",
        "data": 1
    }
]

expected_results = {
    "TEST-01": 10,
    "TEST-02": 3,
    "TEST-03": 1,
    "TEST-04": 120,
    "TEST-05": 0,
    "TEST-06": 4,
    "TEST-07": 25,
    "TEST-08": 0,
    "TEST-09": 1
}


def main():
    print("=" * 70)
    print("       DPCS TASK CORRECTNESS AND DISTRIBUTION TEST")
    print("=" * 70)

    print("\nSubmitting", len(tasks), "tasks to Cluster Master...")

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
        print("\n[FAILED] Cluster execution was unsuccessful.")
        print(response)
        return

    results = response.get("results", [])
    passed = 0
    failed = 0

    print("\nTASK CORRECTNESS RESULTS")
    print("-" * 70)

    for result in results:
        task_id = result.get("task_id")
        actual = result.get("result")
        expected = expected_results.get(task_id)

        is_correct = (
            result.get("success")
            and task_id in expected_results
            and actual == expected
        )

        if is_correct:
            passed += 1
            status = "PASS"
        else:
            failed += 1
            status = "FAIL"

        print(
            f"{task_id} | "
            f"Expected: {expected} | "
            f"Actual: {actual} | "
            f"{status}"
        )

    print("\nWORKLOAD DISTRIBUTION")
    print("-" * 70)

    distribution = response.get(
        "successful_worker_distribution",
        {}
    )

    for worker_id, count in sorted(distribution.items()):
        print(f"{worker_id}: {count} successful tasks")

    print("\nFINAL TEST SUMMARY")
    print("-" * 70)
    print(f"Tasks Submitted   : {len(tasks)}")
    print(f"Correct Results   : {passed}")
    print(f"Incorrect Results : {failed}")
    print(f"Successful Tasks  : {response.get('successful_tasks')}")
    print(f"Failed Tasks      : {response.get('failed_tasks')}")
    print(
        "Execution Time    : "
        f"{response.get('total_execution_time')} seconds"
    )

    if passed == len(tasks) and failed == 0:
        print("\nSUCCESS: ALL TASK CORRECTNESS TESTS PASSED!")
    else:
        print("\nWARNING: SOME TASK CORRECTNESS TESTS FAILED.")

    if len(distribution) == 3 and all(
        count == 3 for count in distribution.values()
    ):
        print("SUCCESS: EQUAL WORKLOAD DISTRIBUTION VERIFIED!")
    else:
        print("WARNING: WORKLOAD DISTRIBUTION WAS NOT EQUAL.")

    print("=" * 70)


if __name__ == "__main__":
    main()
