
import json
import socket

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 6000


def send_master_request(request):
    try:
        with socket.create_connection(
            (MASTER_HOST, MASTER_PORT), timeout=10
        ) as client:

            client.settimeout(15)

            client.sendall(
                (json.dumps(request) + "\n").encode("utf-8")
            )

            response_file = client.makefile("r", encoding="utf-8")
            response_line = response_file.readline()

            if not response_line:
                print("[ERROR] Empty response from master.")
                return None

            return json.loads(response_line)

    except ConnectionRefusedError:
        print("[ERROR] Cluster master is not running.")
    except Exception as error:
        print(f"[ERROR] {error}")

    return None


def check_master():
    print("\nCLUSTER MASTER HEALTH CHECK")
    print("-" * 45)

    response = send_master_request({"command": "STATUS"})

    if response and response.get("success"):
        print(f"Master      : {response['master']}")
        print(f"Address     : {response['host']}:{response['port']}")
        print(f"Status      : {response['status']}")
        print(f"Workers     : {response['registered_workers']}")
        print("\nSUCCESS: Cluster master is operational!")
    else:
        print("[FAILED] Unable to communicate with master.")
        if response:
            print("Response:", response)


def list_workers():
    print("\nREGISTERED CLUSTER WORKERS")
    print("-" * 45)

    response = send_master_request({"command": "LIST_WORKERS"})

    if response and response.get("success"):
        print(f"Total workers: {response['total_workers']}")

        for worker in response["workers"]:
            print(
                f"{worker['worker_id']} | "
                f"{worker['host']}:{worker['port']} | "
                f"{worker['status']}"
            )
    else:
        print("[ERROR] Could not retrieve worker list.")
        if response:
            print(response)


if __name__ == "__main__":
    check_master()
    list_workers()
