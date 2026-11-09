
import json
import socket
import threading
import sys
import time
import math
from datetime import datetime


MASTER_HOST = "127.0.0.1"
MASTER_PORT = 6000


class ClusterWorker:

    def __init__(self, worker_id, host="127.0.0.1", port=6001):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.running = True
        self.server_socket = None

    def send_master_request(self, request):
        try:
            with socket.create_connection(
                (MASTER_HOST, MASTER_PORT), timeout=5
            ) as client:
                client.sendall(
                    (json.dumps(request) + "\n").encode("utf-8")
                )

                response_file = client.makefile("r", encoding="utf-8")
                response = response_file.readline()

                return json.loads(response) if response else None

        except Exception as error:
            print(f"[MASTER COMMUNICATION ERROR] {error}")
            return None

    def register_with_master(self):
        response = self.send_master_request({
            "command": "REGISTER_WORKER",
            "worker_id": self.worker_id,
            "host": self.host,
            "port": self.port
        })

        if response and response.get("success"):
            print(f"[REGISTERED] {self.worker_id} with Cluster Master")
            return True

        print(f"[REGISTRATION FAILED] {self.worker_id}")
        return False

    def unregister_from_master(self):
        response = self.send_master_request({
            "command": "UNREGISTER_WORKER",
            "worker_id": self.worker_id
        })

        if response and response.get("success"):
            print(f"[UNREGISTERED] {self.worker_id}")

    def execute_task(self, task):
        task_type = task.get("task_type")
        task_id = task.get("task_id", "UNKNOWN")
        data = task.get("data")

        start_time = time.perf_counter()

        try:
            if task_type == "SUM":
                if not isinstance(data, list) or not all(
                    isinstance(x, (int, float)) and not isinstance(x, bool)
                    for x in data
                ):
                    raise ValueError("SUM requires a list of numbers")

                result = sum(data)

            elif task_type == "FACTORIAL":
                if (
                    not isinstance(data, int)
                    or isinstance(data, bool)
                    or data < 0
                    or data > 1000
                ):
                    raise ValueError("FACTORIAL requires an integer from 0 to 1000")

                result = math.factorial(data)

            elif task_type == "PRIME_COUNT":
                if (
                    not isinstance(data, int)
                    or isinstance(data, bool)
                    or data < 0
                    or data > 1000000
                ):
                    raise ValueError("PRIME_COUNT requires an integer from 0 to 1000000")

                count = 0

                for number in range(2, data + 1):
                    is_prime = True

                    for divisor in range(2, math.isqrt(number) + 1):
                        if number % divisor == 0:
                            is_prime = False
                            break

                    if is_prime:
                        count += 1

                result = count

            else:
                raise ValueError("Unsupported task type")

            execution_time = time.perf_counter() - start_time

            print(
                f"[TASK COMPLETED] {task_id} | "
                f"{task_type} | Result: {result} | "
                f"Time: {execution_time:.6f}s"
            )

            return {
                "success": True,
                "task_id": task_id,
                "worker_id": self.worker_id,
                "task_type": task_type,
                "result": result,
                "execution_time": round(execution_time, 6)
            }

        except Exception as error:
            return {
                "success": False,
                "task_id": task_id,
                "worker_id": self.worker_id,
                "error": str(error)
            }

    def handle_request(self, request):
        command = request.get("command")

        if command == "STATUS":
            return {
                "success": True,
                "worker_id": self.worker_id,
                "status": "ACTIVE",
                "host": self.host,
                "port": self.port,
                "timestamp": datetime.now().isoformat()
            }

        elif command == "PING":
            return {
                "success": True,
                "worker_id": self.worker_id,
                "message": "PONG"
            }

        elif command == "EXECUTE_TASK":
            return self.execute_task(request.get("task", {}))

        return {
            "success": False,
            "error": "Unknown worker command"
        }

    def handle_client(self, client_socket, address):
        try:
            with client_socket:
                file = client_socket.makefile("r", encoding="utf-8")
                line = file.readline()

                if not line:
                    return

                request = json.loads(line)
                response = self.handle_request(request)

                client_socket.sendall(
                    (json.dumps(response) + "\n").encode("utf-8")
                )

        except json.JSONDecodeError:
            print(f"[INVALID REQUEST] From {address}")
        except Exception as error:
            print(f"[WORKER CLIENT ERROR] {error}")

    def start(self):
        self.server_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)

        print("=" * 50)
        print(f"       {self.worker_id}")
        print("=" * 50)
        print(f"Worker Address : {self.host}:{self.port}")
        print("Status         : ONLINE")

        self.register_with_master()

        print("Waiting for cluster tasks...")
        print("=" * 50)

        try:
            while self.running:
                client_socket, address = self.server_socket.accept()

                thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                thread.start()

        except KeyboardInterrupt:
            print(f"\n[SHUTDOWN] {self.worker_id} stopping...")

        finally:
            self.running = False
            self.unregister_from_master()
            self.server_socket.close()
            print(f"[OFFLINE] {self.worker_id} stopped.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m cluster.worker WORKER_ID PORT")
        sys.exit(1)

    worker_id = sys.argv[1]
    port = int(sys.argv[2])

    worker = ClusterWorker(
        worker_id=worker_id,
        port=port
    )

    worker.start()
