
import json
import socket
import threading
import time

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


class ClusterMaster:

    def __init__(self, host="127.0.0.1", port=6000):

        self.host = host
        self.port = port

        self.workers = {}
        self.lock = threading.Lock()

        self.running = True
        self.server_socket = None

        # Worker health-check configuration.
        self.health_check_timeout = 2
        self.worker_connection_timeout = 5
        self.worker_execution_timeout = 60

    # -------------------------------------------------
    # WORKER STATUS MANAGEMENT
    # -------------------------------------------------

    def mark_worker_status(self, worker_id, status):

        with self.lock:

            worker = self.workers.get(worker_id)

            if not worker:
                return

            previous_status = worker.get("status")

            worker["status"] = status
            worker["last_checked"] = datetime.now().isoformat()

            if status == "ACTIVE":
                worker["last_seen"] = datetime.now().isoformat()

            if previous_status != status:

                print(
                    f"[WORKER STATUS CHANGED] "
                    f"{worker_id}: {previous_status} -> {status}"
                )

    def check_worker(self, worker):

        """
        Check actual worker availability using TCP STATUS.
        """

        worker_id = worker["worker_id"]

        try:

            with socket.create_connection(
                (worker["host"], worker["port"]),
                timeout=self.health_check_timeout
            ) as client:

                client.settimeout(self.health_check_timeout)

                request = {
                    "command": "STATUS"
                }

                client.sendall(
                    (json.dumps(request) + "\n").encode("utf-8")
                )

                response_file = client.makefile(
                    "r",
                    encoding="utf-8"
                )

                response_line = response_file.readline()

                if not response_line:
                    raise ConnectionError("Empty worker response")

                response = json.loads(response_line)

                if (
                    response.get("success")
                    and response.get("worker_id") == worker_id
                    and response.get("status") == "ACTIVE"
                ):

                    self.mark_worker_status(
                        worker_id,
                        "ACTIVE"
                    )

                    return True

                raise ConnectionError("Invalid worker health response")

        except Exception as error:

            self.mark_worker_status(
                worker_id,
                "OFFLINE"
            )

            print(
                f"[WORKER HEALTH CHECK FAILED] "
                f"{worker_id}: {error}"
            )

            return False

    def refresh_worker_status(self):

        """
        Refresh the status of every registered worker.
        """

        with self.lock:
            registered_workers = list(self.workers.values())

        if not registered_workers:
            return []

        active_workers = []

        # Check workers concurrently.
        with ThreadPoolExecutor(
            max_workers=len(registered_workers)
        ) as executor:

            futures = {
                executor.submit(self.check_worker, worker): worker
                for worker in registered_workers
            }

            for future in as_completed(futures):

                worker = futures[future]

                try:

                    if future.result():
                        active_workers.append(worker)

                except Exception as error:

                    self.mark_worker_status(
                        worker["worker_id"],
                        "OFFLINE"
                    )

                    print(
                        f"[HEALTH CHECK ERROR] "
                        f"{worker['worker_id']}: {error}"
                    )

        # Preserve the original registration order.
        active_ids = {
            worker["worker_id"]
            for worker in active_workers
        }

        return [
            worker for worker in registered_workers
            if worker["worker_id"] in active_ids
        ]

    # -------------------------------------------------
    # MASTER STATUS
    # -------------------------------------------------

    def get_status(self):

        self.refresh_worker_status()

        with self.lock:

            registered_count = len(self.workers)

            active_count = sum(
                1 for worker in self.workers.values()
                if worker["status"] == "ACTIVE"
            )

        return {
            "success": True,
            "status": "ONLINE",
            "master": "CLUSTER-MASTER",
            "host": self.host,
            "port": self.port,
            "registered_workers": registered_count,
            "active_workers": active_count,
            "offline_workers": registered_count - active_count,
            "timestamp": datetime.now().isoformat()
        }

    # -------------------------------------------------
    # WORKER REGISTRATION
    # -------------------------------------------------

    def register_worker(self, request):

        worker_id = request.get("worker_id")
        host = request.get("host")
        port = request.get("port")

        if (
            not isinstance(worker_id, str)
            or not worker_id.strip()
            or not isinstance(host, str)
            or not host.strip()
            or not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):

            return {
                "success": False,
                "error": "Invalid worker registration details"
            }

        worker_info = {
            "worker_id": worker_id,
            "host": host,
            "port": port,
            "status": "ACTIVE",
            "registered_at": datetime.now().isoformat(),
            "last_seen": datetime.now().isoformat(),
            "last_checked": None
        }

        with self.lock:

            self.workers[worker_id] = worker_info

        print(
            f"[WORKER REGISTERED] "
            f"{worker_id} | {host}:{port}"
        )

        return {
            "success": True,
            "message": f"{worker_id} registered successfully",
            "worker": worker_info
        }

    def unregister_worker(self, worker_id):

        if not isinstance(worker_id, str):
            return {
                "success": False,
                "error": "Invalid worker ID"
            }

        with self.lock:

            removed = self.workers.pop(
                worker_id,
                None
            )

        if removed:

            print(
                f"[WORKER REMOVED] {worker_id}"
            )

            return {
                "success": True,
                "message": f"{worker_id} unregistered"
            }

        return {
            "success": False,
            "error": "Worker not found"
        }

    # -------------------------------------------------
    # LIST REGISTERED WORKERS
    # -------------------------------------------------

    def list_workers(self):

        self.refresh_worker_status()

        with self.lock:

            worker_list = [
                worker.copy()
                for worker in self.workers.values()
            ]

        active_count = sum(
            1 for worker in worker_list
            if worker["status"] == "ACTIVE"
        )

        return {
            "success": True,
            "total_workers": len(worker_list),
            "active_workers": active_count,
            "offline_workers": len(worker_list) - active_count,
            "workers": worker_list
        }

    # -------------------------------------------------
    # EXECUTE TASK ON A WORKER
    # -------------------------------------------------

    def execute_on_worker(self, worker, task):

        """Send one computational task to a worker."""

        worker_id = worker["worker_id"]

        try:

            with socket.create_connection(
                (worker["host"], worker["port"]),
                timeout=self.worker_connection_timeout
            ) as client:

                client.settimeout(
                    self.worker_execution_timeout
                )

                request = {
                    "command": "EXECUTE_TASK",
                    "task": task
                }

                client.sendall(
                    (json.dumps(request) + "\n").encode("utf-8")
                )

                response_file = client.makefile(
                    "r",
                    encoding="utf-8"
                )

                response_line = response_file.readline()

                if not response_line:

                    raise ConnectionError(
                        "Worker returned an empty response"
                    )

                response = json.loads(response_line)

                if not isinstance(response, dict):
                    raise ValueError("Invalid worker response")

                # A task validation error does not mean
                # that the worker itself is offline.
                if not response.get("success"):

                    response.setdefault("task_id", task.get("task_id"))
                    response.setdefault("worker_id", worker_id)

                return response

        except Exception as error:

            self.mark_worker_status(
                worker_id,
                "OFFLINE"
            )

            print(
                f"[TASK EXECUTION FAILED] "
                f"{task.get('task_id')} | "
                f"{worker_id} | {error}"
            )

            return {
                "success": False,
                "task_id": task.get("task_id"),
                "worker_id": worker_id,
                "error": str(error)
            }

    # -------------------------------------------------
    # PARALLEL BATCH EXECUTION
    # -------------------------------------------------

    def execute_batch(self, tasks):

        """Distribute tasks among currently healthy workers."""

        if not isinstance(tasks, list) or not tasks:

            return {
                "success": False,
                "error": "Provide a non-empty list of tasks"
            }

        if len(tasks) > 100:

            return {
                "success": False,
                "error": "Maximum 100 tasks allowed per batch"
            }

        # Validate all tasks before scheduling.
        for index, task in enumerate(tasks):

            if not isinstance(task, dict):

                return {
                    "success": False,
                    "error": f"Task at index {index} must be an object"
                }

            if (
                not isinstance(task.get("task_id"), str)
                or not task.get("task_id").strip()
                or not isinstance(task.get("task_type"), str)
                or not task.get("task_type").strip()
            ):

                return {
                    "success": False,
                    "error": (
                        f"Task at index {index} is missing "
                        "a valid task_id or task_type"
                    )
                }

        # Check workers before task assignment.
        available_workers = self.refresh_worker_status()

        if not available_workers:

            return {
                "success": False,
                "total_tasks": len(tasks),
                "successful_tasks": 0,
                "failed_tasks": len(tasks),
                "error": "No active workers are available",
                "worker_distribution": {},
                "successful_worker_distribution": {},
                "results": []
            }

        # Assign tasks in round-robin order.
        assignments = []

        for index, task in enumerate(tasks):

            worker = available_workers[
                index % len(available_workers)
            ]

            assignments.append(
                (worker, task)
            )

        print("\n[BATCH STARTED]")
        print(f"Tasks          : {len(assignments)}")
        print(f"Registered     : {len(self.workers)}")
        print(f"Active Workers : {len(available_workers)}")

        start_time = time.perf_counter()

        results = []

        # Execute tasks concurrently.
        with ThreadPoolExecutor(
            max_workers=len(available_workers)
        ) as executor:

            futures = {
                executor.submit(
                    self.execute_on_worker,
                    worker,
                    task
                ): (worker, task)

                for worker, task in assignments
            }

            for future in as_completed(futures):

                try:

                    results.append(
                        future.result()
                    )

                except Exception as error:

                    worker, task = futures[future]

                    results.append({
                        "success": False,
                        "task_id": task.get("task_id"),
                        "worker_id": worker["worker_id"],
                        "error": str(error)
                    })

        total_time = time.perf_counter() - start_time

        results.sort(
            key=lambda item: str(
                item.get("task_id", "")
            )
        )

        successful = sum(
            1 for result in results
            if result.get("success")
        )

        failed = len(results) - successful

        # Count attempted tasks per worker.
        worker_distribution = {}

        for result in results:

            worker_id = result.get(
                "worker_id",
                "UNKNOWN"
            )

            worker_distribution[worker_id] = (
                worker_distribution.get(worker_id, 0) + 1
            )

        # Count only successful tasks per worker.
        successful_worker_distribution = {}

        for result in results:

            if result.get("success"):

                worker_id = result.get(
                    "worker_id",
                    "UNKNOWN"
                )

                successful_worker_distribution[worker_id] = (
                    successful_worker_distribution.get(
                        worker_id,
                        0
                    ) + 1
                )

        print(
            f"[BATCH COMPLETED] "
            f"Successful: {successful} | "
            f"Failed: {failed} | "
            f"Time: {total_time:.6f}s"
        )

        return {
            "success": failed == 0,
            "total_tasks": len(tasks),
            "successful_tasks": successful,
            "failed_tasks": failed,
            "total_execution_time": round(
                total_time,
                6
            ),
            "active_workers_used": len(available_workers),
            "worker_distribution": worker_distribution,
            "successful_worker_distribution": (
                successful_worker_distribution
            ),
            "results": results
        }

    # -------------------------------------------------
    # REQUEST ROUTER
    # -------------------------------------------------

    def handle_request(self, request):

        if not isinstance(request, dict):

            return {
                "success": False,
                "error": "Invalid request format"
            }

        command = request.get("command")

        if command == "STATUS":

            return self.get_status()

        elif command == "LIST_WORKERS":

            return self.list_workers()

        elif command == "REGISTER_WORKER":

            return self.register_worker(request)

        elif command == "UNREGISTER_WORKER":

            return self.unregister_worker(
                request.get("worker_id")
            )

        elif command == "EXECUTE_BATCH":

            return self.execute_batch(
                request.get("tasks", [])
            )

        return {
            "success": False,
            "error": "Unknown command"
        }

    # -------------------------------------------------
    # CLIENT CONNECTION HANDLER
    # -------------------------------------------------

    def handle_client(self, client_socket, address):

        try:

            with client_socket:

                client_socket.settimeout(130)

                response_file = client_socket.makefile(
                    "r",
                    encoding="utf-8"
                )

                line = response_file.readline()

                if not line:
                    return

                request = json.loads(line)

                response = self.handle_request(request)

                client_socket.sendall(
                    (
                        json.dumps(response) + "\n"
                    ).encode("utf-8")
                )

        except json.JSONDecodeError:

            print(
                f"[INVALID REQUEST] From {address}"
            )

        except Exception as error:

            print(
                f"[CLIENT ERROR] {address}: {error}"
            )

    # -------------------------------------------------
    # START MASTER SERVER
    # -------------------------------------------------

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

        self.server_socket.bind(
            (self.host, self.port)
        )

        self.server_socket.listen(10)

        self.server_socket.settimeout(1)

        print("=" * 55)
        print("             DPCS CLUSTER MASTER")
        print("=" * 55)

        print(
            f"Master Address : {self.host}:{self.port}"
        )

        print("Status         : ONLINE")
        print("Worker Health  : ENABLED")
        print("Task Scheduling: ACTIVE")
        print("=" * 55)
        print("Waiting for worker and client connections...")

        try:

            while self.running:

                try:

                    client_socket, address = (
                        self.server_socket.accept()
                    )

                    thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )

                    thread.start()

                except socket.timeout:
                    continue

        except KeyboardInterrupt:

            print(
                "\n[SHUTDOWN] Cluster Master stopping..."
            )

        finally:

            self.running = False

            if self.server_socket:

                self.server_socket.close()

            print(
                "[OFFLINE] Cluster Master stopped."
            )


# -----------------------------------------------------
# MAIN
# -----------------------------------------------------

if __name__ == "__main__":

    master = ClusterMaster()
    master.start()
