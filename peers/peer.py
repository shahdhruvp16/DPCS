
import argparse
import base64
import json
import os
import socket
import threading
import time
from pathlib import Path


class PeerNode:

    def __init__(
        self,
        peer_id,
        host,
        port,
        master_host="127.0.0.1",
        master_port=6000
    ):

        self.peer_id = peer_id
        self.host = host
        self.port = port

        self.master_host = master_host
        self.master_port = master_port

        self.start_time = time.time()
        self.running = False
        self.server_socket = None

        # Initialize an independent resource directory.
        project_root = Path(__file__).resolve().parent.parent

        self.resource_dir = (
            project_root / "resources" / self.peer_id
        )

        self.resource_dir.mkdir(
            parents=True,
            exist_ok=True
        )

    # -------------------------------------------------
    # PEER STATUS
    # -------------------------------------------------

    def get_status(self):

        return {
            "peer_id": self.peer_id,
            "host": self.host,
            "port": self.port,
            "state": "ACTIVE",
            "pid": os.getpid(),
            "uptime_seconds": round(
                time.time() - self.start_time,
                2
            )
        }

    # -------------------------------------------------
    # RESOURCE MANAGEMENT
    # -------------------------------------------------

    def list_resources(self):

        resources = []

        for file_path in self.resource_dir.iterdir():

            if (
                file_path.is_file()
                and not file_path.is_symlink()
            ):

                resources.append({
                    "filename": file_path.name,
                    "size_bytes": file_path.stat().st_size
                })

        return resources

    def get_resource(self, filename):

        # Validate filename.
        if (
            not isinstance(filename, str)
            or not filename.strip()
            or filename in (".", "..")
            or Path(filename).name != filename
            or "/" in filename
            or "\\" in filename
        ):

            return {
                "success": False,
                "error": "Invalid filename"
            }

        file_path = self.resource_dir / filename

        if (
            not file_path.is_file()
            or file_path.is_symlink()
        ):

            return {
                "success": False,
                "error": "Resource not found"
            }

        # Maximum resource size: 5 MB.
        if file_path.stat().st_size > 5 * 1024 * 1024:

            return {
                "success": False,
                "error": "Resource exceeds 5 MB limit"
            }

        try:

            with open(file_path, "rb") as file:
                file_content = file.read()

            encoded_content = base64.b64encode(
                file_content
            ).decode("ascii")

            return {
                "success": True,
                "filename": filename,
                "size_bytes": len(file_content),
                "content": encoded_content
            }

        except OSError as error:

            return {
                "success": False,
                "error": str(error)
            }

    # -------------------------------------------------
    # CLUSTER COMMUNICATION
    # -------------------------------------------------

    def submit_to_cluster(self, tasks):

        """
        Forward computational tasks to the Cluster Master.

        The peer acts as a gateway between the P2P
        network and the cluster computing system.
        """

        if (
            not isinstance(tasks, list)
            or not tasks
            or len(tasks) > 100
        ):

            return {
                "success": False,
                "peer_id": self.peer_id,
                "error": "Task list must contain between 1 and 100 tasks"
            }

        cluster_request = {
            "command": "EXECUTE_BATCH",
            "tasks": tasks
        }

        try:

            print(
                f"\n[CLUSTER REQUEST] "
                f"{self.peer_id} submitted "
                f"{len(tasks)} tasks to Cluster Master."
            )

            with socket.create_connection(
                (self.master_host, self.master_port),
                timeout=10
            ) as cluster_socket:

                cluster_socket.settimeout(120)

                cluster_socket.sendall(
                    (
                        json.dumps(cluster_request) + "\n"
                    ).encode("utf-8")
                )

                response_file = cluster_socket.makefile(
                    "r",
                    encoding="utf-8"
                )

                response_line = response_file.readline()

                if not response_line:

                    return {
                        "success": False,
                        "peer_id": self.peer_id,
                        "error": "Cluster Master returned no response"
                    }

                cluster_response = json.loads(
                    response_line
                )

                print(
                    f"[CLUSTER RESPONSE] "
                    f"{self.peer_id} received aggregated results "
                    f"from Cluster Master."
                )

                return {
                    "success": True,
                    "peer_id": self.peer_id,
                    "cluster_response": cluster_response
                }

        except socket.timeout:

            print(
                f"[CLUSTER TIMEOUT] "
                f"{self.peer_id}: Cluster request timed out."
            )

            return {
                "success": False,
                "peer_id": self.peer_id,
                "error": "Cluster request timed out"
            }

        except (
            json.JSONDecodeError,
            UnicodeDecodeError
        ) as error:

            return {
                "success": False,
                "peer_id": self.peer_id,
                "error": f"Invalid response from Cluster Master: {error}"
            }

        except OSError as error:

            print(
                f"[CLUSTER CONNECTION ERROR] {error}"
            )

            return {
                "success": False,
                "peer_id": self.peer_id,
                "error": f"Unable to connect to Cluster Master: {error}"
            }

        except Exception as error:

            return {
                "success": False,
                "peer_id": self.peer_id,
                "error": str(error)
            }

    # -------------------------------------------------
    # CLIENT REQUEST HANDLER
    # -------------------------------------------------

    def handle_client(self, client_socket, address):

        try:

            with client_socket:

                client_socket.settimeout(10)

                data = b""

                while b"\n" not in data:

                    chunk = client_socket.recv(4096)

                    if not chunk:
                        return

                    data += chunk

                    if len(data) > 65536:

                        response = {
                            "success": False,
                            "error": "Request exceeds maximum size"
                        }

                        client_socket.sendall(
                            (
                                json.dumps(response) + "\n"
                            ).encode("utf-8")
                        )

                        return

                request = json.loads(
                    data.split(b"\n", 1)[0].decode("utf-8")
                )

                if not isinstance(request, dict):

                    response = {
                        "success": False,
                        "error": "Invalid request format"
                    }

                else:

                    command = request.get("command")

                    # ---------------------------------
                    # STATUS COMMAND
                    # ---------------------------------

                    if command == "STATUS":

                        response = {
                            "success": True,
                            "peer": self.get_status()
                        }

                    # ---------------------------------
                    # PING COMMAND
                    # ---------------------------------

                    elif command == "PING":

                        response = {
                            "success": True,
                            "message": "PONG",
                            "peer_id": self.peer_id
                        }

                    # ---------------------------------
                    # DIRECT PEER MESSAGE
                    # ---------------------------------

                    elif command == "SEND_MESSAGE":

                        sender = request.get(
                            "sender",
                            "UNKNOWN"
                        )

                        message = request.get(
                            "message",
                            ""
                        )

                        if (
                            not isinstance(message, str)
                            or not message.strip()
                        ):

                            response = {
                                "success": False,
                                "error": "Message cannot be empty"
                            }

                        else:

                            print(
                                f"\n[MESSAGE RECEIVED]\n"
                                f"From: {sender}\n"
                                f"To: {self.peer_id}\n"
                                f"Message: {message}\n"
                            )

                            response = {
                                "success": True,
                                "message": "Message delivered",
                                "sender": sender,
                                "receiver": self.peer_id
                            }

                    # ---------------------------------
                    # LIST AVAILABLE RESOURCES
                    # ---------------------------------

                    elif command == "LIST_RESOURCES":

                        response = {
                            "success": True,
                            "peer_id": self.peer_id,
                            "resources": self.list_resources()
                        }

                    # ---------------------------------
                    # DOWNLOAD RESOURCE
                    # ---------------------------------

                    elif command == "GET_RESOURCE":

                        filename = request.get(
                            "filename",
                            ""
                        )

                        response = self.get_resource(filename)

                    # ---------------------------------
                    # P2P + CLUSTER INTEGRATION
                    # ---------------------------------

                    elif command == "EXECUTE_CLUSTER_TASKS":

                        tasks = request.get("tasks", [])

                        # Allow enough time for cluster processing.
                        client_socket.settimeout(130)

                        response = self.submit_to_cluster(tasks)

                    # ---------------------------------
                    # UNKNOWN COMMAND
                    # ---------------------------------

                    else:

                        response = {
                            "success": False,
                            "error": "Unknown command"
                        }

                client_socket.sendall(
                    (
                        json.dumps(response) + "\n"
                    ).encode("utf-8")
                )

        except socket.timeout:

            print(
                f"[TIMEOUT] Client connection: {address}"
            )

        except (
            json.JSONDecodeError,
            UnicodeDecodeError
        ) as error:

            print(
                f"[INVALID REQUEST] {address}: {error}"
            )

            try:

                response = {
                    "success": False,
                    "error": "Invalid JSON request"
                }

                client_socket.sendall(
                    (
                        json.dumps(response) + "\n"
                    ).encode("utf-8")
                )

            except OSError:
                pass

        except OSError as error:

            print(
                f"[CONNECTION ERROR] {address}: {error}"
            )

    # -------------------------------------------------
    # START PEER SERVER
    # -------------------------------------------------

    def start(self):

        self.running = True

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

        print("\n" + "=" * 50)
        print("        DPCS PEER NODE STARTED")
        print("=" * 50)

        print(f" Peer ID      : {self.peer_id}")
        print(f" Host         : {self.host}")
        print(f" Port         : {self.port}")
        print(f" PID          : {os.getpid()}")
        print(f" Resources    : {self.resource_dir}")

        print(
            f" Cluster Host : "
            f"{self.master_host}:{self.master_port}"
        )

        print("=" * 50)
        print("Waiting for incoming connections...\n")

        try:

            while self.running:

                try:

                    client_socket, address = (
                        self.server_socket.accept()
                    )

                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )

                    client_thread.start()

                except socket.timeout:
                    continue

        except KeyboardInterrupt:

            print(
                f"\n[{self.peer_id}] Shutdown requested"
            )

        finally:

            self.stop()

    # -------------------------------------------------
    # STOP PEER SERVER
    # -------------------------------------------------

    def stop(self):

        self.running = False

        if self.server_socket:

            try:
                self.server_socket.close()

            except OSError:
                pass

            self.server_socket = None

        print(
            f"[{self.peer_id}] Node stopped"
        )


# -----------------------------------------------------
# MAIN FUNCTION
# -----------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="DPCS Independent Peer Node"
    )

    parser.add_argument(
        "--id",
        required=True
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--port",
        type=int,
        required=True
    )

    parser.add_argument(
        "--master-host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--master-port",
        type=int,
        default=6000
    )

    args = parser.parse_args()

    peer = PeerNode(
        peer_id=args.id,
        host=args.host,
        port=args.port,
        master_host=args.master_host,
        master_port=args.master_port
    )

    peer.start()


if __name__ == "__main__":
    main()
