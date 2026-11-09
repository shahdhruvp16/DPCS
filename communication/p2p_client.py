
import base64
import json
import socket
from pathlib import Path

from peers.peer_registry import get_peer


# -----------------------------------------------------
# SEND AND RECEIVE JSON REQUESTS
# -----------------------------------------------------

def send_request(host, port, request):

    with socket.create_connection(
        (host, port),
        timeout=5
    ) as client:

        client.settimeout(15)

        client.sendall(
            (json.dumps(request) + "\n").encode("utf-8")
        )

        response = b""

        while b"\n" not in response:

            chunk = client.recv(65536)

            if not chunk:
                raise ConnectionError(
                    "Peer closed connection without responding"
                )

            response += chunk

            if len(response) > 8 * 1024 * 1024:
                raise ValueError(
                    "Response exceeds maximum allowed size"
                )

        return json.loads(
            response.split(b"\n", 1)[0].decode("utf-8")
        )


# -----------------------------------------------------
# DIRECT PEER-TO-PEER MESSAGING
# -----------------------------------------------------

def send_message(sender_id, receiver_id, message):

    peer = get_peer(receiver_id)

    if peer is None:

        print(f"[ERROR] Unknown peer: {receiver_id}")
        return False

    if not isinstance(message, str) or not message.strip():

        print("[ERROR] Message cannot be empty")
        return False

    request = {
        "command": "SEND_MESSAGE",
        "sender": sender_id,
        "message": message
    }

    try:

        response = send_request(
            peer["host"],
            peer["port"],
            request
        )

        if response.get("success"):

            print(
                f"[SUCCESS] {sender_id} -> {receiver_id}"
            )

            print(f"Message: {message}")

            return True

        print(
            f"[FAILED] {response.get('error')}"
        )

        return False

    except (OSError, ValueError) as error:

        print(
            f"[CONNECTION FAILED] "
            f"{receiver_id}: {error}"
        )

        return False


# -----------------------------------------------------
# CHECK PEER AVAILABILITY
# -----------------------------------------------------

def check_peer(peer_id):

    peer = get_peer(peer_id)

    if peer is None:
        return False

    try:

        response = send_request(
            peer["host"],
            peer["port"],
            {"command": "PING"}
        )

        return response.get("success", False)

    except (OSError, ValueError):
        return False


# -----------------------------------------------------
# LIST RESOURCES FROM ANOTHER PEER
# -----------------------------------------------------

def list_resources(requester_id, provider_id):

    if get_peer(requester_id) is None:
        print(f"[ERROR] Unknown requester: {requester_id}")
        return []

    peer = get_peer(provider_id)

    if peer is None:
        print(f"[ERROR] Unknown provider: {provider_id}")
        return []

    try:

        response = send_request(
            peer["host"],
            peer["port"],
            {
                "command": "LIST_RESOURCES"
            }
        )

        if not response.get("success"):

            print("[FAILED] Could not list resources")
            return []

        resources = response.get("resources", [])

        print(
            f"\nRESOURCES AVAILABLE ON {provider_id}"
        )

        print("-" * 55)

        if not resources:

            print("No resources available.")

        for resource in resources:

            print(
                f"Filename: {resource['filename']} | "
                f"Size: {resource['size_bytes']} bytes"
            )

        print("-" * 55)

        print(f"Total resources: {len(resources)}")

        return resources

    except (OSError, ValueError, KeyError) as error:

        print(
            f"[RESOURCE ERROR] {error}"
        )

        return []


# -----------------------------------------------------
# DOWNLOAD RESOURCE FROM ANOTHER PEER
# -----------------------------------------------------

def download_resource(
    requester_id,
    provider_id,
    filename
):

    if get_peer(requester_id) is None:

        print(f"[ERROR] Unknown requester: {requester_id}")
        return False

    peer = get_peer(provider_id)

    if peer is None:

        print(f"[ERROR] Unknown provider: {provider_id}")
        return False

    if (
        not isinstance(filename, str)
        or not filename.strip()
        or filename in (".", "..")
        or Path(filename).name != filename
        or "/" in filename
        or "\\" in filename
    ):

        print("[ERROR] Invalid filename")
        return False

    request = {
        "command": "GET_RESOURCE",
        "filename": filename
    }

    try:

        response = send_request(
            peer["host"],
            peer["port"],
            request
        )

        if not response.get("success"):

            print(
                f"[DOWNLOAD FAILED] "
                f"{response.get('error')}"
            )

            return False

        content = base64.b64decode(
            response["content"],
            validate=True
        )

        expected_size = response["size_bytes"]

        if len(content) != expected_size:

            print(
                "[DOWNLOAD FAILED] Size verification failed"
            )

            return False

        # Create requester-specific download directory
        project_root = (
            Path(__file__).resolve().parent.parent
        )

        download_dir = (
            project_root
            / "downloads"
            / requester_id
        )

        download_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        destination = download_dir / filename

        destination.write_bytes(content)

        print("\n" + "=" * 45)
        print(" RESOURCE DOWNLOAD SUCCESSFUL")
        print("=" * 45)
        print(f"Filename  : {filename}")
        print(f"Provider  : {provider_id}")
        print(f"Requester : {requester_id}")
        print(f"Size      : {expected_size} bytes")
        print(f"Saved to  : {destination}")
        print("=" * 45)

        return True

    except (OSError, ValueError, KeyError) as error:

        print(
            f"[DOWNLOAD ERROR] {error}"
        )

        return False


# -----------------------------------------------------
# INTERACTIVE CLIENT MENU
# -----------------------------------------------------

def main():

    print("\n" + "=" * 45)
    print(" DPCS P2P COMMUNICATION CLIENT")
    print("=" * 45)

    print("1. Send Message")
    print("2. Check Peer Availability")
    print("3. List Peer Resources")
    print("4. Download Peer Resource")
    print("5. Exit")

    while True:

        print("\n" + "-" * 45)

        choice = input(
            "Select operation (1-5): "
        ).strip()

        if choice == "1":

            sender = input(
                "Enter sender Peer ID: "
            ).strip().upper()

            receiver = input(
                "Enter receiver Peer ID: "
            ).strip().upper()

            message = input(
                "Enter message: "
            ).strip()

            send_message(
                sender,
                receiver,
                message
            )

        elif choice == "2":

            peer_id = input(
                "Enter Peer ID: "
            ).strip().upper()

            if check_peer(peer_id):

                print(
                    f"[ONLINE] {peer_id}"
                )

            else:

                print(
                    f"[OFFLINE] {peer_id}"
                )

        elif choice == "3":

            requester = input(
                "Enter requester Peer ID: "
            ).strip().upper()

            provider = input(
                "Enter provider Peer ID: "
            ).strip().upper()

            list_resources(
                requester,
                provider
            )

        elif choice == "4":

            requester = input(
                "Enter requester Peer ID: "
            ).strip().upper()

            provider = input(
                "Enter provider Peer ID: "
            ).strip().upper()

            filename = input(
                "Enter filename to download: "
            ).strip()

            download_resource(
                requester,
                provider,
                filename
            )

        elif choice == "5":

            print("Exiting DPCS client.")
            break

        else:

            print(
                "[ERROR] Invalid selection. Choose 1-5."
            )


if __name__ == "__main__":
    main()
