
import json
import socket

PEERS = [
    ("PEER-01", "127.0.0.1", 5001),
    ("PEER-02", "127.0.0.1", 5002),
    ("PEER-03", "127.0.0.1", 5003),
    ("PEER-04", "127.0.0.1", 5004),
]


def check_peer(peer_id, host, port):
    try:
        with socket.create_connection(
            (host, port), timeout=3
        ) as client:

            request = {"command": "STATUS"}

            client.sendall(
                (json.dumps(request) + "\n").encode()
            )

            response = b""

            while b"\n" not in response:
                chunk = client.recv(4096)
                if not chunk:
                    break
                response += chunk

            result = json.loads(response.split(b"\n")[0])

            if result.get("success"):
                peer = result["peer"]

                print(
                    f"[ONLINE] {peer['peer_id']} | "
                    f"Port: {peer['port']} | "
                    f"State: {peer['state']}"
                )
                return True

    except (OSError, json.JSONDecodeError, KeyError) as error:
        print(f"[OFFLINE] {peer_id} | {error}")

    return False


if __name__ == "__main__":
    print("\nDPCS PEER NETWORK HEALTH CHECK")
    print("-" * 45)

    online = 0

    for peer in PEERS:
        if check_peer(*peer):
            online += 1

    print("-" * 45)
    print(f"Total Peers : {len(PEERS)}")
    print(f"Online      : {online}")
    print(f"Offline     : {len(PEERS) - online}")

    if online == len(PEERS):
        print("\nSUCCESS: All peer nodes are operational!")
    else:
        print("\nWARNING: Some peer nodes are unavailable.")
