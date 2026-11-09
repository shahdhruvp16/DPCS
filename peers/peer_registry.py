PEERS = {
    "PEER-01": {
        "host": "127.0.0.1",
        "port": 5001
    },
    "PEER-02": {
        "host": "127.0.0.1",
        "port": 5002
    },
    "PEER-03": {
        "host": "127.0.0.1",
        "port": 5003
    },
    "PEER-04": {
        "host": "127.0.0.1",
        "port": 5004
    }
}


def get_peer(peer_id):
    return PEERS.get(peer_id)


def get_all_peers():
    return PEERS.copy()