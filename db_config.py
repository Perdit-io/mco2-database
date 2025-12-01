# Configuration for the 3 Distributed Nodes
NODE_CONFIG = {
    "node1": {
        "host": "10.2.14.60",  # Node 1 (Central) IP
        "user": "root",
        "password": "",
        "database": "STADVDB",
        "port": 3306,
    },
    "node2": {
        "host": "10.2.14.61",  # Node 2 (< 1980) IP
        "user": "root",
        "password": "",
        "database": "STADVDB",
        "port": 3306,
    },
    "node3": {
        "host": "10.2.14.62",  # Node 3 (>= 1980) IP
        "user": "root",
        "password": "",
        "database": "STADVDB",
        "port": 3306,
    },
}

# Fragmentation Rule (The "Cutoff" Year)
FRAGMENTATION_YEAR = 1980
