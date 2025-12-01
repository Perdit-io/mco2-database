# Configuration for the 3 Distributed Nodes
NODE_CONFIG = {
    "node1": {
        "host": "192.168.x.x",  # REPLACE with Node 1 (Central) IP
        "user": "admin",
        "password": "password123",
        "database": "STADVDB",
        "port": 3306,
    },
    "node2": {
        "host": "192.168.x.x",  # REPLACE with Node 2 (< 1980) IP
        "user": "admin",
        "password": "password123",
        "database": "STADVDB",
        "port": 3306,
    },
    "node3": {
        "host": "192.168.x.x",  # REPLACE with Node 3 (>= 1980) IP
        "user": "admin",
        "password": "password123",
        "database": "STADVDB",
        "port": 3306,
    },
}

# Fragmentation Rule (The "Cutoff" Year)
FRAGMENTATION_YEAR = 1980
