import json
import threading
import time

import requests

# ==============================================================================
# CONFIGURATION
# ==============================================================================
NODES = [
    {"name": "Server 0 (Central / Node 1)", "url": "http://ccscloud.dlsu.edu.ph:60160"},
    {"name": "Server 1 (Fragment < 1980)", "url": "http://ccscloud.dlsu.edu.ph:60161"},
    {"name": "Server 2 (Fragment >= 1980)", "url": "http://ccscloud.dlsu.edu.ph:60162"},
]

# TARGETS
CONCURRENCY_YEAR = 1950  # Targets Node 2 (<1980)
RECOVERY_YEAR = 1975  # Targets Node 2 (<1980)
RECOVERY_ID = "rec_test"

# FULL LIST OF ISOLATION LEVELS
ISOLATION_LEVELS = [
    "READ UNCOMMITTED",
    "READ COMMITTED",
    "REPEATABLE READ",
    "SERIALIZABLE",
]


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def get_target_id(base_url, year):
    try:
        resp = requests.get(f"{base_url}/movies?year={year}", timeout=5)
        if resp.status_code == 200 and resp.json().get("data"):
            return resp.json()["data"][0]["id"]
    except Exception as e:
        print(f"   [Error connecting to {base_url}] {e}")
    return None


def cleanup_recovery_data(base_url):
    try:
        requests.delete(
            f"{base_url}/movies?id={RECOVERY_ID}&year={RECOVERY_YEAR}", timeout=5
        )
    except:
        pass


def check_movie_exists(base_url, year, movie_id):
    try:
        resp = requests.get(f"{base_url}/movies?year={year}", timeout=5)
        data = resp.json().get("data", [])
        for movie in data:
            if movie["id"] == movie_id:
                return True
    except:
        pass
    return False


def simulate_concurrency_user(
    base_url, user_id, action, isolation, sleep_time, movie_id, result_collector
):
    payload = {
        "year": CONCURRENCY_YEAR,
        "id": movie_id,
        "action": action,
        "isolation_level": isolation,
        "sleep": sleep_time,
        "rating": 5.5 if user_id == 1 else 9.9,
    }

    start_time = time.time()
    try:
        resp = requests.post(f"{base_url}/transaction", json=payload, timeout=10)
        duration = time.time() - start_time

        data = resp.json().get("data")
        seen_value = "N/A"
        if action == "read" and data:
            seen_value = data[0]["rating"]

        result_collector[user_id] = {
            "duration": round(duration, 2),
            "seen_value": seen_value,
            "status": resp.status_code,
        }
    except Exception as e:
        print(f"User {user_id} Error: {e}")


# ==============================================================================
# PART 1: CONCURRENCY CONTROL (Full Matrix on All Nodes)
# ==============================================================================
def run_concurrency_matrix():
    print("\n" + "=" * 60)
    print("PART 1: CONCURRENCY & TRANSPARENCY CHECK")
    print("Goal: Prove consistency across 3 Cases x 4 Isolation Levels x 3 Nodes")
    print("=" * 60)

    for node_config in NODES:
        node_name = node_config["name"]
        base_url = node_config["url"]

        print(f"\n>>> TESTING ON: {node_name} <<<")

        target_id = get_target_id(base_url, CONCURRENCY_YEAR)
        if not target_id:
            print(f"    Error: Could not fetch data from {node_name}. Skipping...")
            continue
        print(f"    Target ID: {target_id}")

        for iso in ISOLATION_LEVELS:
            print(f"    [Isolation: {iso}]")

            # CASE 1: Read-Read (Expect: Fast)
            run_concurrency_case(
                base_url, "C1: Read-Read  ", iso, "read", "read", 2, target_id
            )

            # CASE 2: Write-Read (Expect: Fast unless Serializable)
            run_concurrency_case(
                base_url, "C2: Write-Read ", iso, "write", "read", 2, target_id
            )

            # CASE 3: Write-Write (Expect: Blocked)
            run_concurrency_case(
                base_url, "C3: Write-Write", iso, "write", "write", 2, target_id
            )


def run_concurrency_case(
    base_url, case_name, isolation, t1_action, t2_action, t1_sleep, movie_id
):
    results = {}
    t1 = threading.Thread(
        target=simulate_concurrency_user,
        args=(base_url, 1, t1_action, isolation, t1_sleep, movie_id, results),
    )
    t2 = threading.Thread(
        target=simulate_concurrency_user,
        args=(base_url, 2, t2_action, isolation, 0, movie_id, results),
    )

    t1.start()
    time.sleep(0.5)
    t2.start()

    t1.join()
    t2.join()

    u2_stats = results.get(2, {})
    duration = u2_stats.get("duration", 0)
    val = u2_stats.get("seen_value", "N/A")

    # Interpretation Logic
    status = "NOT BLOCKED"
    if duration > 0.8:
        status = "BLOCKED    "  # Padding for alignment

    print(f"       -> {case_name} | Time: {duration}s | Val: {val} | Result: {status}")


# ==============================================================================
# PART 2: GLOBAL FAILURE RECOVERY (Both Scenarios)
# ==============================================================================
def run_recovery_suite():
    # Define Controllers
    CENTRAL_NODE_URL = NODES[0]["url"]  # Server 0
    NODE_2_URL = NODES[1]["url"]  # Server 1 (Use this to control when Central is down)

    print("\n" + "=" * 60)
    print("PART 2: GLOBAL FAILURE RECOVERY")
    print("Goal: Test bidirectional failure (Central down vs. Fragment down)")
    print("=" * 60)

    # Clean start
    cleanup_recovery_data(CENTRAL_NODE_URL)
    time.sleep(1)

    # --- SCENARIO A: STOP NODE 2 (Simulates Cases #3 & #4) ---
    print("\n--- SCENARIO A: Replication Failure (Central -> Node 2) ---")
    print("We will attempt to write to Node 2 while it is DOWN.")

    input("ACTION: Stop 'mysql' on Server 1 (Node 2) now. Press Enter...")

    # Insert via Central
    payload = {
        "id": RECOVERY_ID,
        "title": "Recovery Test A",
        "year": RECOVERY_YEAR,
        "rating": 5.0,
        "genre": "Test",
    }
    try:
        resp = requests.post(f"{CENTRAL_NODE_URL}/movies", json=payload, timeout=5)
        if resp.status_code == 201 and "partial" in resp.json().get("status", ""):
            print(">>> PASS: Central Node detected failure and logged the transaction.")
        else:
            print(f">>> FAIL: Status {resp.status_code} - {resp.json()}")
    except Exception as e:
        print(f"Error: {e}")

    # Restore
    input("ACTION: Start 'mysql' on Server 1 (Node 2) now. Press Enter...")

    # Verify Missing
    if check_movie_exists(CENTRAL_NODE_URL, RECOVERY_YEAR, RECOVERY_ID):
        print(">>> FAIL: Data appeared magically?")
    else:
        print(">>> PASS: Data is missing from Node 2 as expected.")

    # Recover
    print("Triggering Recovery...")
    requests.post(
        f"{CENTRAL_NODE_URL}/recover", json={"source": "node1", "target": "node2"}
    )

    time.sleep(1)
    if check_movie_exists(CENTRAL_NODE_URL, RECOVERY_YEAR, RECOVERY_ID):
        print(">>> PASS: Data recovered successfully!")
    else:
        print(">>> FAIL: Recovery did not sync data.")

    # Cleanup for Next Test
    cleanup_recovery_data(CENTRAL_NODE_URL)

    # --- SCENARIO B: STOP CENTRAL NODE (Simulates Cases #1 & #2) ---
    print("\n--- SCENARIO B: Replication Failure (Node 2 -> Central) ---")
    print("We will attempt to write from Node 2 while Central MySQL is DOWN.")
    print("NOTE: We send the request to Server 1 (Node 2) because Server 0 is 'down'.")

    input(
        "ACTION: Stop 'mysql' on Server 0 (Central) now. (KEEP PYTHON RUNNING!). Press Enter..."
    )

    # Insert via Node 2 (Server 1)
    payload["title"] = "Recovery Test B"
    try:
        # Note: We send this request to NODE_2_URL, not Central!
        resp = requests.post(f"{NODE_2_URL}/movies", json=payload, timeout=5)
        if resp.status_code == 201 and "partial" in resp.json().get("status", ""):
            print(
                ">>> PASS: Node 2 detected Central failure and logged the transaction."
            )
        else:
            print(f">>> FAIL: Status {resp.status_code} - {resp.json()}")
    except Exception as e:
        print(f"Error: {e}")

    # Restore
    input("ACTION: Start 'mysql' on Server 0 (Central) now. Press Enter...")

    # Recover (Triggered from Node 2 pushing to Node 1)
    print("Triggering Recovery...")
    requests.post(f"{NODE_2_URL}/recover", json={"source": "node2", "target": "node1"})

    time.sleep(1)
    # Check consistency via Central to prove it got the data back
    if check_movie_exists(CENTRAL_NODE_URL, RECOVERY_YEAR, RECOVERY_ID):
        print(">>> PASS: Central Node recovered successfully!")
    else:
        print(">>> FAIL: Central Node missing data.")

    # Final Cleanup
    cleanup_recovery_data(CENTRAL_NODE_URL)
    print("\n=== FULL TEST SUITE COMPLETE ===")


# ==============================================================================
# MAIN RUNNER
# ==============================================================================
if __name__ == "__main__":
    try:
        if "<YOUR_SERVER_0_PORT>" in NODES[0]["url"]:
            print("ERROR: Please update the NODES list with your ports.")
        else:
            run_concurrency_matrix()
            run_recovery_suite()
    except KeyboardInterrupt:
        print("\nTest Cancelled.")
