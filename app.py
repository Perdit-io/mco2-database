import time

import mysql.connector
from flask import Flask, jsonify, request
from mysql.connector import Error

import db_config

app = Flask(__name__)


# --- HELPER: Connect to a specific Node ---
def get_db_connection(node_name):
    config = db_config.NODE_CONFIG[node_name]
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"Error connecting to {node_name}: {e}")
        return None


# --- HELPER: Router (Location Transparency) ---
# Decides which fragment node (2 or 3) holds the data based on the year
def get_fragment_node(year):
    if int(year) < db_config.FRAGMENTATION_YEAR:
        return "node2"  # Old Movies
    else:
        return "node3"  # New Movies


# --- HELPER: Save Failed Transaction to Local Log ---
def log_failed_transaction(local_node_conn, target_node, query, params):
    try:
        cursor = local_node_conn.cursor()
        # We store params as a JSON string to retrieve them easily later
        params_str = json.dumps(params)
        log_query = "INSERT INTO recovery_log (target_node, query_text, params_text) VALUES (%s, %s, %s)"
        cursor.execute(log_query, (target_node, query, params_str))
        local_node_conn.commit()
        print(f"Logged failed transaction for {target_node}")
    except Error as e:
        print(f"Failed to log transaction: {e}")


# =====================================================
#  FEATURE 1: CRUD with FAILURE HANDLING
# =====================================================


@app.route("/movies", methods=["GET"])
def get_movies():
    year_filter = request.args.get("year")
    if year_filter:
        target_node = get_fragment_node(year_filter)
    else:
        target_node = "node1"

    conn = get_db_connection(target_node)
    if not conn:
        return jsonify({"error": f"Failed to connect to {target_node}"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM movies"
        if year_filter:
            query += f" WHERE year = {year_filter}"
        query += " LIMIT 100"

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify({"source_node": target_node, "data": results})
    except Error as e:
        return jsonify({"error": str(e)}), 500


@app.route("/movies", methods=["POST"])
def add_movie():
    data = request.json
    year = int(data.get("year"))

    # 1. Determine Nodes
    fragment_node = get_fragment_node(year)
    nodes_to_update = ["node1", fragment_node]

    query = "INSERT INTO movies (id, title, year, rating, genre) VALUES (%s, %s, %s, %s, %s)"
    vals = (
        data.get("id"),
        data.get("title"),
        year,
        data.get("rating"),
        data.get("genre"),
    )

    success_count = 0
    errors = []

    # We need a "Primary" connection (usually Node 1) to store logs if the other fails
    # If Node 1 itself fails, we store logs on the Fragment node.

    active_connections = {}

    # 2. Try to Execute on All Nodes
    for node in nodes_to_update:
        conn = get_db_connection(node)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, vals)
                conn.commit()
                cursor.close()
                active_connections[node] = conn  # Keep open for logging if needed
                success_count += 1
            except Error as e:
                errors.append(f"{node} Error: {str(e)}")
                conn.close()  # Close if query failed
        else:
            errors.append(f"{node} Connection Failed")

    # 3. Recovery Logic: If one failed but other succeeded, LOG IT
    if success_count == 1:
        failed_node = [n for n in nodes_to_update if n not in active_connections][0]
        succeeded_node = [n for n in active_connections.keys()][0]

        print(
            f"Partial Failure! Logging {failed_node} transaction to {succeeded_node}..."
        )
        log_failed_transaction(
            active_connections[succeeded_node], failed_node, query, vals
        )

        # Cleanup
        active_connections[succeeded_node].close()

        return jsonify(
            {
                "status": "partial_success",
                "message": f"Written to {succeeded_node}, Logged for {failed_node}",
            }
        ), 201

    # Cleanup normal case
    for conn in active_connections.values():
        conn.close()

    if success_count == 2:
        return jsonify({"status": "success"}), 201
    else:
        return jsonify({"status": "failure", "errors": errors}), 500


# =====================================================
#  FEATURE 2: RECOVERY ROUTE (Trigger this to sync)
# =====================================================
@app.route("/recover", methods=["POST"])
def recover_node():
    # Helper to force a sync. E.g., POST /recover {"source": "node1", "target": "node2"}
    data = request.json
    source_node = data.get("source")  # Where the logs are (e.g., node1)
    target_node = data.get("target")  # Who just came back online (e.g., node2)

    source_conn = get_db_connection(source_node)
    target_conn = get_db_connection(target_node)

    if not source_conn or not target_conn:
        return jsonify({"error": "Cannot connect to one of the nodes"}), 500

    try:
        # 1. Fetch Logs
        source_cursor = source_conn.cursor(dictionary=True)
        # Get logs specifically for this target
        source_cursor.execute(
            "SELECT * FROM recovery_log WHERE target_node = %s ORDER BY id ASC",
            (target_node,),
        )
        logs = source_cursor.fetchall()

        if not logs:
            return jsonify({"status": "clean", "message": "No pending logs found."})

        print(f"Found {len(logs)} pending transactions for {target_node}")

        # 2. Replay Logs
        target_cursor = target_conn.cursor()
        ids_to_delete = []

        for log in logs:
            query = log["query_text"]
            params = tuple(
                json.loads(log["params_text"])
            )  # Convert back from list to tuple

            try:
                target_cursor.execute(query, params)
                target_conn.commit()
                ids_to_delete.append(log["id"])
            except Error as e:
                print(f"Failed to replay log {log['id']}: {e}")
                # Optional: Stop or Skip? For demo, we skip.

        # 3. Clean up Logs
        if ids_to_delete:
            format_strings = ",".join(["%s"] * len(ids_to_delete))
            delete_query = f"DELETE FROM recovery_log WHERE id IN ({format_strings})"
            source_cursor.execute(delete_query, tuple(ids_to_delete))
            source_conn.commit()

        return jsonify({"status": "success", "recovered_count": len(ids_to_delete)})

    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        source_conn.close()
        target_conn.close()


# =====================================================
#  FEATURE 3: CONCURRENCY EXPERIMENTS (Simulation)
# =====================================================


@app.route("/transaction", methods=["POST"])
def execute_transaction():
    data = request.json
    year = int(data.get("year", 2000))
    action = data.get("action")
    isolation_level = data.get("isolation_level", "READ COMMITTED")
    sleep_time = data.get("sleep", 0)

    # Route to correct node
    node_name = get_fragment_node(year)
    conn = get_db_connection(node_name)

    if not conn:
        return jsonify({"error": "Connection failed"}), 500

    conn.autocommit = False

    results = None
    try:
        cursor = conn.cursor(dictionary=True)

        # 1. Set Isolation Level
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")

        # 2. Start Transaction
        conn.start_transaction()
        print(f"[{node_name}] Transaction Started ({isolation_level})...")

        if action == "read":
            cursor.execute("SELECT * FROM movies WHERE year = %s LIMIT 1", (year,))
            results = cursor.fetchall()
            if sleep_time > 0:
                time.sleep(sleep_time)  # Simulate holding shared lock

        elif action == "write":
            new_rating = data.get("rating")
            target_id = data.get("id")  # <--- NEW: Accept specific ID

            if target_id:
                # Strict collision test: Update specific ID
                print(f"Updating specific ID: {target_id}")
                cursor.execute(
                    "UPDATE movies SET rating = %s WHERE id = %s",
                    (new_rating, target_id),
                )
            else:
                # Loose test: Update any movie in that year (Existing logic)
                cursor.execute(
                    "UPDATE movies SET rating = %s WHERE year = %s LIMIT 1",
                    (new_rating, year),
                )

            rows_affected = cursor.rowcount
            print(f"[{node_name}] Rows affected/locked: {rows_affected}")

            if sleep_time > 0:
                print(f"Sleeping for {sleep_time}s (holding lock)...")
                time.sleep(sleep_time)

        # 3. Commit
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"status": "success", "node": node_name, "data": results})

    except Error as e:
        print(f"Transaction Error: {e}")
        if conn.is_connected():
            conn.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    # Run on 0.0.0.0 so it is accessible from the outside world
    # Run on Port 80
    app.run(host="0.0.0.0", port=80, threaded=True)
