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


# =====================================================
#  FEATURE 1: STANDARD CRUD (View & Add Movies)
# =====================================================


@app.route("/movies", methods=["GET"])
def get_movies():
    year_filter = request.args.get("year")

    # Router Logic
    if year_filter:
        target_node = get_fragment_node(year_filter)
    else:
        target_node = "node1"  # Default to Central

    # Execute Query
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
    # This handles "Update-Anywhere" Replication
    data = request.json
    year = int(data.get("year"))

    # 1. Determine Target Nodes (Central + Fragment)
    fragment_node = get_fragment_node(year)
    nodes_to_update = ["node1", fragment_node]

    success_count = 0
    errors = []

    query = "INSERT INTO movies (id, title, year, rating, genre) VALUES (%s, %s, %s, %s, %s)"
    vals = (
        data.get("id"),
        data.get("title"),
        year,
        data.get("rating"),
        data.get("genre"),
    )

    # 2. Replicate to both
    for node in nodes_to_update:
        conn = get_db_connection(node)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, vals)
                conn.commit()
                cursor.close()
                conn.close()
                success_count += 1
            except Error as e:
                errors.append(f"{node} Error: {str(e)}")
        else:
            errors.append(f"{node} Connection Failed")

    if success_count == 2:
        return jsonify(
            {"status": "success", "message": "Replicated to both nodes"}
        ), 201
    else:
        return jsonify({"status": "partial_error", "errors": errors}), 206


# =====================================================
#  FEATURE 2: CONCURRENCY EXPERIMENTS (Simulation)
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
