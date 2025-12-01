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


# --- ROUTE 1: Search (Read Query) ---
# Demonstrates "Fragmentation Transparency" - User doesn't know where data comes from
@app.route("/movies", methods=["GET"])
def get_movies():
    year_filter = request.args.get("year")

    # 1. Router Logic
    if year_filter:
        target_node = get_fragment_node(year_filter)
        print(f"Routing query for year {year_filter} to {target_node}")
    else:
        # If no year is specified, we must query Node 1 (Central) to get everything
        target_node = "node1"
        print("No year specified. Routing to Central Node 1.")

    # 2. Execute Query
    conn = get_db_connection(target_node)
    if not conn:
        return jsonify({"error": f"Failed to connect to {target_node}"}), 500

    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM movies"

    # Add filtering if needed
    if year_filter:
        query += f" WHERE year = {year_filter}"

    query += " LIMIT 100"  # Safety limit

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return jsonify({"source_node": target_node, "data": results})


# --- ROUTE 2: Insert (Write Query with Replication) ---
# Demonstrates "Update-Anywhere" & Synchronous Replication
@app.route("/movies", methods=["POST"])
def add_movie():
    data = request.json
    movie_id = data.get("id")
    title = data.get("title")
    year = int(data.get("year"))
    rating = data.get("rating")
    genre = data.get("genre")

    # 1. Determine Target Nodes
    # We must write to Central (Node 1) AND the correct Fragment (Node 2 or 3)
    fragment_node_name = get_fragment_node(year)
    nodes_to_update = ["node1", fragment_node_name]

    print(f"Replicating insert to: {nodes_to_update}")

    # 2. Execute Distributed Transaction
    success_count = 0
    errors = []

    query = "INSERT INTO movies (id, title, year, rating, genre) VALUES (%s, %s, %s, %s, %s)"
    vals = (movie_id, title, year, rating, genre)

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

    # 3. Response logic
    if success_count == 2:
        return jsonify(
            {"status": "success", "message": "Replicated to both nodes"}
        ), 201
    elif success_count == 1:
        # This is where you'd implement "Recovery" logic later (Case #1)
        return jsonify(
            {
                "status": "partial_success",
                "message": "Written to 1 node only",
                "errors": errors,
            }
        ), 206
    else:
        return jsonify({"status": "failure", "errors": errors}), 500


# --- MAIN ---
if __name__ == "__main__":
    # Run on 0.0.0.0 so it is accessible from the outside world
    app.run(host="0.0.0.0", port=5000)
