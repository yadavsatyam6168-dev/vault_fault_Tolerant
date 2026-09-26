from flask import Flask, jsonify, request
import main  # Humara existing main.py import kar rahe hain

app = Flask(__name__)
main.setup_nodes()

@app.route("/")
def home():
    return app.send_static_file("index.html")

@app.route("/store", methods=["POST"])
def store():
    data = request.json
    filename = data.get("filename")
    replicas = data.get("replicas", 3)
    main.store_file(filename, replicas)
    return jsonify({"message": f"{filename} stored with {replicas} replicas"})

@app.route("/health/<filename>")
def health(filename):
    healthy, failed = main.check_node_health(filename)
    return jsonify({"healthy_nodes": healthy, "failed_nodes": failed})

@app.route("/fail", methods=["POST"])
def fail_node():
    data = request.json
    filename = data.get("filename")
    node = data.get("node")
    import os
    filepath = os.path.join(node, filename)
    if os.path.exists(filepath):
        os.remove(filepath)
        return jsonify({"message": f"{node} failed for {filename}"})
    return jsonify({"message": "File not found on this node"})

@app.route("/repair", methods=["POST"])
def repair():
    data = request.json
    filename = data.get("filename")
    main.repair_file(filename)
    return jsonify({"message": f"Repair attempted for {filename}"})

@app.route("/rebalance", methods=["POST"])
def rebalance_route():
    main.rebalance()
    return jsonify({"message": "Rebalancing done"})

@app.route("/retrieve/<filename>")
def retrieve(filename):
    path = main.retrieve_file(filename)
    if path:
        return jsonify({"message": f"File found", "path": path})
    return jsonify({"message": "File not found"})

@app.route("/storage-report")
def storage_report():
    main.storage_overhead_report()
    return jsonify({"message": "Check terminal for report"})

if __name__ == "__main__":
    app.run(debug=True, port=5000)