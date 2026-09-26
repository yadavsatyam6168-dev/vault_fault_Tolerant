import os
import shutil
import hashlib
import json
import threading
import time

NODES = ["node1", "node2", "node3"]
METADATA_FILE = "metadata.json"
PARTITIONED_NODES = set()
file_lock = threading.Lock()


def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {}


def save_metadata(data):
    with open(METADATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def setup_nodes():
    for node in NODES:
        os.makedirs(node, exist_ok=True)
    print("Nodes ready:", NODES)


def get_checksum(filepath):
    with open(filepath, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def toggle_partition(node):
    if node in PARTITIONED_NODES:
        PARTITIONED_NODES.remove(node)
        print(f"{node} network mein wapas aa gaya (reconnected).")
    else:
        PARTITIONED_NODES.add(node)
        print(f"{node} network se disconnect ho gaya (partitioned)!")


def store_file(filepath, replicas=3):
    if not os.path.exists(filepath):
        print("Error: File nahi mili:", filepath)
        return

    if replicas > len(NODES):
        print(f"Error: Sirf {len(NODES)} nodes hain, {replicas} replicas nahi ban sakte.")
        return

    filename = os.path.basename(filepath)
    checksum = get_checksum(filepath)
    print("Storing file:", filename)
    print("Checksum:", checksum)
    print(f"Replication factor: {replicas}")

    target_nodes = NODES[:replicas]

    for node in target_nodes:
        dest = os.path.join(node, filename)
        shutil.copy(filepath, dest)
        print(f"  -> Copied to {node}")

    metadata = load_metadata()
    metadata[filename] = {
        "checksum": checksum,
        "replicas": replicas,
        "nodes": target_nodes,
        "stored_at": str(__import__("datetime").datetime.now())
    }
    save_metadata(metadata)

    print("File replicated to", replicas, "nodes successfully! Metadata saved.")


def check_node_health(filename):
    healthy_nodes = []
    failed_nodes = []

    for node in NODES:
        filepath = os.path.join(node, filename)
        if node in PARTITIONED_NODES:
            failed_nodes.append(node)
        elif os.path.exists(filepath):
            healthy_nodes.append(node)
        else:
            failed_nodes.append(node)

    print("Healthy nodes:", healthy_nodes)
    print("Failed nodes:", failed_nodes)
    if PARTITIONED_NODES:
        print("Network partitioned nodes:", list(PARTITIONED_NODES))
    return healthy_nodes, failed_nodes


def repair_file(filename):
    start_time = time.time()
    healthy_nodes, failed_nodes = check_node_health(filename)

    if not failed_nodes:
        print("Sab nodes theek hain, repair ki zaroorat nahi.")
        return

    if not healthy_nodes:
        print("ERROR: Sabhi nodes fail ho gaye! File poori tarah kho gayi.")
        return

    source = os.path.join(healthy_nodes[0], filename)

    for node in failed_nodes:
        dest = os.path.join(node, filename)
        shutil.copy(source, dest)
        print(f"  -> Repaired! File copied back to {node}")

    print("Auto-repair complete. Sabhi nodes ab healthy hain.")
    elapsed = time.time() - start_time
    print(f"Recovery time: {elapsed:.4f} seconds")


def retrieve_file(filename):
    for node in NODES:
        filepath = os.path.join(node, filename)
        if node in PARTITIONED_NODES:
            continue
        if os.path.exists(filepath):
            print(f"File mil gayi node '{node}' mein.")
            print("Checksum:", get_checksum(filepath))
            return filepath
    print("Error: File kahin bhi nahi mili, sabhi replicas kho gaye ya unreachable hain.")
    return None


def storage_overhead_report():
    metadata = load_metadata()
    if not metadata:
        print("Koi file store nahi hui hai abhi.")
        return

    total_actual_size = 0
    total_single_copy_size = 0

    for filename, info in metadata.items():
        for node in info["nodes"]:
            filepath = os.path.join(node, filename)
            if os.path.exists(filepath):
                total_actual_size += os.path.getsize(filepath)

        for node in info["nodes"]:
            filepath = os.path.join(node, filename)
            if os.path.exists(filepath):
                total_single_copy_size += os.path.getsize(filepath)
                break

    if total_single_copy_size == 0:
        print("Koi file mili nahi calculate karne ke liye.")
        return

    overhead = total_actual_size - total_single_copy_size
    overhead_percent = (overhead / total_single_copy_size) * 100

    print("\n--- Storage Overhead Report ---")
    print(f"Single copy total size: {total_single_copy_size} bytes")
    print(f"Actual total size (with replicas): {total_actual_size} bytes")
    print(f"Extra storage due to replication: {overhead} bytes ({overhead_percent:.2f}% overhead)")


def concurrent_user_task(user_id, filename):
    with file_lock:
        print(f"[User {user_id}] File store kar raha hai...")
        store_file(filename, replicas=3)

    time.sleep(0.5)

    with file_lock:
        print(f"[User {user_id}] File retrieve kar raha hai...")
        retrieve_file(filename)


def simulate_concurrent_access():
    filename = input("Kis file ke liye concurrent access simulate karna hai: ")
    num_users_input = input("Kitne users ek saath access karenge (jaise 3): ")
    num_users = int(num_users_input)

    if not os.path.exists(filename):
        print("Pehle yeh file store karo (option 1 se), tabhi simulate ho sakta hai.")
        return

    threads = []
    print(f"\n--- {num_users} users ek saath access kar rahe hain ---")

    for i in range(num_users):
        t = threading.Thread(target=concurrent_user_task, args=(i + 1, filename))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("--- Sabhi users ka access complete ---")


def rebalance():
    start_time = time.time()
    metadata = load_metadata()

    if not metadata:
        print("Koi file store nahi hui hai abhi, rebalance karne ko kuch nahi hai.")
        return

    print("\n--- Rebalancing shuru ---")

    for filename, info in metadata.items():
        expected_replicas = info["replicas"]
        healthy_nodes, failed_nodes = check_node_health(filename)
        current_count = len(healthy_nodes)

        print(f"\nFile: {filename}")
        print(f"  Expected replicas: {expected_replicas}, Current healthy copies: {current_count}")

        if current_count < expected_replicas:
            needed = expected_replicas - current_count
            available_targets = [n for n in NODES if n not in healthy_nodes][:needed]

            if not healthy_nodes:
                print("  ERROR: Koi healthy copy nahi mili, rebalance nahi ho sakta.")
                continue

            source = os.path.join(healthy_nodes[0], filename)
            for node in available_targets:
                dest = os.path.join(node, filename)
                shutil.copy(source, dest)
                print(f"  -> Rebalanced: copied to {node}")

            info["nodes"] = healthy_nodes + available_targets
            metadata[filename] = info
        else:
            print("  Sab theek hai, rebalance ki zaroorat nahi.")

    save_metadata(metadata)
    print("\n--- Rebalancing complete ---")
    elapsed = time.time() - start_time
    print(f"Total rebalancing time: {elapsed:.4f} seconds")


def menu():
    setup_nodes()
    print("\n===== VAULT: Fault-Tolerant Storage System =====")

    while True:
        print("\nKya karna hai?")
        print("1. File store karo")
        print("2. Node health check karo")
        print("3. Node fail simulate karo")
        print("4. Auto-repair chalao")
        print("5. File retrieve karo")
        print("6. Exit")
        print("7. Rebalance karo (sabhi files check karke fix karo)")
        print("8. Network partition toggle karo (node ko disconnect/reconnect karo)")
        print("9. Concurrent access simulate karo (multiple users)")
        print("10. Storage overhead report dekho")

        choice = input("Apna choice number likho: ")

        if choice == "1":
            filename = input("Kis file ka naam store karna hai (jaise test.txt): ")
            replicas_input = input(f"Kitne replicas banane hain (1 se {len(NODES)}, default 3): ")
            if replicas_input.strip() == "":
                replicas = 3
            else:
                replicas = int(replicas_input)
            store_file(filename, replicas)

        elif choice == "2":
            filename = input("Kis file ka health check karna hai: ")
            check_node_health(filename)

        elif choice == "3":
            filename = input("Kis file ke liye node fail karna hai: ")
            node = input("Kaunsa node fail karna hai (node1/node2/node3): ")
            filepath = os.path.join(node, filename)
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"{node} ko FAIL kar diya (file delete kar di)!")
            else:
                print("Yeh file is node mein already nahi hai.")

        elif choice == "4":
            filename = input("Kis file ko repair karna hai: ")
            repair_file(filename)

        elif choice == "5":
            filename = input("Kis file ko retrieve karna hai: ")
            retrieve_file(filename)

        elif choice == "7":
            rebalance()

        elif choice == "8":
            node = input("Kaunsa node partition/reconnect karna hai (node1/node2/node3): ")
            toggle_partition(node)

        elif choice == "9":
            simulate_concurrent_access()

        elif choice == "10":
            storage_overhead_report()

        elif choice == "6":
            print("Vault band ho raha hai. Bye!")
            break

        else:
            print("Galat choice, dobara try karo.")


if __name__ == "__main__":
    menu()