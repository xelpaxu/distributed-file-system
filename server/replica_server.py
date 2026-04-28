# =============================================================================
# server/replica_server.py
# =============================================================================
#
# PURPOSE:
#   The BACKUP REPLICA MANAGER (secondary) in our passive (primary-backup)
#   replication scheme. This process:
#     1. Receives propagated WRITE/DELETE updates from the primary
#     2. Applies them to its own local file store
#     3. Responds to clients directly after primary failure (promotion)
#     4. Responds to SYNC requests — lets the restarted primary pull its state
#     5. Responds to HEARTBEAT pings from the client
#
# TEXTBOOK GROUNDING — PASSIVE REPLICATION (§18.3.1, p.778–779):
#   "In the pure form of the model, front ends communicate only with the
#    primary replica manager to obtain the service. The primary replica manager
#    executes the operations and sends copies of the updated data to the backups.
#    If the primary fails, one of the backups is promoted to act as the primary."
#
#   This file IS that backup. In normal operation it only receives propagation
#   messages from the primary. When the client detects primary failure, it
#   switches to this server's address — the backup is now "promoted."
#
# TEXTBOOK GROUNDING — STATELESS SERVER (§12.2, p.532–533):
#   Same principle as the primary. The replica stores no per-client state.
#   It can crash and restart without needing session recovery.
#
# SOURCE REFERENCES (for README citation):
#   - Python socket module: https://docs.python.org/3/library/socket.html
#   - Python threading module: https://docs.python.org/3/library/threading.html
#   - Python os module: https://docs.python.org/3/library/os.html
#   - Python glob module: https://docs.python.org/3/library/glob.html
# =============================================================================

import socket
import threading
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.protocol import (
    send_message, recv_message,
    REPLICA_PORT,
    OP_READ, OP_WRITE, OP_DELETE, OP_LIST, OP_HEARTBEAT, OP_SYNC,
    STATUS_OK, STATUS_ERROR,
    SOCKET_TIMEOUT
)

# ---------------------------------------------------------------------------
# FILE STORE PATH — replica uses a SEPARATE directory from the primary.
# This simulates two different physical servers.
# ---------------------------------------------------------------------------
FILE_STORE_DIR = os.path.join(os.path.dirname(__file__), "file_store_replica")


# =============================================================================
# FILE STORE OPERATIONS (same interface as primary)
# Textbook ref: §12.2, Fig 12.6, p.532
# The replica must support the same operations as the primary so it can
# serve clients directly after promotion.
# =============================================================================

def fs_read(filename: str) -> tuple:
    path = _safe_path(filename)
    if path is None:
        return None, "Invalid filename"
    if not os.path.exists(path):
        return None, f"File not found: {filename}"
    with open(path, "r", encoding="utf-8") as f:
        return f.read(), None


def fs_write(filename: str, data: str) -> tuple:
    path = _safe_path(filename)
    if path is None:
        return False, "Invalid filename"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)
    return True, None


def fs_delete(filename: str) -> tuple:
    path = _safe_path(filename)
    if path is None:
        return False, "Invalid filename"
    if not os.path.exists(path):
        return False, f"File not found: {filename}"
    os.remove(path)
    return True, None


def fs_list() -> tuple:
    try:
        files = []
        if os.path.exists(FILE_STORE_DIR):
            for fname in os.listdir(FILE_STORE_DIR):
                path = os.path.join(FILE_STORE_DIR, fname)
                if os.path.isfile(path):
                    stats = os.stat(path)
                    files.append({
                        "name": fname,
                        "size": stats.st_size,
                        "modified": stats.st_mtime
                    })
        return files, None
    except Exception as e:
        return None, str(e)


def _safe_path(filename: str):
    safe_name = filename.lstrip("/").replace("..", "")
    full_path  = os.path.realpath(os.path.join(FILE_STORE_DIR, safe_name))
    if not full_path.startswith(os.path.realpath(FILE_STORE_DIR)):
        return None
    return full_path


def _get_all_files() -> dict:
    """
    Returns a dict of { relative_filename: content } for ALL files in the
    replica's file store. Used when the primary requests a sync after recovery.
    Source: https://docs.python.org/3/library/os.html#os.walk
    """
    result = {}
    for dirpath, _, filenames in os.walk(FILE_STORE_DIR):
        for fname in filenames:
            abs_path = os.path.join(dirpath, fname)
            rel_path = os.path.relpath(abs_path, FILE_STORE_DIR)
            try:
                with open(abs_path, "r", encoding="utf-8") as f:
                    result[rel_path] = f.read()
            except (OSError, UnicodeDecodeError):
                pass   # skip unreadable files
    return result


# =============================================================================
# CONNECTION HANDLER
# Textbook ref: §4.2.4, p.155 and §7.4, p.291 — one thread per connection.
#
# The replica handles THREE kinds of incoming connections:
#   1. PROPAGATION from primary (OP_WRITE, OP_DELETE)
#   2. CLIENT REQUESTS — after primary failure, clients connect here directly
#   3. SYNC REQUEST from a recovering primary (OP_SYNC)
# =============================================================================

def handle_connection(conn: socket.socket, addr):
    """
    Handles one incoming connection to the replica.
    The message's 'op' field determines what kind of connection this is.
    """
    print(f"[REPLICA] Connection from {addr}")
    conn.settimeout(SOCKET_TIMEOUT)

    try:
        while True:
            request = recv_message(conn)
            if request is None:
                break

            op       = request.get("op")
            filename = request.get("filename", "")
            data     = request.get("data", "")

            # ------------------------------------------------------------------
            # HEARTBEAT — client checking if this server is alive.
            # Textbook ref: §4.2.3, p.150 — timeout-based failure detection.
            # ------------------------------------------------------------------
            if op == OP_HEARTBEAT:
                send_message(conn, {"status": STATUS_OK, "op": OP_HEARTBEAT})
                continue

            # ------------------------------------------------------------------
            # SYNC REQUEST — primary just recovered and is asking for our state.
            # We return all files so the primary can rebuild its store.
            # Textbook ref: §18.3.1, p.779 — "the replica managers that survive
            # agree on which operations had been performed at the point when the
            # replacement primary takes over."
            # ------------------------------------------------------------------
            if op == OP_SYNC:
                all_files = _get_all_files()
                send_message(conn, {"status": STATUS_OK, "files": all_files})
                print(f"[REPLICA] Sent {len(all_files)} file(s) to recovering primary.")
                break   # sync is a one-shot exchange; close after

            # ------------------------------------------------------------------
            # PROPAGATED UPDATE from primary (Step 4 of the protocol).
            # The replica applies WRITE and DELETE to its own store.
            # Textbook ref: §18.3.1, p.779 — "the primary sends the updated
            # state... to all the backups. The backups send an acknowledgement."
            # ------------------------------------------------------------------
            if op == OP_WRITE:
                ok, err = fs_write(filename, data)
                if ok:
                    print(f"[REPLICA] Replicated WRITE '{filename}'")
                    send_message(conn, {"status": STATUS_OK})
                else:
                    print(f"[REPLICA] WRITE failed '{filename}': {err}")
                    send_message(conn, {"status": STATUS_ERROR, "message": err})

            elif op == OP_DELETE:
                ok, err = fs_delete(filename)
                if ok:
                    print(f"[REPLICA] Replicated DELETE '{filename}'")
                    send_message(conn, {"status": STATUS_OK})
                else:
                    # File not found on replica is not fatal —
                    # it may never have been propagated successfully before.
                    print(f"[REPLICA] DELETE '{filename}': {err}")
                    send_message(conn, {"status": STATUS_OK})  # ACK anyway

            # ------------------------------------------------------------------
            # CLIENT READ — replica serves reads after promotion.
            # Textbook ref: §18.3.1, p.779 — "If the primary fails, one of the
            # backups is promoted to act as the primary."
            # Once promoted, the replica handles ALL operations including reads.
            # ------------------------------------------------------------------
            elif op == OP_READ:
                file_data, err = fs_read(filename)
                if err:
                    send_message(conn, {"status": STATUS_ERROR, "message": err})
                else:
                    send_message(conn, {"status": STATUS_OK, "data": file_data})

            elif op == OP_LIST:
                files, err = fs_list()
                if err:
                    send_message(conn, {"status": STATUS_ERROR, "message": err})
                else:
                    send_message(conn, {"status": STATUS_OK, "data": files})

            else:
                send_message(conn, {
                    "status": STATUS_ERROR,
                    "message": f"Unknown op: {op}"
                })

    except (OSError, TimeoutError) as e:
        print(f"[REPLICA] Connection error with {addr}: {e}")
    finally:
        conn.close()
        print(f"[REPLICA] Closed connection from {addr}")


# =============================================================================
# MAIN SERVER LOOP
# Identical structure to primary_server.py.
# Textbook ref: §4.2.4, Fig 4.6, p.157 — server creates listening socket,
# calls accept() in a loop, spawns a thread per client.
# =============================================================================

def main():
    os.makedirs(FILE_STORE_DIR, exist_ok=True)

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", REPLICA_PORT))
    server_sock.listen(10)
    print(f"[REPLICA] Listening on port {REPLICA_PORT} ...")
    print(f"[REPLICA] File store: {FILE_STORE_DIR}")

    try:
        while True:
            conn, addr = server_sock.accept()
            t = threading.Thread(
                target=handle_connection,
                args=(conn, addr),
                daemon=True
            )
            t.start()
    except KeyboardInterrupt:
        print("\n[REPLICA] Shutting down.")
    finally:
        server_sock.close()


if __name__ == "__main__":
    main()