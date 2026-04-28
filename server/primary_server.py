# =============================================================================
# server/primary_server.py
# =============================================================================
#
# PURPOSE:
#   The PRIMARY REPLICA MANAGER in our passive (primary-backup) replication
#   scheme. This process:
#     1. Accepts TCP connections from clients
#     2. Executes READ / WRITE / DELETE on the local file store
#     3. Propagates every WRITE and DELETE to the replica server (backup)
#     4. Responds to heartbeat pings from the client
#     5. Accepts SYNC requests from the replica after recovery
#
# TEXTBOOK GROUNDING — PASSIVE REPLICATION (§18.3.1, p.778–779):
#   "In the passive or primary-backup model of replication for fault tolerance,
#    there is at any one time a single primary replica manager and one or more
#    secondary replica managers — 'backups' or 'slaves'. Front ends communicate
#    only with the primary replica manager to obtain the service. The primary
#    executes the operations and sends copies of the updated data to the backups."
#
#   The 5-step protocol (p.779) that this server implements:
#     Step 1 — REQUEST:     client sends {op, filename, data, request_id}
#     Step 2 — COORDINATION: primary checks request_id for duplicate detection
#     Step 3 — EXECUTION:   primary runs the op on its local file store
#     Step 4 — AGREEMENT:   primary forwards update to replica; waits for ACK
#     Step 5 — RESPONSE:    primary sends {status, data} back to client
#
# TEXTBOOK GROUNDING — THREADING (§7.4, p.291 and §4.2.4, p.155):
#   "When a server accepts a connection, it generally creates a new thread in
#    which to communicate with the new client. The advantage of using a separate
#    thread for each client is that the server can block when waiting for input
#    without delaying other clients."
#   Our server spawns a thread per client connection, matching this model.
#
# TEXTBOOK GROUNDING — STATELESS SERVER (§12.2, p.532–533):
#   "Stateless servers can be restarted after a failure and resume operation
#    without any need for clients or the server to restore any state."
#   The server stores NO per-client session state. Every request carries
#   everything needed to service it (filename, data, request_id).
#
# TEXTBOOK GROUNDING — IDEMPOTENT OPERATIONS (§12.2, p.532):
#   "With the exception of Create, the operations are idempotent, allowing the
#    use of at-least-once RPC semantics – clients may repeat calls to which they
#    receive no reply."
#   We cache recent responses keyed by request_id so that if a client retries
#   a request (because the network dropped the reply), we return the same
#   response without re-executing the operation.
#
# SOURCE REFERENCES (for README citation):
#   - Python socket module: https://docs.python.org/3/library/socket.html
#   - Python threading module: https://docs.python.org/3/library/threading.html
#   - Python os module (file I/O): https://docs.python.org/3/library/os.html
# =============================================================================

import socket
import threading
import os
import sys
import time

# Allow imports from the project root (so `shared.protocol` works)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.protocol import (
    send_message, recv_message,
    PRIMARY_PORT, REPLICA_HOST, REPLICA_PORT,
    OP_READ, OP_WRITE, OP_DELETE, OP_LIST, OP_HEARTBEAT, OP_SYNC,
    STATUS_OK, STATUS_ERROR,
    SOCKET_TIMEOUT
)

# ---------------------------------------------------------------------------
# FILE STORE PATH
# Textbook ref: §12.2, p.530 — the "flat file service" stores file data
# identified by UFIDs. We simplify: filenames ARE the identifiers (like
# a flat namespace), stored as real files in FILE_STORE_DIR.
# ---------------------------------------------------------------------------
FILE_STORE_DIR = os.path.join(os.path.dirname(__file__), "file_store_primary")

# ---------------------------------------------------------------------------
# DUPLICATE REQUEST CACHE
# Textbook ref: §18.3.1, p.779, Step 2 — "The primary takes each request
# atomically, in the order in which it receives it. It checks the unique
# identifier, in case it has already executed the request, and if so it simply
# resends the response."
# ---------------------------------------------------------------------------
seen_requests = {}          # { request_id: response_dict }
seen_requests_lock = threading.Lock()

# Maximum cache size to prevent unbounded memory growth
MAX_CACHE = 500


# =============================================================================
# FILE STORE OPERATIONS
# Textbook ref: §12.2, Fig 12.6, p.532 — flat file service operations.
# We implement Read, Write, Delete as described there.
# =============================================================================

def fs_read(filename: str) -> tuple:
    """
    Read(FileId, i, n) — §12.2, p.532
    Returns (data_string, None) on success or (None, error_message) on failure.
    """
    path = _safe_path(filename)
    if path is None:
        return None, "Invalid filename"
    if not os.path.exists(path):
        return None, f"File not found: {filename}"
    with open(path, "r", encoding="utf-8") as f:
        return f.read(), None


def fs_write(filename: str, data: str) -> tuple:
    """
    Write(FileId, i, Data) — §12.2, p.532
    Creates or overwrites the file. Returns (True, None) or (False, error).
    """
    path = _safe_path(filename)
    if path is None:
        return False, "Invalid filename"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    print("[DEBUG WRITE] Writing file to:", path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)
    return True, None


def fs_delete(filename: str) -> tuple:
    """
    Delete(FileId) — §12.2, p.532
    Returns (True, None) or (False, error).
    """
    path = _safe_path(filename)
    if path is None:
        return False, "Invalid filename"
    if not os.path.exists(path):
        return False, f"File not found: {filename}"
    os.remove(path)
    return True, None


def fs_list():
    try:
        print("\n[DEBUG] ===== LIST CALLED =====")
        print("[DEBUG] DIR:", FILE_STORE_DIR)

        if not os.path.exists(FILE_STORE_DIR):
            print("[DEBUG] Directory does NOT exist")
            return [], None

        raw_files = os.listdir(FILE_STORE_DIR)
        print("[DEBUG] RAW FILES:", raw_files)

        files = []
        for fname in raw_files:
            path = os.path.join(FILE_STORE_DIR, fname)

            if os.path.isfile(path):
                stats = os.stat(path)
                files.append({
                    "name": fname,
                    "size": stats.st_size,
                    "modified": stats.st_mtime
                })

        print("[DEBUG] FINAL FILE LIST:", files)
        return files, None

    except Exception as e:
        print("[DEBUG ERROR]:", e)
        return None, str(e)


def _safe_path(filename: str):
    """
    Resolves the filename to an absolute path under FILE_STORE_DIR.
    Rejects any path that tries to escape the store via '../'.
    Security principle: never trust client-supplied file paths blindly.
    Source: https://docs.python.org/3/library/os.path.html#os.path.realpath
    """
    # Strip leading slashes so os.path.join works correctly
    safe_name = filename.lstrip("/").replace("..", "")
    full_path  = os.path.realpath(os.path.join(FILE_STORE_DIR, safe_name))
    if not full_path.startswith(os.path.realpath(FILE_STORE_DIR)):
        return None   # path traversal attempt — reject
    return full_path


# =============================================================================
# REPLICA PROPAGATION
# Textbook ref: §18.3.1, p.779, Step 4 — "If the request is an update, then
# the primary sends the updated state, the response and the unique identifier
# to all the backups. The backups send an acknowledgement."
#
# We propagate WRITE and DELETE (the two mutating operations) to the replica.
# READ is not propagated because it does not change state.
# =============================================================================

def propagate_to_replica(message: dict) -> bool:
    """
    Opens a short-lived TCP connection to the replica and sends an update.
    Returns True if the replica acknowledged, False otherwise.

    Textbook ref: §4.2.4, p.154 — each propagation opens a fresh TCP
    connection. This keeps the primary stateless with respect to the replica
    connection.
    """
    try:
        with socket.create_connection(
            (REPLICA_HOST, REPLICA_PORT), timeout=SOCKET_TIMEOUT
        ) as s:
            send_message(s, message)
            ack = recv_message(s)
            return ack is not None and ack.get("status") == STATUS_OK
    except (ConnectionRefusedError, OSError, TimeoutError):
        # Replica is down — log it but do NOT block the client response.
        # Textbook ref: §18.3.1, p.779 — the primary still responds to the
        # client; availability is preserved even if a backup is temporarily
        # unreachable.
        print("[PRIMARY] WARNING: Could not reach replica for propagation.")
        return False


# =============================================================================
# CLIENT REQUEST HANDLER
# Each connected client is served in its own thread.
# Textbook ref: §4.2.4, p.155 — "When a server accepts a connection, it
# generally creates a new thread in which to communicate with the new client."
# =============================================================================

def handle_client(conn: socket.socket, addr):
    """
    Handles the full lifecycle of one client connection.
    Implements the 5-step passive replication protocol (§18.3.1, p.779).
    """
    print(f"[PRIMARY] Connection from {addr}")
    conn.settimeout(SOCKET_TIMEOUT)

    try:
        while True:
            # ------------------------------------------------------------------
            # STEP 1 — REQUEST  (§18.3.1, p.779)
            # Receive the request from the client (our "front end").
            # ------------------------------------------------------------------
            request = recv_message(conn)
            if request is None:
                break    # client closed the connection

            op          = request.get("op")
            filename    = request.get("filename", "")
            data        = request.get("data", "")
            request_id  = request.get("request_id", "")

            print(f"[PRIMARY] {op} '{filename}' (req_id={request_id[:8]}...)")

            # ------------------------------------------------------------------
            # STEP 2 — COORDINATION: duplicate detection (§18.3.1, p.779)
            # "It checks the unique identifier, in case it has already executed
            #  the request, and if so it simply resends the response."
            # ------------------------------------------------------------------
            if op == OP_HEARTBEAT:
                # Heartbeat is not a file operation — just reply immediately.
                send_message(conn, {"status": STATUS_OK, "op": OP_HEARTBEAT})
                continue

            with seen_requests_lock:
                if request_id and request_id in seen_requests:
                    # Duplicate! Resend cached response without re-executing.
                    send_message(conn, seen_requests[request_id])
                    print(f"[PRIMARY] Duplicate request {request_id[:8]}, resent cached response.")
                    continue

            # ------------------------------------------------------------------
            # STEP 3 — EXECUTION (§18.3.1, p.779)
            # The primary executes the request on its local file store.
            # Textbook ref: §12.2, Fig 12.6, p.532 — READ, WRITE, DELETE ops.
            # ------------------------------------------------------------------
            response = {}

            if op == OP_READ:
                file_data, err = fs_read(filename)
                if err:
                    response = {"status": STATUS_ERROR, "message": err}
                else:
                    response = {"status": STATUS_OK, "data": file_data}

            elif op == OP_WRITE:
                ok, err = fs_write(filename, data)
                if err:
                    response = {"status": STATUS_ERROR, "message": err}
                else:
                    response = {"status": STATUS_OK, "message": f"Written: {filename}"}

            elif op == OP_DELETE:
                ok, err = fs_delete(filename)
                if err:
                    response = {"status": STATUS_ERROR, "message": err}
                else:
                    response = {"status": STATUS_OK, "message": f"Deleted: {filename}"}

            elif op == OP_LIST:
                files, err = fs_list()
                if err:
                    response = {"status": STATUS_ERROR, "message": err}
                else:
                    response = {"status": STATUS_OK, "data": files}

            else:
                response = {"status": STATUS_ERROR, "message": f"Unknown op: {op}"}

            # ------------------------------------------------------------------
            # STEP 4 — AGREEMENT: propagate mutations to replica (§18.3.1, p.779)
            # "If the request is an update, then the primary sends the updated
            #  state, the response and the unique identifier to all the backups."
            # We only propagate WRITE and DELETE (state-changing operations).
            # READ does not change state so no propagation is needed.
            # ------------------------------------------------------------------
            if op in (OP_WRITE, OP_DELETE) and response["status"] == STATUS_OK:
                propagate_to_replica({
                    "op":         op,
                    "filename":   filename,
                    "data":       data,
                    "request_id": request_id
                })
                # Note: even if propagation fails, we still respond to the client.
                # This is the availability trade-off in passive replication —
                # the primary stays responsive even if the backup is unreachable.

            # ------------------------------------------------------------------
            # STEP 5 — RESPONSE (§18.3.1, p.779)
            # Primary responds to the client.
            # Also cache the response for duplicate detection.
            # ------------------------------------------------------------------
            with seen_requests_lock:
                if request_id:
                    seen_requests[request_id] = response
                    # Evict oldest entries if cache is full
                    if len(seen_requests) > MAX_CACHE:
                        oldest = next(iter(seen_requests))
                        del seen_requests[oldest]

            send_message(conn, response)

    except (OSError, TimeoutError) as e:
        print(f"[PRIMARY] Connection error with {addr}: {e}")
    finally:
        conn.close()
        print(f"[PRIMARY] Closed connection from {addr}")


# =============================================================================
# SYNC HANDLER
# When the primary restarts after a crash, the replica may have newer state.
# The primary requests a full state sync from the replica.
#
# Textbook ref: §18.3.1, p.779 — "If the primary fails, one of the backups is
# promoted to act as the primary... the new primary takes over where the last
# left off."
# Our simplified model: on restart, primary fetches all file names+contents
# from the replica and overwrites its local file store.
# =============================================================================

def sync_from_replica():
    """
    On startup, ask the replica for its full file listing and pull all files.
    This re-synchronises the primary after a crash.
    """
    print("[PRIMARY] Attempting to sync state from replica...")
    try:
        with socket.create_connection(
            (REPLICA_HOST, REPLICA_PORT), timeout=SOCKET_TIMEOUT
        ) as s:
            send_message(s, {"op": OP_SYNC, "role": "primary_requesting_sync"})
            response = recv_message(s)
            if response and response.get("status") == STATUS_OK:
                files = response.get("files", {})
                os.makedirs(FILE_STORE_DIR, exist_ok=True)
                for fname, content in files.items():
                    fs_write(fname, content)
                print(f"[PRIMARY] Synced {len(files)} file(s) from replica.")
            else:
                print("[PRIMARY] Replica unavailable for sync — starting fresh.")
    except (ConnectionRefusedError, OSError):
        print("[PRIMARY] Replica not reachable — starting with existing local state.")


# =============================================================================
# MAIN SERVER LOOP
# Textbook ref: §4.2.4, p.155–157 and Fig 4.6 — the server creates a
# ServerSocket, calls accept() in a loop, and spawns a thread per connection.
# Python equivalent: socket.socket() → bind() → listen() → accept() loop.
# =============================================================================

def main():
    os.makedirs(FILE_STORE_DIR, exist_ok=True)

    # On startup, try to sync from replica (handles crash recovery).
    sync_from_replica()

    # Create the listening TCP socket.
    # Textbook ref: §4.2.2, p.149 — "For a process to receive messages, its
    # socket must be bound to a local port and one of the Internet addresses."
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR lets us restart immediately without "Address already in use".
    # Source: https://docs.python.org/3/library/socket.html#socket.SO_REUSEADDR
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server_sock.bind(("0.0.0.0", PRIMARY_PORT))
    server_sock.listen(10)    # backlog: up to 10 pending connections queued
    print(f"[PRIMARY] Listening on port {PRIMARY_PORT} ...")
    print(f"[PRIMARY] File store: {FILE_STORE_DIR}")

    try:
        while True:
            # accept() blocks until a client connects.
            # Textbook ref: §4.2.4, p.155 — "the server role involves creating
            # a listening socket bound to a server port and waiting for clients
            # to request connections."
            conn, addr = server_sock.accept()

            # Spawn a new thread for this client.
            # Textbook ref: §7.4, p.291 — multithreaded server handles many
            # requests at once.
            t = threading.Thread(
                target=handle_client,
                args=(conn, addr),
                daemon=True    # thread exits when main process exits
            )
            t.start()

    except KeyboardInterrupt:
        print("\n[PRIMARY] Shutting down.")
    finally:
        server_sock.close()


if __name__ == "__main__":
    main()