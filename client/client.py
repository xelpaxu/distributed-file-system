# =============================================================================
# client/client.py
# =============================================================================
#
# PURPOSE:
#   The CLIENT MODULE / FRONT END that users interact with.
#   Implements READ, WRITE, DELETE against the distributed file system.
#   Handles crash detection, automatic failover to the replica, and retry logic.
#
# TEXTBOOK GROUNDING — FRONT END (§18.2, p.770):
#   "Each client's requests are first handled by a component called a front end.
#    The role of the front end is to communicate by message passing with one or
#    more of the replica managers, rather than forcing the client to do this
#    itself explicitly. It is the vehicle for making replication transparent."
#   This file IS the front end. The user calls read/write/delete and never
#   needs to know that two servers exist.
#
# TEXTBOOK GROUNDING — CRASH DETECTION via TIMEOUT (§4.2.3, p.150–151):
#   "In some programs, it is not appropriate that a process that has invoked a
#    receive operation should wait indefinitely in situations where the sending
#    process may have crashed or the expected message may have been lost.
#    To allow for such requirements, timeouts can be set on sockets."
#   We set a socket timeout. If no reply arrives in time, we assume the
#   primary has crashed and switch to the replica.
#
# TEXTBOOK GROUNDING — FAILOVER (§18.3.1, p.779):
#   "If the primary fails, one of the backups is promoted to act as the primary."
#   Our client implements this by catching connection errors and retrying
#   against the replica's address.
#
# TEXTBOOK GROUNDING — IDEMPOTENT RETRY (§12.2, p.532):
#   "The operations are idempotent, allowing the use of at-least-once RPC
#    semantics – clients may repeat calls to which they receive no reply."
#   We attach a UUID request_id to every request. If we retry on the new
#   server, and the old one actually processed it, the request_id lets the
#   server deduplicate (Step 2, §18.3.1).
#
# SOURCE REFERENCES (for README citation):
#   - Python socket module: https://docs.python.org/3/library/socket.html
#   - Python uuid module:   https://docs.python.org/3/library/uuid.html
#   - Python threading module: https://docs.python.org/3/library/threading.html
# =============================================================================

import socket
import threading
import time
import uuid
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.protocol import (
    send_message, recv_message,
    PRIMARY_HOST, PRIMARY_PORT,
    REPLICA_HOST, REPLICA_PORT,
    OP_READ, OP_WRITE, OP_DELETE, OP_LIST, OP_HEARTBEAT,
    STATUS_OK, STATUS_ERROR,
    SOCKET_TIMEOUT, HEARTBEAT_INTERVAL
)


# =============================================================================
# DFSClient — the main class users instantiate
# =============================================================================

class DFSClient:
    """
    Distributed File System Client (Front End).

    Usage:
        client = DFSClient()
        client.write("notes.txt", "Hello world")
        data = client.read("notes.txt")
        client.delete("notes.txt")
        client.close()
    """

    def __init__(self):
        # Current active server — starts as primary.
        # Textbook ref: §18.3.1, p.779 — front end communicates with primary.
        self._server_host = PRIMARY_HOST
        self._server_port = PRIMARY_PORT
        self._using_primary = True

        # Lock protects _server_host/_server_port during failover.
        # Source: https://docs.python.org/3/library/threading.html#threading.Lock
        self._server_lock = threading.Lock()

        # Start the heartbeat thread (background crash detector).
        self._running = True
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True
        )
        self._hb_thread.start()
        print("[CLIENT] DFS client started. Connected to PRIMARY.")

    # -------------------------------------------------------------------------
    # PUBLIC API — READ / WRITE / DELETE
    # Textbook ref: §12.2, Fig 12.6, p.532 — flat file service interface.
    # -------------------------------------------------------------------------

    def read(self, filename: str) -> str | None:
        """
        READ — fetches the content of a file from the DFS.
        Returns file content as string, or None on error.
        """
        response = self._send_request({
            "op":          OP_READ,
            "filename":    filename,
            "data":        "",
            "request_id":  str(uuid.uuid4())
        })
        if response and response.get("status") == STATUS_OK:
            return response.get("data")
        else:
            print(f"[CLIENT] READ error: {response.get('message') if response else 'No response'}")
            return None

    def write(self, filename: str, data: str) -> bool:
        """
        WRITE — creates or overwrites a file in the DFS.
        Returns True on success, False on failure.
        """
        response = self._send_request({
            "op":          OP_WRITE,
            "filename":    filename,
            "data":        data,
            "request_id":  str(uuid.uuid4())
        })
        if response and response.get("status") == STATUS_OK:
            return True
        else:
            print(f"[CLIENT] WRITE error: {response.get('message') if response else 'No response'}")
            return False

    def delete(self, filename: str) -> bool:
        """
        DELETE — removes a file from the DFS.
        Returns True on success, False on failure.
        """
        response = self._send_request({
            "op":          OP_DELETE,
            "filename":    filename,
            "data":        "",
            "request_id":  str(uuid.uuid4())
        })
        if response and response.get("status") == STATUS_OK:
            return True
        else:
            print(f"[CLIENT] DELETE error: {response.get('message') if response else 'No response'}")
            return False

    def list_files(self) -> list | None:
        """
        LIST - returns a list of files in the DFS.
        """
        response = self._send_request({
            "op":          OP_LIST,
            "filename":    "",
            "data":        "",
            "request_id":  str(uuid.uuid4())
        })
        if response and response.get("status") == STATUS_OK:
            return response.get("data")
        else:
            print(f"[CLIENT] LIST error: {response.get('message') if response else 'No response'}")
            return None

    def close(self):
        """Stop the heartbeat thread gracefully."""
        self._running = False

    # -------------------------------------------------------------------------
    # CORE SEND / RECEIVE LOGIC with FAILOVER
    # -------------------------------------------------------------------------

    def _send_request(self, request: dict, retried: bool = False) -> dict | None:
        """
        Opens a TCP connection to the current active server, sends the request,
        waits for a response, and returns it.

        If the connection fails (primary crashed), triggers failover to replica
        and retries the request once.

        Textbook ref: §4.2.4, p.154 — "Establishing a connection involves a
        connect request from client to server followed by an accept request from
        server to client before any communication can take place."
        """
        with self._server_lock:
            host = self._server_host
            port = self._server_port

        try:
            # Open a fresh TCP connection per request.
            # This keeps the client simple and matches the stateless server model
            # (§12.2, p.533 — no persistent per-client state at the server).
            with socket.create_connection((host, port), timeout=SOCKET_TIMEOUT) as s:
                send_message(s, request)
                response = recv_message(s)
                return response

        except (ConnectionRefusedError, OSError, TimeoutError) as e:
            print(f"[CLIENT] Could not reach server at {host}:{port} — {e}")

            if not retried:
                # Attempt failover once.
                self._failover()
                return self._send_request(request, retried=True)
            else:
                print("[CLIENT] Both servers unreachable. Request failed.")
                return None

    def _failover(self):
        """
        Switches the active server from primary → replica (or back).

        Textbook ref: §18.3.1, p.779 — "If the primary fails, one of the
        backups is promoted to act as the primary."
        Our implementation: the client transparently redirects to the replica.
        """
        with self._server_lock:
            if self._using_primary:
                self._server_host = REPLICA_HOST
                self._server_port = REPLICA_PORT
                self._using_primary = False
                print("[CLIENT] *** PRIMARY FAILED — switching to REPLICA ***")
            else:
                # Already on replica; try switching back to primary.
                self._server_host = PRIMARY_HOST
                self._server_port = PRIMARY_PORT
                self._using_primary = True
                print("[CLIENT] *** Switching back to PRIMARY ***")

    # -------------------------------------------------------------------------
    # HEARTBEAT LOOP — Crash Detection
    #
    # Textbook ref: §4.2.3, p.150–151 — "timeouts can be set on sockets.
    # Choosing an appropriate timeout interval is difficult, but it should be
    # fairly large in comparison with the time required to transmit a message."
    #
    # We send a lightweight HEARTBEAT message every HEARTBEAT_INTERVAL seconds.
    # If the server does not reply within SOCKET_TIMEOUT seconds, we treat it
    # as a crash and trigger failover proactively.
    #
    # This is PROACTIVE crash detection — the client notices the crash even if
    # no regular request is in flight at that moment.
    # -------------------------------------------------------------------------

    def _heartbeat_loop(self):
        """
        Background thread: periodically pings the active server.
        On timeout/error → failover.
        """
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self._running:
                break

            with self._server_lock:
                host = self._server_host
                port = self._server_port
                is_primary = self._using_primary

            try:
                with socket.create_connection((host, port), timeout=SOCKET_TIMEOUT) as s:
                    send_message(s, {"op": OP_HEARTBEAT, "request_id": ""})
                    reply = recv_message(s)
                    if reply and reply.get("status") == STATUS_OK:
                        # Server alive — if we were on replica, check if
                        # primary has come back.
                        if not is_primary:
                            self._try_return_to_primary()

            except (ConnectionRefusedError, OSError, TimeoutError):
                srv_name = "PRIMARY" if is_primary else "REPLICA"
                print(f"[CLIENT] Heartbeat: {srv_name} not responding — failing over.")
                self._failover()

    def _try_return_to_primary(self):
        """
        If we're currently using the replica, check if the primary is back up.
        If it is, switch back.

        Textbook ref: §18.3.1, p.779 — after the primary recovers and
        re-synchronises, it can resume its role.
        """
        try:
            with socket.create_connection(
                (PRIMARY_HOST, PRIMARY_PORT), timeout=SOCKET_TIMEOUT
            ) as s:
                send_message(s, {"op": OP_HEARTBEAT, "request_id": ""})
                reply = recv_message(s)
                if reply and reply.get("status") == STATUS_OK:
                    with self._server_lock:
                        self._server_host = PRIMARY_HOST
                        self._server_port = PRIMARY_PORT
                        self._using_primary = True
                    print("[CLIENT] Primary is back online — switched back to PRIMARY.")
        except (ConnectionRefusedError, OSError, TimeoutError):
            pass   # Primary still down, stay on replica


# =============================================================================
# INTERACTIVE COMMAND-LINE INTERFACE
# =============================================================================

def print_help():
    print("""
Commands:
  write <filename> <content>   — write content to a file
  read  <filename>             — read a file
  delete <filename>            — delete a file
  help                         — show this message
  quit                         — exit
""")


def main():
    client = DFSClient()
    print_help()

    try:
        while True:
            try:
                raw = input("dfs> ").strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not raw:
                continue

            parts = raw.split(maxsplit=2)
            cmd   = parts[0].lower()

            if cmd == "quit":
                break

            elif cmd == "help":
                print_help()

            elif cmd == "write":
                if len(parts) < 3:
                    print("Usage: write <filename> <content>")
                    continue
                filename, content = parts[1], parts[2]
                ok = client.write(filename, content)
                print(f"  → {'OK' if ok else 'FAILED'}")

            elif cmd == "read":
                if len(parts) < 2:
                    print("Usage: read <filename>")
                    continue
                filename = parts[1]
                data = client.read(filename)
                if data is not None:
                    print(f"  → {repr(data)}")

            elif cmd == "delete":
                if len(parts) < 2:
                    print("Usage: delete <filename>")
                    continue
                filename = parts[1]
                ok = client.delete(filename)
                print(f"  → {'OK' if ok else 'FAILED'}")

            else:
                print(f"Unknown command: {cmd}. Type 'help' for usage.")

    finally:
        client.close()
        print("[CLIENT] Goodbye.")


if __name__ == "__main__":
    main()