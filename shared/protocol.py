# =============================================================================
# shared/protocol.py
# =============================================================================
#
# PURPOSE:
#   Defines the message format (marshalling) and shared constants used by
#   ALL components: primary server, replica server, and client.
#
# TEXTBOOK GROUNDING:
#   Section 4.3, p.158 — "Marshalling is the process of taking a collection
#   of data items and assembling them into a form suitable for transmission
#   in a message."
#
#   Section 4.3.3, p.164 — The book discusses JSON as a lightweight external
#   data representation: "There is also considerable interest in JSON
#   (JavaScript Object Notation) as an approach to external data
#   representation... a step towards more lightweight approaches."
#
#   We use Python's built-in `json` module for marshalling/unmarshalling.
#   The book's Java examples use DataOutputStream.writeUTF() (Fig 4.5, p.156);
#   our equivalent is json.dumps() encoded to UTF-8 bytes over a TCP stream.
#
# SOURCE REFERENCES (for README citation):
#   - Python json module: https://docs.python.org/3/library/json.html
#   - Python socket module: https://docs.python.org/3/library/socket.html
#   - struct module for length-prefix framing:
#     https://docs.python.org/3/library/struct.html
# =============================================================================

import json
import struct

# ---------------------------------------------------------------------------
# PORT CONSTANTS
# Textbook ref: §4.2.1, p.148 — "A local port is a message destination within
# a computer, specified as an integer. Servers generally publicize their port
# numbers for use by clients."
# ---------------------------------------------------------------------------
PRIMARY_HOST   = "127.0.0.1"
PRIMARY_PORT   = 9000          # Port the primary server listens on

REPLICA_HOST   = "127.0.0.1"
REPLICA_PORT   = 9001          # Port the replica (backup) server listens on

HEARTBEAT_INTERVAL = 2         # seconds between heartbeat pings
SOCKET_TIMEOUT     = 5         # seconds before a recv is considered failed

# ---------------------------------------------------------------------------
# OPERATION CODES
# Textbook ref: §12.2, Fig 12.6, p.532 — The flat file service defines these
# operations: Read, Write, Delete (and Create, GetAttributes, SetAttributes).
# We implement the three core ones required by the assignment.
# ---------------------------------------------------------------------------
OP_READ      = "READ"
OP_WRITE     = "WRITE"
OP_DELETE    = "DELETE"
OP_LIST      = "LIST"
OP_HEARTBEAT = "HEARTBEAT"   # Our crash-detection ping (not in textbook interface,
                              # but required for fault tolerance — §18.3.1, p.779)
OP_SYNC      = "SYNC"        # Used by primary to push state to replica after recovery

# ---------------------------------------------------------------------------
# STATUS CODES
# ---------------------------------------------------------------------------
STATUS_OK    = "OK"
STATUS_ERROR = "ERROR"

# ---------------------------------------------------------------------------
# FRAMING: Length-Prefix Protocol
#
# PROBLEM: TCP is a stream protocol (§4.2.4, p.153). When we send two JSON
# messages back-to-back, the receiver cannot tell where one ends and the next
# begins — TCP may deliver them as one big chunk or split them.
#
# SOLUTION: Before each JSON message, we send a 4-byte integer (big-endian)
# that tells the receiver exactly how many bytes to read for that message.
# This is called "length-prefix framing."
#
# Source: https://docs.python.org/3/library/struct.html
# struct.pack(">I", n) packs n as a 4-byte unsigned int, big-endian.
# ---------------------------------------------------------------------------

def send_message(sock, data: dict):
    """
    Marshals a dict to JSON, frames it with a 4-byte length prefix,
    and sends it over the TCP socket.

    Textbook ref: §4.3, p.158 — marshalling = assembling data into a form
    suitable for transmission in a message.
    """
    payload = json.dumps(data).encode("utf-8")   # marshal dict → bytes
    length  = struct.pack(">I", len(payload))     # 4-byte big-endian length
    sock.sendall(length + payload)               # send length header + body


def recv_message(sock) -> dict:
    """
    Reads a length-prefixed message from the socket and unmarshals it back
    to a dict.

    Textbook ref: §4.3, p.158 — "Unmarshalling is the process of disassembling
    them on arrival to produce an equivalent collection of data items."
    """
    # Step 1: Read exactly 4 bytes for the length header
    raw_len = _recv_exact(sock, 4)
    if raw_len is None:
        return None
    msg_len = struct.unpack(">I", raw_len)[0]    # decode length

    # Step 2: Read exactly msg_len bytes for the body
    raw_body = _recv_exact(sock, msg_len)
    if raw_body is None:
        return None

    return json.loads(raw_body.decode("utf-8"))  # unmarshal bytes → dict


def _recv_exact(sock, n: int) -> bytes:
    """
    Reads exactly n bytes from the socket. TCP may deliver data in fragments
    (§4.2.4, p.153 — "The application can choose how much data it writes to
    a stream or reads from it"), so we loop until we have all n bytes.
    """
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:          # connection closed by remote side
            return None
        buf += chunk
    return buf