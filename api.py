# api_bridge.py  —  drop this in your dfs/ root and run it alongside your servers
# pip install flask flask-cors

from flask import Flask, request, jsonify
from flask_cors import CORS
import socket, struct, json, uuid

app = Flask(__name__)
CORS(app)  # allows the browser frontend to call this

PRIMARY_HOST, PRIMARY_PORT = "127.0.0.1", 9000
REPLICA_HOST,  REPLICA_PORT  = "127.0.0.1", 9001
TIMEOUT = 5

# ── low-level protocol (mirrors shared/protocol.py) ──────────────────────────

def send_msg(sock, data):
    payload = json.dumps(data).encode("utf-8")
    sock.sendall(struct.pack(">I", len(payload)) + payload)

def recv_msg(sock):
    def read_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf
    raw = read_exact(4)
    if raw is None:
        return None
    body = read_exact(struct.unpack(">I", raw)[0])
    return json.loads(body.decode("utf-8")) if body else None

def call_server(host, port, payload):
    with socket.create_connection((host, port), timeout=TIMEOUT) as s:
        send_msg(s, payload)
        return recv_msg(s)

# ── routes ────────────────────────────────────────────────────────────────────

@app.post("/write")
def write():
    body = request.json
    req = {"op": "WRITE", "filename": body["filename"],
           "data": body["data"], "request_id": str(uuid.uuid4())}
    try:
        resp = call_server(PRIMARY_HOST, PRIMARY_PORT, req)
        return jsonify(resp)
    except Exception as e:
        try:
            resp = call_server(REPLICA_HOST, REPLICA_PORT, req)
            return jsonify({**resp, "failover": True})
        except Exception as e2:
            return jsonify({"status": "ERROR", "message": str(e2)}), 503

@app.post("/read")
def read():
    body = request.json
    req = {"op": "READ", "filename": body["filename"],
           "data": "", "request_id": str(uuid.uuid4())}
    try:
        resp = call_server(PRIMARY_HOST, PRIMARY_PORT, req)
        return jsonify(resp)
    except Exception:
        try:
            resp = call_server(REPLICA_HOST, REPLICA_PORT, req)
            return jsonify({**resp, "failover": True})
        except Exception as e:
            return jsonify({"status": "ERROR", "message": str(e)}), 503

@app.post("/delete")
def delete():
    body = request.json
    req = {"op": "DELETE", "filename": body["filename"],
           "data": "", "request_id": str(uuid.uuid4())}
    try:
        resp = call_server(PRIMARY_HOST, PRIMARY_PORT, req)
        return jsonify(resp)
    except Exception:
        try:
            resp = call_server(REPLICA_HOST, REPLICA_PORT, req)
            return jsonify({**resp, "failover": True})
        except Exception as e:
            return jsonify({"status": "ERROR", "message": str(e)}), 503

@app.get("/list")
def list_files():
    req = {"op": "LIST", "filename": "", "data": "", "request_id": str(uuid.uuid4())}
    try:
        resp = call_server(PRIMARY_HOST, PRIMARY_PORT, req)
        return jsonify({**resp, "source": "primary"})
    except Exception:
        try:
            resp = call_server(REPLICA_HOST, REPLICA_PORT, req)
            return jsonify({**resp, "source": "replica", "failover": True})
        except Exception as e:
            return jsonify({"status": "ERROR", "message": str(e)}), 503

@app.get("/heartbeat")
def heartbeat():
    results = {}
    for name, host, port in [("primary", PRIMARY_HOST, PRIMARY_PORT),
                               ("replica", REPLICA_HOST, REPLICA_PORT)]:
        try:
            resp = call_server(host, port,
                               {"op": "HEARTBEAT", "request_id": ""})
            results[name] = "ok" if resp and resp.get("status") == "OK" else "error"
        except Exception:
            results[name] = "offline"
    return jsonify(results)

if __name__ == "__main__":
    print("Bridge running on http://127.0.0.1:5000")
    app.run(port=5000, debug=False)