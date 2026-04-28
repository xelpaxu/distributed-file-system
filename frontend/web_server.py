import struct
import json
import socket
import threading
import os
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from client.client import DFSClient

# Initialize a global DFSClient instance
# This will be used by our web handlers.
dfs_client = DFSClient()

class APIHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == '/api/files':
            files = dfs_client.list_files()
            if files is not None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(files).encode('utf-8'))
            else:
                self.send_error(500, "Could not retrieve files from DFS")
            return
            
        elif path == '/api/download':
            fname = query.get('name', [None])[0]
            if not fname:
                self.send_error(400, "Missing 'name' query parameter")
                return
            data = dfs_client.read(fname)
            if data is not None:
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.end_headers()
                self.wfile.write(data.encode('utf-8'))
            else:
                self.send_error(404, "File not found or error reading")
            return

        # Serve static files for everything else
        super().do_GET()

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path == '/api/upload':
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            try:
                payload = json.loads(body.decode('utf-8'))
                fname = payload.get('name')
                content = payload.get('content')
                if not fname or content is None:
                    self.send_error(400, "Missing name or content")
                    return
                ok = dfs_client.write(fname, content)
                if ok:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": "success"}).encode('utf-8'))
                else:
                    self.send_error(500, "Failed to write file to DFS")
            except Exception as e:
                self.send_error(400, f"Invalid JSON payload: {e}")
            return

        self.send_error(404, "Endpoint not found")

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == '/api/files':
            fname = query.get('name', [None])[0]
            if not fname:
                self.send_error(400, "Missing 'name' query parameter")
                return
            ok = dfs_client.delete(fname)
            if ok:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "success"}).encode('utf-8'))
            else:
                self.send_error(500, "Failed to delete file from DFS")
            return

        self.send_error(404, "Endpoint not found")

if __name__ == '__main__':
    PORT = 8765
    server = ThreadingHTTPServer(('0.0.0.0', PORT), APIHandler)
    print(f"[WEB] Serving static frontend and REST API on port {PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[WEB] Shutting down")
        dfs_client.close()
        server.server_close()
