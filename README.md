# Distributed File System — README

## Project Overview

A simplified distributed file system implementing passive (primary-backup) replication
and fault tolerance, as described in:

> Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G.
> *Distributed Systems: Concepts and Design*, 5th Edition.
> Pearson Education, 2012.

---

## Architecture

```
          ┌─────────────────────────────────────┐
          │           CLIENT (Front End)        │
          │  client/client.py  — port any       │
          │  • sends READ / WRITE / DELETE      │
          │  • heartbeat thread (crash detect)  │
          │  • automatic failover to replica    │
          └────────────┬───────────────┬────────┘
                       │ TCP           │ TCP (on failover)
                       ▼               ▼
          ┌────────────────┐   ┌────────────────┐
          │ PRIMARY SERVER │   │ REPLICA SERVER │
          │ port 9000      │──►│ port 9001      │
          │                │   │                │
          │ file_store_    │   │ file_store_    │
          │  primary/      │   │  replica/      │
          └────────────────┘   └────────────────┘
               propagates WRITE/DELETE ──►
```

**Textbook reference:** Section 18.3.1, Figure 18.3, p.779

---

## File Structure

```
dfs/
├── shared/
│   ├── __init__.py
│   └── protocol.py          # marshalling, message format, constants
├── server/
│   ├── primary_server.py    # primary replica manager
│   ├── replica_server.py    # backup replica manager
│   ├── file_store_primary/  # created at runtime
│   └── file_store_replica/  # created at runtime
├── client/
│   └── client.py            # front end / interactive CLI
├── fault_test.py            # automated crash-recovery test
└── requirements.txt
```

---

## Setup

**Prerequisites:** Python 3.10 or higher. No third-party packages needed.

```bash
# Verify Python version
python --version
```

---

## Running the System

Open **three separate terminals** in the `dfs/` directory.

### Terminal 1 — Start the Replica (Backup) Server FIRST

```bash
python server/replica_server.py
```

Expected output:
```
[REPLICA] Listening on port 9001 ...
[REPLICA] File store: .../server/file_store_replica
```

### Terminal 2 — Start the Primary Server

```bash
python server/primary_server.py
```

Expected output:
```
[PRIMARY] Attempting to sync state from replica...
[PRIMARY] Replica not reachable — starting with existing local state.
[PRIMARY] Listening on port 9000 ...
[PRIMARY] File store: .../server/file_store_primary
```

### Terminal 3 — Start the Client

```bash
python client/client.py
```

Expected output:
```
[CLIENT] DFS client started. Connected to PRIMARY.

Commands:
  write <filename> <content>   — write content to a file
  read  <filename>             — read a file
  delete <filename>            — delete a file
  help                         — show this message
  quit                         — exit

dfs>
```

---

## Example Session

```
dfs> write hello.txt Hello world
  → OK

dfs> read hello.txt
  → 'Hello world'

dfs> write hello.txt Updated content
  → OK

dfs> read hello.txt
  → 'Updated content'

dfs> delete hello.txt
  → OK

dfs> read hello.txt
  → [CLIENT] READ error: File not found: hello.txt
```

---

## Crash & Recovery Demo (Manual)

This demonstrates the three required outcomes:
**(a) crash detected, (b) service continues, (c) server recovers and re-syncs.**

**Step 1** — Write a file from the client:
```
dfs> write demo.txt Crash recovery test
```

**Step 2** — Kill the primary (Terminal 2): press `Ctrl+C`

**Step 3** — Wait ~5 seconds, then try reading:
```
dfs> read demo.txt
[CLIENT] Heartbeat: PRIMARY not responding — failing over.
[CLIENT] *** PRIMARY FAILED — switching to REPLICA ***
  → 'Crash recovery test'
```

**Step 4** — Write more data while on replica:
```
dfs> write new.txt Written during outage
  → OK
```

**Step 5** — Restart primary (Terminal 2):
```bash
python server/primary_server.py
```
Primary will sync from replica:
```
[PRIMARY] Attempting to sync state from replica...
[PRIMARY] Synced 2 file(s) from replica.
[PRIMARY] Listening on port 9000 ...
```

**Step 6** — Wait ~5 seconds; client heartbeat detects primary is back:
```
[CLIENT] Primary is back online — switched back to PRIMARY.
```

**Step 7** — Verify new file exists on recovered primary:
```
dfs> read new.txt
  → 'Written during outage'
```

---

## Automated Fault Test

```bash
# With both servers running:
python fault_test.py
```

Follow the prompts. The script pauses and asks you to kill/restart the primary.

---

## Textbook Citations

| Section | Page | Concept Applied |
|---------|------|-----------------|
| §4.2.2  | p.149 | Socket abstraction — endpoint for IPC |
| §4.2.4  | p.154–157 | TCP stream communication, ServerSocket pattern |
| §4.3    | p.158 | Marshalling/unmarshalling — JSON over TCP |
| §4.3.3  | p.164 | JSON as lightweight external data representation |
| §7.4    | p.291 | Multithreaded server — one thread per client |
| §12.2   | p.530–531 | File service architecture (flat file + directory + client module) |
| §12.2   | p.532 | Read, Write, Delete interface (Figure 12.6) |
| §12.2   | p.532–533 | Stateless server and idempotent operations |
| §18.1   | p.766–767 | Replication motivation; availability formula 1 – p^n |
| §18.2   | p.769–770 | Replica managers and front end roles (Figure 18.1) |
| §18.3   | p.778 | Sequential consistency definition |
| §18.3.1 | p.778–779 | Passive (primary-backup) replication — 5-step protocol |
| §2.4    | p.68 | Crash failure model — process stops executing |
| §4.2.3  | p.150–151 | Socket timeouts for crash detection |

---

## Online Documentation Referenced

All code written by the group. The following Python standard library
documentation was consulted for syntax:

- Python `socket` module: https://docs.python.org/3/library/socket.html
- Python `threading` module: https://docs.python.org/3/library/threading.html
- Python `json` module: https://docs.python.org/3/library/json.html
- Python `struct` module: https://docs.python.org/3/library/struct.html
- Python `os` module: https://docs.python.org/3/library/os.html
- Python `uuid` module: https://docs.python.org/3/library/uuid.html

*(No AI-generated code was used in this implementation.)*
