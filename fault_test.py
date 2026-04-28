# =============================================================================
# fault_test.py
# =============================================================================
#
# PURPOSE:
#   Automated test script that demonstrates the crash-and-recovery scenario
#   required by the assignment. Run this AFTER starting both servers.
#
# WHAT IT PROVES:
#   (a) Crash is detected       — client notices primary is down
#   (b) Service continues       — client transparently uses replica
#   (c) Recovery and re-sync    — primary restarts and pulls state from replica
#
# Textbook ref: §18.3.1, p.779 — "If the primary fails, one of the backups is
# promoted to act as the primary... the new primary takes over exactly where
# the last left off."
#
# HOW TO USE:
#   Terminal 1: python server/replica_server.py
#   Terminal 2: python server/primary_server.py
#   Terminal 3: python fault_test.py
#   (then kill Terminal 2 when the test tells you to, then restart it)
# =============================================================================

import sys
import os
import time
import socket

sys.path.insert(0, os.path.dirname(__file__))
from client.client import DFSClient
from shared.protocol import STATUS_OK


def separator(title):
    width = 60
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def check(condition, pass_msg, fail_msg):
    if condition:
        print(f"  [PASS] {pass_msg}")
    else:
        print(f"  [FAIL] {fail_msg}")
    return condition


def main():
    print("\n" + "#" * 60)
    print("  DISTRIBUTED FILE SYSTEM — FAULT TEST")
    print("  Coulouris et al., §18.3.1, p.778–779")
    print("#" * 60)

    client = DFSClient()
    all_passed = True

    # =========================================================================
    # PHASE 1: Normal operation — verify basic file operations
    # =========================================================================
    separator("PHASE 1: Normal Operation (Primary Active)")

    ok = client.write("test.txt", "Hello from Phase 1")
    all_passed &= check(ok, "WRITE test.txt succeeded", "WRITE test.txt failed")

    data = client.read("test.txt")
    all_passed &= check(
        data == "Hello from Phase 1",
        "READ test.txt returned correct content",
        f"READ returned wrong content: {data!r}"
    )

    ok = client.write("report.txt", "Annual report 2024")
    all_passed &= check(ok, "WRITE report.txt succeeded", "WRITE report.txt failed")

    print(f"\n  Both files written. Replica should have received propagated copies.")
    print(f"  (Check replica terminal for '[REPLICA] Replicated WRITE' messages)")

    # =========================================================================
    # PHASE 2: Simulate primary crash
    # =========================================================================
    separator("PHASE 2: Primary Crash Simulation")
    print("  ACTION REQUIRED: Kill the PRIMARY server now.")
    print("  (In Terminal 2, press Ctrl+C  — or run:  kill <pid of primary_server.py>)")
    print()
    input("  Press ENTER here once the primary is stopped...")

    # Give the heartbeat thread a moment to detect the failure
    print("  Waiting for crash detection (up to 7 seconds)...")
    time.sleep(7)

    # =========================================================================
    # PHASE 3: Verify service continues via replica
    # =========================================================================
    separator("PHASE 3: Continued Service via Replica")
    print("  Textbook ref: §18.3.1, p.779 — client retries on backup after primary fails.")

    data = client.read("test.txt")
    all_passed &= check(
        data == "Hello from Phase 1",
        "READ test.txt from REPLICA returned correct content",
        f"READ from replica failed or wrong: {data!r}"
    )

    ok = client.write("new_during_outage.txt", "Written while primary was down")
    all_passed &= check(
        ok,
        "WRITE during primary outage succeeded (via replica)",
        "WRITE during primary outage FAILED"
    )

    ok = client.delete("report.txt")
    all_passed &= check(ok, "DELETE report.txt via replica succeeded", "DELETE via replica failed")

    # =========================================================================
    # PHASE 4: Primary recovery and re-synchronisation
    # =========================================================================
    separator("PHASE 4: Primary Recovery & Re-Synchronisation")
    print("  Textbook ref: §18.3.1, p.779 — recovered primary re-syncs from backup.")
    print()
    print("  ACTION REQUIRED: Restart the PRIMARY server.")
    print("  (In Terminal 2: python server/primary_server.py)")
    print()
    input("  Press ENTER here once the primary has restarted and printed 'Listening'...")

    print("  Waiting for heartbeat to detect primary is back (up to 7 seconds)...")
    time.sleep(7)

    # =========================================================================
    # PHASE 5: Verify recovered primary has correct state
    # =========================================================================
    separator("PHASE 5: State Consistency After Recovery")
    print("  The primary should have synced from replica and have the latest state.")

    # test.txt should still be there
    data = client.read("test.txt")
    all_passed &= check(
        data == "Hello from Phase 1",
        "READ test.txt from recovered PRIMARY: correct",
        f"READ test.txt after recovery: wrong ({data!r})"
    )

    # new_during_outage.txt was written to replica while primary was down
    data = client.read("new_during_outage.txt")
    all_passed &= check(
        data == "Written while primary was down",
        "READ new_during_outage.txt from recovered PRIMARY: correct (sync worked!)",
        f"READ new_during_outage.txt after recovery: wrong ({data!r})"
    )

    # report.txt was deleted via replica; primary should NOT have it
    data = client.read("report.txt")
    all_passed &= check(
        data is None,
        "report.txt correctly absent from recovered PRIMARY (delete replicated)",
        "report.txt still exists on primary — sync did not replicate delete"
    )

    # =========================================================================
    # FINAL RESULT
    # =========================================================================
    separator("FAULT TEST RESULT")
    if all_passed:
        print("  ALL TESTS PASSED")
        print()
        print("  Demonstrated:")
        print("  (a) Crash detected via heartbeat timeout")
        print("  (b) Client continued service via replica (transparent failover)")
        print("  (c) Primary recovered and re-synchronised state from replica")
    else:
        print("  SOME TESTS FAILED — check server logs above for details.")

    client.close()
    print()


if __name__ == "__main__":
    main()