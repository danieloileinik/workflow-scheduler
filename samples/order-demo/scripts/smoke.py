#!/usr/bin/env python3
"""Exercise the running sample through its public HTTP API and real workers."""

import argparse
import json
import time
import urllib.error
import urllib.request
import uuid


def request(base, path, body=None, expected=200):
    data = None if body is None else json.dumps(body).encode()
    call = urllib.request.Request(base + path, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(call, timeout=10) as response:
            assert response.status == expected, (path, response.status)
            value = response.read()
            return json.loads(value) if value else None
    except urllib.error.HTTPError as error:
        if error.code == expected:
            return None
        raise AssertionError(f"{path}: HTTP {error.code}: {error.read().decode()}") from error


def wait_for(check, description, timeout=150):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = check()
        if last:
            print(f"PASS {description}", flush=True)
            return last
        time.sleep(0.5)
    raise AssertionError(f"Timed out: {description}; last result: {last}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:5080")
    args = parser.parse_args()
    base = args.url.rstrip("/")

    def healthy():
        try:
            request(base, "/health")
            return True
        except (AssertionError, OSError):
            return False

    wait_for(healthy, "Orders API and scheduler are ready", timeout=90)

    def create(failure):
        body = {"orderId": str(uuid.uuid4()), "amount": 1490, "paymentFailure": failure}
        order = request(base, "/api/orders", body, expected=201)
        replay = request(base, "/api/orders", body, expected=201)
        assert order["processId"] == replay["processId"]
        request(base, "/api/orders", {**body, "amount": 1}, expected=409)
        return order["id"]

    normal = create("None")
    retry = create("Once")
    failed = create("Always")
    cancelled = create("None")
    print("PASS create replay keeps the process ID and rejects changed payloads", flush=True)

    def state(order_id, predicate):
        value = request(base, f"/api/orders/{order_id}")
        assert not value["warnings"], value["warnings"]
        return value if predicate(value) else None

    def awaiting_approval(value):
        return (value["process"]["status"] == "Waiting" and value["inventory"]["state"] == "Reserved"
                and value["payment"]["state"] == "Charged" and any(
            step["status"] == "Waiting" for step in value["process"]["stepHistory"]
        ))

    wait_for(lambda: state(normal, awaiting_approval), "Normal order waits for an external event")
    request(base, f"/api/orders/{normal}/approve", {}, expected=204)
    completed = wait_for(lambda: state(normal, lambda value: value["process"]["status"] == "Completed"
                         and value["inventory"]["state"] == "Shipped" and value["payment"]["state"] == "Charged"),
                         "Approval resumes the process and ships the order")
    assert completed["inventory"]["state"] == "Shipped"
    assert completed["payment"]["state"] == "Charged"

    wait_for(lambda: state(cancelled, awaiting_approval), "Paid order is ready for cancellation")
    request(base, f"/api/orders/{cancelled}/cancel", {}, expected=204)
    compensated = wait_for(lambda: state(cancelled, lambda value:
        value["process"]["status"] == "Cancelled" and value["inventory"]["state"] == "Released"
        and value["payment"]["state"] == "Refunded"), "Cancellation refunds payment and releases stock")
    refund = next(event for event in compensated["payment"]["operations"] if event["command"] == "RefundPayment")
    release = next(event for event in compensated["inventory"]["operations"] if event["command"] == "ReleaseStock")
    assert refund["recordedAt"] <= release["recordedAt"]

    recovered = wait_for(lambda: state(retry, awaiting_approval), "A delayed scheduler retry recovers the first payment failure")
    attempts = sorted(recovered["payment"]["operations"], key=lambda event: event["attempt"])
    assert [(event["attempt"], event["success"]) for event in attempts] == [(0, False), (1, True)]
    request(base, f"/api/orders/{retry}/approve", {}, expected=204)
    wait_for(lambda: state(retry, lambda value: value["process"]["status"] == "Completed"), "Recovered order completes")

    exhausted = wait_for(lambda: state(failed, lambda value:
        value["process"]["status"] == "Failed" and value["inventory"]["state"] == "Released"
        and len(value["payment"]["operations"]) == 4),
        "Exhausted payment retries fail the process and compensate the reservation")
    attempts = exhausted["payment"]["operations"]
    assert len(attempts) == 4 and all(not event["success"] for event in attempts)
    assert exhausted["payment"]["state"] == "NotCharged"
    print("All sample scenarios passed. No scheduler database shortcuts or accelerated retries were used.", flush=True)


if __name__ == "__main__":
    main()
