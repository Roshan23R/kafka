"""
Pattern 8: Change Data Capture
----------------------------------
Registers the Debezium Postgres connector with Kafka Connect. Kafka
Connect exposes a REST API (localhost:8083) for managing connectors --
unlike everything else in this repo, this step never touches Kafka's
own producer/consumer client at all, it's a plain HTTP POST.

Run this ONCE, after the cdc profile containers are up and Kafka
Connect has finished starting (give it ~20-30s after `docker compose
--profile cdc up -d`):
    python register_connector.py
"""

import json
import time
import requests

CONNECT_URL = "http://localhost:8083"


def main():
    with open("connector-config.json") as f:
        config = json.load(f)

    # Kafka Connect can take a little while to be ready right after startup
    for attempt in range(10):
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            resp.raise_for_status()
            break
        except requests.exceptions.RequestException:
            print(f"Kafka Connect not ready yet, retrying... ({attempt + 1}/10)")
            time.sleep(3)
    else:
        print("Kafka Connect never became ready. Check `docker compose logs kafka-connect`.")
        return

    print("Registering connector...")
    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
    )

    if resp.status_code == 201:
        print("Connector registered successfully.")
    elif resp.status_code == 409:
        print("Connector already exists -- that's fine, nothing to do.")
    else:
        print(f"Unexpected response ({resp.status_code}): {resp.text}")
        return

    # confirm it's actually running, not just accepted
    status_resp = requests.get(f"{CONNECT_URL}/connectors/{config['name']}/status")
    print(json.dumps(status_resp.json(), indent=2))


if __name__ == "__main__":
    main()
