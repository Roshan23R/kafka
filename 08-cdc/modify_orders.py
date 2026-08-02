"""
Pattern 8: Change Data Capture
----------------------------------
Simulates an application making ordinary writes to the orders table --
completely unaware that Kafka or Debezium even exist. This is the
whole point of CDC: the application just does normal SQL, and change
capture happens transparently on the side.

Run:
    python modify_orders.py insert "Dave" 99.99
    python modify_orders.py update 1 SHIPPED
    python modify_orders.py delete 2
    python modify_orders.py list
"""

import sys
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "shopdb",
    "user": "postgres",
    "password": "postgres",
}


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1]
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    if command == "insert":
        name, amount = sys.argv[2], sys.argv[3]
        cur.execute(
            "INSERT INTO orders (customer_name, amount) VALUES (%s, %s) RETURNING order_id",
            (name, amount),
        )
        order_id = cur.fetchone()[0]
        print(f"Inserted order {order_id} for {name} (${amount})")

    elif command == "update":
        order_id, status = sys.argv[2], sys.argv[3]
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE order_id = %s",
            (status, order_id),
        )
        print(f"Updated order {order_id} -> status={status}")

    elif command == "delete":
        order_id = sys.argv[2]
        cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        print(f"Deleted order {order_id}")

    elif command == "list":
        cur.execute("SELECT order_id, customer_name, status, amount FROM orders ORDER BY order_id")
        for row in cur.fetchall():
            print(row)

    else:
        print(f"Unknown command: {command}")
        print(__doc__)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
