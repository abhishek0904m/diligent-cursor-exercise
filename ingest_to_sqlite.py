"""
Ingest CSV files into SQLite `ecommerce.db`.
Creates tables (DROP IF EXISTS) and bulk inserts rows from the CSV files.

Run:
    python d:\\Desktop\\aaa\\ingest_to_sqlite.py

"""
import sqlite3
import csv
import os
from typing import Dict

WORKDIR = os.path.dirname(__file__) or '.'
DB_PATH = os.path.join(WORKDIR, 'ecommerce.db')

# Define schemas for each table. Use SQLite types.
SCHEMAS = {
    'customers.csv': {
        'table': 'customers',
        'columns': [
            ('customer_id', 'TEXT'),
            ('name', 'TEXT'),
            ('email', 'TEXT'),
            ('phone', 'TEXT'),
            ('address', 'TEXT'),
            ('created_at', 'TEXT')
        ]
    },
    'products.csv': {
        'table': 'products',
        'columns': [
            ('product_id', 'TEXT'),
            ('name', 'TEXT'),
            ('category', 'TEXT'),
            ('price', 'REAL'),
            ('stock', 'INTEGER'),
            ('created_at', 'TEXT')
        ]
    },
    'orders.csv': {
        'table': 'orders',
        'columns': [
            ('order_id', 'TEXT'),
            ('customer_id', 'TEXT'),
            ('product_id', 'TEXT'),
            ('quantity', 'INTEGER'),
            ('order_date', 'TEXT'),
            ('status', 'TEXT'),
            ('subtotal', 'REAL'),
            ('shipping', 'REAL'),
            ('total', 'REAL')
        ]
    },
    'payments.csv': {
        'table': 'payments',
        'columns': [
            ('payment_id', 'TEXT'),
            ('order_id', 'TEXT'),
            ('amount', 'REAL'),
            ('method', 'TEXT'),
            ('status', 'TEXT'),
            ('payment_date', 'TEXT')
        ]
    },
    'reviews.csv': {
        'table': 'reviews',
        'columns': [
            ('review_id', 'TEXT'),
            ('product_id', 'TEXT'),
            ('customer_id', 'TEXT'),
            ('rating', 'INTEGER'),
            ('review_text', 'TEXT'),
            ('review_date', 'TEXT')
        ]
    }
}


def convert_value(col_type: str, value: str):
    """Convert CSV string to appropriate Python value for SQLite insertion."""
    if value is None or value == '':
        return None
    if col_type == 'INTEGER':
        try:
            return int(float(value))
        except Exception:
            return None
    if col_type == 'REAL':
        try:
            return float(value)
        except Exception:
            return None
    # For TEXT and all else, return the string as-is
    return value


def create_table(conn: sqlite3.Connection, table: str, columns: list):
    cols_sql = ', '.join([f'"{name}" {ctype}' for name, ctype in columns])
    sql = f"DROP TABLE IF EXISTS \"{table}\"; CREATE TABLE \"{table}\" ({cols_sql});"
    conn.executescript(sql)


def ingest_csv(conn: sqlite3.Connection, csv_path: str, schema: Dict):
    table = schema['table']
    columns = schema['columns']
    col_names = [c[0] for c in columns]
    col_types = {c[0]: c[1] for c in columns}

    # Read CSV
    full_path = os.path.join(WORKDIR, csv_path)
    if not os.path.exists(full_path):
        print(f"Warning: {full_path} not found, skipping.")
        return 0

    with open(full_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            # Build row values in column order
            vals = [convert_value(col_types[col], r.get(col, '').strip()) for col in col_names]
            rows.append(tuple(vals))

    if not rows:
        return 0

    placeholders = ','.join(['?'] * len(col_names))
    insert_sql = f'INSERT INTO "{table}" ({",".join(["\""+c+"\"" for c in col_names])}) VALUES ({placeholders})'
    with conn:
        conn.executemany(insert_sql, rows)
    return len(rows)


def main():
    # Remove existing DB (optional) -- we'll overwrite tables anyway
    conn = sqlite3.connect(DB_PATH)

    try:
        total = {}
        # Create tables and ingest each CSV
        for csv_file, schema in SCHEMAS.items():
            print(f"Creating table '{schema['table']}' and loading from {csv_file}...")
            create_table(conn, schema['table'], schema['columns'])
            count = ingest_csv(conn, csv_file, schema)
            print(f"Inserted {count} rows into {schema['table']}")
            total[schema['table']] = count

        # List tables and counts
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r[0] for r in cur.fetchall()]
        print('\nCreated tables:')
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM \"{t}\"")
            c = cur.fetchone()[0]
            print(f" - {t}: {c} rows")

        print(f"\nDatabase written to: {DB_PATH}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
