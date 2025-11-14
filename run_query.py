# run_query.py
# Adaptive JOIN query runner for ecommerce.db
# Usage: python run_query.py

import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "ecommerce.db")

# candidate column names to try (ordered by preference)
PRODUCT_NAME_CANDIDATES = ["product_name", "name", "title", "product_title", "product"]
ORDER_TOTAL_CANDIDATES   = ["total_amount", "total", "amount", "order_total"]
REVIEW_RATING_CANDIDATES = ["rating", "stars", "score"]
ORDER_ID_CANDIDATES      = ["order_id", "id"]
PRODUCT_ID_CANDIDATES    = ["product_id", "id"]
CUSTOMER_ID_CANDIDATES   = ["customer_id", "id"]
QUANTITY_CANDIDATES      = ["quantity", "qty"]

def get_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [r[0] for r in cur.fetchall()]

def get_columns(conn, table):
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

def find_first_in(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

def build_query(conn):
    tables = get_tables(conn)
    tables_lower = [t.lower() for t in tables]

    # require at least customers and orders
    if "customers" not in tables_lower or "orders" not in tables_lower:
        raise RuntimeError("Database must have at least 'customers' and 'orders' tables.")

    # find exact table names (preserve case)
    def find_table(name):
        for t in tables:
            if t.lower() == name:
                return t
        return None

    t_customers = find_table("customers")
    t_orders = find_table("orders")
    t_products = find_table("products") if "products" in tables_lower else None
    t_order_items = find_table("order_items") if "order_items" in tables_lower else None
    t_reviews = find_table("reviews") if "reviews" in tables_lower else None

    orders_cols = get_columns(conn, t_orders)
    customers_cols = get_columns(conn, t_customers)
    products_cols = get_columns(conn, t_products) if t_products else []
    oi_cols = get_columns(conn, t_order_items) if t_order_items else []
    reviews_cols = get_columns(conn, t_reviews) if t_reviews else []

    # choose id column names
    cust_id_col = find_first_in(customers_cols, CUSTOMER_ID_CANDIDATES) or customers_cols[0]
    order_id_col = find_first_in(orders_cols, ORDER_ID_CANDIDATES) or orders_cols[0]

    # product id location: prefer order_items, then orders
    product_id_col = None
    if oi_cols:
        product_id_col = find_first_in(oi_cols, PRODUCT_ID_CANDIDATES)
    if not product_id_col:
        product_id_col = find_first_in(orders_cols, PRODUCT_ID_CANDIDATES)

    # product name column
    product_name_col = find_first_in(products_cols, PRODUCT_NAME_CANDIDATES) if products_cols else None

    # choose quantity and subtotal/total columns
    quantity_col = find_first_in(oi_cols, QUANTITY_CANDIDATES) if oi_cols else None
    subtotal_col = find_first_in(oi_cols, ["subtotal", "line_total", "amount"]) if oi_cols else None
    order_total_col = find_first_in(orders_cols, ORDER_TOTAL_CANDIDATES)

    # review rating column
    rating_col = find_first_in(reviews_cols, REVIEW_RATING_CANDIDATES) if reviews_cols else None

    # Build SQL based on what's available
    if t_order_items and product_name_col:
        # Best-case: order_items + products + reviews
        sql = f"""
        SELECT
            c.{cust_id_col} AS customer_id,
            c.name AS customer_name,
            p.{product_name_col} AS product_name,
            o.{order_id_col} AS order_id,
            o.order_date,
            oi.{quantity_col or 'quantity'} AS quantity,
            oi.{subtotal_col or 'subtotal'} AS subtotal,
            {('r.' + rating_col) if rating_col else 'NULL AS rating'}
        FROM {t_customers} c
        JOIN {t_orders} o ON c.{cust_id_col} = o.customer_id
        JOIN {t_order_items} oi ON o.{order_id_col} = oi.order_id
        LEFT JOIN {t_products} p ON oi.product_id = p.{product_id_col or 'product_id'}
        LEFT JOIN {t_reviews} r ON r.customer_id = c.{cust_id_col} AND r.product_id = p.{product_id_col or 'product_id'}
        LIMIT 50;
        """
        return sql

    # fallback: if orders has product_id and products table present
    if product_id_col and t_products and product_name_col:
        qty = "1"
        subtotal_expr = f"o.{order_total_col}" if order_total_col else "o.total_amount"
        sql = f"""
        SELECT
            c.{cust_id_col} AS customer_id,
            c.name AS customer_name,
            p.{product_name_col} AS product_name,
            o.{order_id_col} AS order_id,
            o.order_date,
            {qty} AS quantity,
            {subtotal_expr} AS subtotal,
            {('r.' + rating_col) if rating_col else 'NULL AS rating'}
        FROM {t_customers} c
        JOIN {t_orders} o ON c.{cust_id_col} = o.customer_id
        LEFT JOIN {t_products} p ON o.{product_id_col} = p.{product_id_col}
        LEFT JOIN {t_reviews} r ON r.customer_id = c.{cust_id_col} AND r.product_id = p.{product_id_col}
        LIMIT 50;
        """
        return sql

    # last resort: show customers + orders totals
    sql = f"""
    SELECT
        c.{cust_id_col} AS customer_id,
        c.name AS customer_name,
        o.{order_id_col} AS order_id,
        o.order_date,
        o.{order_total_col or 'total_amount'} AS total_amount
    FROM {t_customers} c
    LEFT JOIN {t_orders} o ON c.{cust_id_col} = o.customer_id
    LIMIT 50;
    """
    return sql

def print_rows(headers, rows):
    widths = [len(h) for h in headers]
    for r in rows:
        for i, v in enumerate(r):
            s = "" if v is None else str(v)
            widths[i] = max(widths[i], len(s))
    # header
    header_line = " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    sep = "-+-".join("-"*widths[i] for i in range(len(headers)))
    print(header_line)
    print(sep)
    for r in rows:
        print(" | ".join((("" if v is None else str(v)).ljust(widths[i])) for i, v in enumerate(r)))
    print(f"\n{len(rows)} rows returned.\n")

def main():
    if not os.path.exists(DB_PATH):
        print("Database not found at", DB_PATH); sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        print("Tables in database:", ", ".join(get_tables(conn)))
        sql = build_query(conn)
        print("\nExecuting query:\n" + "-"*60 + "\n" + sql.strip() + "\n" + "-"*60)
        cur = conn.execute(sql)
        rows = cur.fetchall()
        if not rows:
            print("Query returned 0 rows.")
            return
        headers = [d[0] for d in cur.description]
        # convert rows to tuples
        rows_t = [tuple(r[h] for h in headers) for r in rows]
        print_rows(headers, rows_t)
    except sqlite3.OperationalError as e:
        print("SQLite error while running query:", e)
    except RuntimeError as e:
        print("Runtime error:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
