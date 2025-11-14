import csv
import random
import uuid
from datetime import datetime, timedelta
import os


def ensure_dir(path):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def generate_customers(n=100):
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    domains = ["example.com", "email.com", "shopnow.com", "mail.com"]
    streets = ["Oak", "Maple", "Pine", "Cedar", "Elm"]
    cities = ["Springfield", "Rivertown", "Greenville", "Fairview"]
    states = ["CA", "NY", "TX", "WA", "FL"]

    rows = []
    for i in range(1, n + 1):
        cid = f"CUST{i:04d}"
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        name = f"{fn} {ln}"
        email = f"{fn.lower()}.{ln.lower()}{i}@{random.choice(domains)}"
        phone = f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        created = datetime.now() - timedelta(days=random.randint(1, 2000))
        address = f"{random.randint(100, 9999)} {random.choice(streets)} St, {random.choice(cities)}, {random.choice(states)}"
        rows.append({
            "customer_id": cid,
            "name": name,
            "email": email,
            "phone": phone,
            "address": address,
            "created_at": created.isoformat()
        })
    return rows


def generate_products(n=100):
    categories = ["Electronics", "Books", "Home", "Toys", "Clothing", "Sports", "Beauty"]
    adjectives = ["Portable", "Advanced", "Smart", "Eco", "Premium", "Compact", "Durable", "Classic"]
    items = ["Headphones", "Lamp", "Backpack", "Blender", "Watch", "Camera", "Mug", "Sneakers", "Jacket", "Game"]

    rows = []
    for i in range(1, n + 1):
        pid = f"PROD{i:04d}"
        name = f"{random.choice(adjectives)} {random.choice(items)}"
        category = random.choice(categories)
        price = round(random.uniform(5.0, 499.99), 2)
        stock = random.randint(0, 500)
        created = datetime.now() - timedelta(days=random.randint(1, 1500))
        rows.append({
            "product_id": pid,
            "name": name,
            "category": category,
            "price": price,
            "stock": stock,
            "created_at": created.isoformat()
        })
    return rows


def generate_orders(n=100, customers=None, products=None):
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    rows = []
    for i in range(1, n + 1):
        oid = f"ORD{i:05d}"
        cust = random.choice(customers)["customer_id"]
        product = random.choice(products)
        qty = random.randint(1, 5)
        order_date = datetime.now() - timedelta(days=random.randint(1, 365))
        subtotal = round(product["price"] * qty, 2)
        shipping = 0 if subtotal > 50 else round(random.uniform(3.99, 9.99), 2)
        total = round(subtotal + shipping, 2)
        status = random.choices(statuses, weights=[10, 20, 30, 30, 10])[0]
        rows.append({
            "order_id": oid,
            "customer_id": cust,
            "product_id": product["product_id"],
            "quantity": qty,
            "order_date": order_date.isoformat(),
            "status": status,
            "subtotal": subtotal,
            "shipping": shipping,
            "total": total
        })
    return rows


def generate_payments(orders):
    methods = ["credit_card", "paypal", "bank_transfer", "apple_pay"]
    statuses = ["paid", "pending", "failed"]
    rows = []
    for o in orders:
        pid = f"PAY-{uuid.uuid4().hex[:8]}"
        order_id = o["order_id"]
        amt = o["total"] if random.random() > 0.05 else round(o["total"] * random.uniform(0.3, 0.9), 2)
        method = random.choice(methods)
        status = random.choices(statuses, weights=[85, 10, 5])[0]
        order_dt = datetime.fromisoformat(o["order_date"])
        pay_dt = order_dt + timedelta(days=random.randint(0, 7))
        rows.append({
            "payment_id": pid,
            "order_id": order_id,
            "amount": amt,
            "method": method,
            "status": status,
            "payment_date": pay_dt.isoformat()
        })
    return rows


def generate_reviews(n=100, customers=None, products=None):
    sample_texts = [
        "Excellent product, highly recommend!",
        "Good value for money.",
        "Arrived late but works as expected.",
        "Not satisfied with the quality.",
        "Exceeded my expectations.",
        "Will buy again.",
        "Too expensive for what it offers.",
        "Five stars!"
    ]
    rows = []
    for i in range(1, n + 1):
        rid = f"REV{i:05d}"
        customer_id = random.choice(customers)["customer_id"]
        product = random.choice(products)
        rating = random.randint(1, 5)
        text = random.choice(sample_texts)
        review_date = datetime.now() - timedelta(days=random.randint(1, 800))
        rows.append({
            "review_id": rid,
            "product_id": product["product_id"],
            "customer_id": customer_id,
            "rating": rating,
            "review_text": text,
            "review_date": review_date.isoformat()
        })
    return rows


def write_csv(path, fieldnames, rows):
    ensure_dir(path)
    with open(path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main(output_dir="."):
    customers = generate_customers(100)
    products = generate_products(100)
    orders = generate_orders(100, customers=customers, products=products)
    payments = generate_payments(orders)
    reviews = generate_reviews(100, customers=customers, products=products)

    write_csv(os.path.join(output_dir, "customers.csv"),
              ["customer_id", "name", "email", "phone", "address", "created_at"], customers)

    write_csv(os.path.join(output_dir, "products.csv"),
              ["product_id", "name", "category", "price", "stock", "created_at"], products)

    write_csv(os.path.join(output_dir, "orders.csv"),
              ["order_id", "customer_id", "product_id", "quantity", "order_date", "status", "subtotal", "shipping", "total"], orders)

    write_csv(os.path.join(output_dir, "payments.csv"),
              ["payment_id", "order_id", "amount", "method", "status", "payment_date"], payments)

    write_csv(os.path.join(output_dir, "reviews.csv"),
              ["review_id", "product_id", "customer_id", "rating", "review_text", "review_date"], reviews)

    print("Generated: customers.csv, products.csv, orders.csv, payments.csv, reviews.csv")


if __name__ == "__main__":
    main()
