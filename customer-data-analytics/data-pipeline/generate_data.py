#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Minimal realistic data generator for Customer Data Analytics:
 users, product_categories, products, product_prices, orders, order_items, user_events

- No external libs; deterministic with SEED
- Produces CSVs in ./seed and SQL COPY scripts to load into Postgres
- Sized for "Lite" volumes; tweak CONFIG to scale

Author: Customer Data Analytics Team
"""

import csv
import os
import random
import math
from datetime import datetime, timedelta, date
from collections import defaultdict

# --------------------------
# CONFIG
# --------------------------
SEED = 42
OUT_DIR = "seed"

CONFIG = {
    "CATEGORIES_N": 12,
    "PRODUCTS_N": 1000,
    "USERS_N": 3000,
    "ORDERS_N": 20000,
    "EVENTS_APPROX": 120000,  # ориентир, реально будет зависеть от покупок
    "MONTHS_HISTORY": 12,     # price history months
    "PRICE_STEP": "week",     # "week" recommended
    "CURRENCY": "USD",
}

# Category price ranges (min, max)
CATEGORY_PRICE_RANGES = {
    "Laptops": (600, 2000),
    "Smartphones": (200, 1200),
    "Electronics": (30, 600),
    "Headphones": (20, 300),
    "Home": (10, 400),
    "Kitchen": (10, 400),
    "Clothing": (10, 150),
    "Shoes": (20, 250),
    "Sports": (10, 300),
    "Beauty": (5, 120),
    "Toys": (5, 120),
    "Accessories": (5, 80),
}

COLORS = ["Black", "White", "Gray", "Blue", "Red", "Green", "Yellow", "Pink", "Brown", "Beige"]
MATERIALS = ["Cotton", "Leather", "Textile", "Plastic", "Steel", "Wood", "Wool", "Polyester"]
STYLES = ["Sport", "Casual", "Classic", "Outdoor", "Tech"]
GENDERS = ["male", "female", "unisex"]
BRANDS = [f"Brand{chr(65+i)}" for i in range(26)] + \
         [f"Neo{i}" for i in range(20)] + \
         [f"Linea{i}" for i in range(20)] + \
         [f"Prime{i}" for i in range(20)]

COUNTRIES = ["US", "DE", "PL", "UK", "FR"]
CITIES = {
    "US": ["NYC", "SF", "LA", "Chicago", "Austin"],
    "DE": ["Berlin", "Munich", "Hamburg", "Cologne"],
    "PL": ["Warsaw", "Krakow", "Gdansk", "Wroclaw"],
    "UK": ["London", "Manchester", "Bristol", "Leeds"],
    "FR": ["Paris", "Lyon", "Marseille", "Lille"],
}

# --------------------------
# Helpers
# --------------------------
def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def rand_dt(start: datetime, end: datetime) -> datetime:
    """Random datetime between start and end, with diurnal/weekend effects."""
    total_sec = int((end - start).total_seconds())
    if total_sec <= 0:
        return start
    t = start + timedelta(seconds=random.randint(0, total_sec))
    # shift into realistic hours (8:00–22:00 more likely)
    hour_bias = random.random()
    if hour_bias < 0.75:
        hour = random.choice([8,9,10,11,12,13,14,15,16,17,18,19,20,21])
        t = t.replace(hour=hour, minute=random.randint(0,59), second=random.randint(0,59))
    # weekend boost
    if t.weekday() >= 5 and random.random() < 0.3:
        t += timedelta(hours=random.randint(0, 6))
    return t

def daterange(start_date: date, end_date: date, step_days=7):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=step_days)

def round_money(x: float) -> float:
    return round(x + 1e-8, 2)

def pick_weighted(items, weights):
    return random.choices(items, weights=weights, k=1)[0]

def price_at_date(price_history_list, target_date: date):
    """
    price_history_list: list of (valid_from_date, price, promo_flag) sorted by date asc
    Return last known price on or before target_date (or None if not found)
    """
    last = None
    for d, p, promo in price_history_list:
        if d <= target_date:
            last = p
        else:
            break
    return last

def now_utc():
    return datetime.now()

# --------------------------
# Generators
# --------------------------
def gen_categories(n: int):
    # select first n from the predefined mapping
    cats = list(CATEGORY_PRICE_RANGES.keys())[:n]
    return cats

def gen_products(products_n: int, categories: list, currency: str):
    products = []
    # Weighted brand popularity (Zipf-ish)
    brands = BRANDS.copy()
    weights = [1/(i+1) for i in range(len(brands))]  # BrandA most popular
    # time window for product created_at
    end = now_utc()
    start = end - timedelta(days=CONFIG["MONTHS_HISTORY"]*30)

    for pid in range(1, products_n+1):
        category = random.choice(categories)
        bmin, bmax = CATEGORY_PRICE_RANGES[category]
        # log-normal-ish price within range
        base = random.random()
        price = round_money(bmin + (bmax - bmin) * (base**1.7))
        brand = pick_weighted(brands, weights)
        color = random.choice(COLORS) if category not in ("Electronics", "Laptops", "Smartphones", "Headphones") or random.random()<0.5 else random.choice(COLORS)
        size = None
        if category in ("Clothing", "Shoes"):
            size = random.choice(["XS","S","M","L","XL","XXL"]) if category=="Clothing" else str(random.randint(36,46))
        material = random.choice(MATERIALS) if random.random()<0.8 else None
        gender = random.choice(GENDERS) if category in ("Clothing","Shoes") else ("unisex" if random.random()<0.8 else random.choice(GENDERS))
        style = random.choice(STYLES) if random.random()<0.8 else None
        rating = round(random.uniform(3.2, 4.9), 2)
        tag_base = [category.lower(), brand.lower()]
        tags = list(set(tag_base + random.sample([c.lower() for c in COLORS], k=random.randint(0,2)) + random.sample([s.lower() for s in STYLES], k=random.randint(0,2))))
        if len(tags)==0:
            tags = None
        attributes_json = {}
        if category in ("Smartphones","Electronics","Laptops","Headphones"):
            if random.random()<0.7: attributes_json["warranty"] = random.choice(["12m","24m"])
            if random.random()<0.5: attributes_json["battery"] = f"{random.choice([3000,4000,4500,5000])}mAh"
        elif category in ("Clothing","Shoes"):
            if random.random()<0.5: attributes_json["fit"] = random.choice(["regular","slim","relaxed"])

        title = f"{brand} {category[:-1] if category.endswith('s') else category} #{pid}"
        description = f"{title} in {color or 'assorted'} style {style or ''}".strip()

        created_at = rand_dt(start, end)
        is_active = random.random() < 0.92

        products.append({
            "product_id": pid,
            "title": title,
            "description": description,
            "category": category,
            "brand": brand,
            "price": price,
            "currency": currency,
            "color": color,
            "size": size,
            "material": material,
            "gender": gender,
            "style": style,
            "rating": rating,
            "tags": tags,
            "attributes_json": attributes_json if attributes_json else None,
            "created_at": created_at,
            "is_active": is_active
        })
    return products

def gen_price_history(products: list, months: int, step: str, currency: str):
    hist = defaultdict(list)  # product_id -> [(date, price, promo_flag)]
    end = date.today()
    start = end - timedelta(days=months*30)
    step_days = 7 if step == "week" else 1

    # seasonality: stronger promos in Nov-Dec; minor in Jun-Jul
    for p in products:
        base_price = p["price"]
        drift = 0.0
        for d in daterange(start, end, step_days):
            # monthly small drift ±0.02 over time
            if d.day <= step_days:
                drift += random.uniform(-0.02, 0.02)
                drift = max(-0.15, min(0.2, drift))
            price = base_price * (1.0 + drift)

            # promo waves
            promo = False
            month = d.month
            if month in (11,12) and random.random()<0.20:
                promo = True
            elif month in (6,7) and random.random()<0.10:
                promo = True
            elif random.random()<0.03:
                promo = True

            if promo:
                discount = random.uniform(0.10, 0.30)
                price = price * (1.0 - discount)

            price = max(0.5, price)
            hist[p["product_id"]].append((d, round_money(price), promo))
    return hist

def gen_users(n: int, months: int):
    end = now_utc()
    start = end - timedelta(days=months*30)
    users = []
    for i in range(1, n+1):
        email = f"user{i}@example.com"
        reg = rand_dt(start, end)
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES[country])
        # 50% have gender/birth_date
        gender = random.choice(["male","female"]) if random.random()<0.5 else None
        birth_date = None
        if random.random()<0.5:
            # 18–65 years old
            years = random.randint(18,65)
            birth_date = (reg.date() - timedelta(days=years*365 - random.randint(0,365)))
        users.append({
            "user_id": i,
            "email": email,
            "phone": None,
            "registered_at": reg,
            "country": country,
            "city": city,
            "gender": gender,
            "birth_date": birth_date
        })
    return users

def gen_orders_and_items(orders_n: int, users: list, products: list, price_hist: dict, currency: str):
    # popularity weights: 20% products -> 70% sales
    prod_ids = [p["product_id"] for p in products]
    prod_weights = []
    for idx, pid in enumerate(prod_ids):
        # Zipf-like
        prod_weights.append(1.0/(idx+1))
    # normalize
    s = sum(prod_weights)
    prod_weights = [w/s for w in prod_weights]

    orders = []
    items = []
    # map user_id -> registered_at
    reg_map = {u["user_id"]: u["registered_at"] for u in users}

    end = now_utc()
    statuses = ["paid"]*45 + ["completed"]*45 + ["cancelled"]*10

    # user segments: 15% power, 60% normal, 25% low
    user_segment = {}
    for u in users:
        r = random.random()
        user_segment[u["user_id"]] = "power" if r<0.15 else ("normal" if r<0.75 else "low")

    for oid in range(1, orders_n+1):
        user = random.choice(users)
        reg = reg_map[user["user_id"]]
        created_at = rand_dt(reg, end)
        status = random.choice(statuses)

        # pick 1–4 items (fewer for expensive categories)
        k = random.randint(1,4)
        order_product_ids = random.choices(prod_ids, weights=prod_weights, k=k)

        total = 0.0
        for pid in order_product_ids:
            # price from price history at order date
            ph = price_hist.get(pid, [])
            price = price_at_date(ph, created_at.date())
            if price is None:
                # fallback to product base price
                price = next(p["price"] for p in products if p["product_id"]==pid)
            # quantity: 1–3, but if expensive category, keep small
            cat = next(p["category"] for p in products if p["product_id"]==pid)
            qmax = 1 if cat in ("Laptops","Smartphones","Electronics") else 3
            qty = random.randint(1, qmax)
            items.append({
                "order_item_id": len(items)+1,
                "order_id": oid,
                "product_id": pid,
                "quantity": qty,
                "unit_price": round_money(price),
                "currency": currency
            })
            total += qty * price

        total = round_money(total)
        orders.append({
            "order_id": oid,
            "user_id": user["user_id"],
            "created_at": created_at,
            "status": status,
            "total_amount": total,
            "currency": currency,
            "promo_id": None
        })

    # adjust totals to exact sum of items (already exact by construction)
    return orders, items

def gen_events(orders, items, users, products):
    """
    Generate view_product, add_to_cart, purchase events.
    - For each order: 1–5 views of its products within 0–3 days before order, plus optional add_to_cart
    - For non-buyers: random browsing
    """
    events = []
    # build order -> product_ids
    o2p = defaultdict(list)
    for it in items:
        o2p[it["order_id"]].append(it["product_id"])

    user_orders = defaultdict(list)
    for o in orders:
        user_orders[o["user_id"]].append(o)

    user_map = {u["user_id"]: u for u in users}

    # events for orders
    for o in orders:
        if o["status"] == "cancelled":
            # still allow pre-events but no final purchase event
            pass
        pids = o2p[o["order_id"]]
        n_views = random.randint(1, 5)
        for _ in range(n_views):
            pid = random.choice(pids)
            t = o["created_at"] - timedelta(days=random.randint(0,3), hours=random.randint(0,22))
            events.append({
                "user_id": o["user_id"],
                "event_time": t,
                "event_type": "view_product",
                "product_id": pid,
                "value": None,
                "meta": None
            })
            if random.random() < 0.5:
                t2 = t + timedelta(minutes=random.randint(5, 240))
                events.append({
                    "user_id": o["user_id"],
                    "event_time": t2 if t2 <= o["created_at"] else o["created_at"] - timedelta(minutes=5),
                    "event_type": "add_to_cart",
                    "product_id": pid,
                    "value": None,
                    "meta": None
                })
        # purchase event (if not cancelled)
        if o["status"] != "cancelled":
            events.append({
                "user_id": o["user_id"],
                "event_time": o["created_at"],
                "event_type": "purchase",
                "product_id": None,
                "value": o["total_amount"],
                "meta": {"order_id": o["order_id"]}
            })

    # browsing-only users (no orders)
    buyer_ids = set([o["user_id"] for o in orders])
    non_buyers = [u for u in users if u["user_id"] not in buyer_ids]
    end = now_utc()
    start = end - timedelta(days=CONFIG["MONTHS_HISTORY"]*30)
    prod_ids = [p["product_id"] for p in products]
    for u in random.sample(non_buyers, k=int(0.7*len(non_buyers))):
        n = random.randint(1, 20)
        for _ in range(n):
            pid = random.choice(prod_ids)
            t = rand_dt(u["registered_at"], end)
            events.append({
                "user_id": u["user_id"],
                "event_time": t,
                "event_type": "view_product",
                "product_id": pid,
                "value": None,
                "meta": None
            })
            if random.random() < 0.3:
                events.append({
                    "user_id": u["user_id"],
                    "event_time": t + timedelta(minutes=random.randint(1,180)),
                    "event_type": "add_to_cart",
                    "product_id": pid,
                    "value": None,
                    "meta": None
                })

    # sort by time
    events.sort(key=lambda e: (e["user_id"], e["event_time"]))
    return events

# --------------------------
# Writers
# --------------------------
def write_csv(path, rows, header):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([r.get(h) if not isinstance(r.get(h), dict) else str(r.get(h)) for h in header])

def write_products_csv(path, products):
    header = ["product_id","title","description","category","brand","price","currency","color","size","material",
              "gender","style","rating","tags","attributes_json","created_at","is_active"]
    rows = []
    for p in products:
        # Convert attributes_json to proper JSON string
        attrs_json = None
        if p["attributes_json"]:
            import json
            attrs_json = json.dumps(p["attributes_json"])
        
        rows.append({
            **p,
            "tags": "{" + ",".join(p["tags"]) + "}" if p["tags"] else None,
            "attributes_json": attrs_json,
            "created_at": p["created_at"].isoformat()
        })
    write_csv(path, rows, header)

def write_product_prices_csv(path, price_hist):
    header = ["product_id","valid_from","price","currency","promo_flag"]
    rows = []
    for pid, lst in price_hist.items():
        for d, price, promo in lst:
            rows.append({
                "product_id": pid,
                "valid_from": d.isoformat(),
                "price": price,
                "currency": CONFIG["CURRENCY"],
                "promo_flag": promo
            })
    write_csv(path, rows, header)

def write_users_csv(path, users):
    header = ["user_id","email","phone","registered_at","country","city","gender","birth_date"]
    rows = []
    for u in users:
        rows.append({
            **u,
            "registered_at": u["registered_at"].isoformat(),
            "birth_date": u["birth_date"].isoformat() if u["birth_date"] else None
        })
    write_csv(path, rows, header)

def write_orders_csv(path, orders):
    header = ["order_id","user_id","created_at","status","total_amount","currency","promo_id"]
    rows = []
    for o in orders:
        rows.append({**o, "created_at": o["created_at"].isoformat()})
    write_csv(path, rows, header)

def write_order_items_csv(path, items):
    header = ["order_item_id","order_id","product_id","quantity","unit_price","currency"]
    write_csv(path, items, header)

def write_events_csv(path, events):
    header = ["event_id","user_id","event_time","event_type","product_id","value","meta"]
    rows = []
    for i, e in enumerate(events, start=1):
        # Convert meta to proper JSON string
        meta_json = None
        if e["meta"]:
            import json
            meta_json = json.dumps(e["meta"])
        
        rows.append({
            "event_id": i,
            "user_id": e["user_id"],
            "event_time": e["event_time"].isoformat(),
            "event_type": e["event_type"],
            "product_id": e["product_id"],
            "value": e["value"],
            "meta": meta_json
        })
    write_csv(path, rows, header)

def write_categories_sql(path, categories):
    with open(path, "w", encoding="utf-8") as f:
        f.write("-- product_categories\n")
        for c in categories:
            f.write(f"INSERT INTO product_categories(category) VALUES ('{c}');\n")

def write_copy_sql(path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\\copy products FROM 'seed/02_products.csv' CSV HEADER NULL '';\n")
        f.write("\\copy product_prices FROM 'seed/03_product_prices.csv' CSV HEADER NULL '';\n")
        f.write("\\copy users FROM 'seed/04_users.csv' CSV HEADER NULL '';\n")
        f.write("\\copy orders FROM 'seed/05_orders.csv' CSV HEADER NULL '';\n")
        f.write("\\copy order_items FROM 'seed/06_order_items.csv' CSV HEADER NULL '';\n")
        f.write("\\copy user_events FROM 'seed/07_user_events.csv' CSV HEADER NULL '';\n")

def write_validate_sql(path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("""
-- 1. Sum(items) == total_amount
SELECT COUNT(*) AS total,
       SUM(CASE WHEN t.sum_items = o.total_amount THEN 1 ELSE 0 END) AS ok
FROM orders o
JOIN (
  SELECT order_id, SUM(quantity*unit_price)::numeric(12,2) AS sum_items
  FROM order_items GROUP BY order_id
) t ON t.order_id = o.order_id;

-- 2. No events before registration
SELECT COUNT(*) AS bad_events
FROM user_events e JOIN users u USING(user_id)
WHERE e.event_time < u.registered_at;

-- 3. Price exists at order date
SELECT COUNT(*) AS missing_price
FROM order_items oi
JOIN orders o USING(order_id)
LEFT JOIN LATERAL (
  SELECT price FROM product_prices pp
  WHERE pp.product_id=oi.product_id AND pp.valid_from<=o.created_at::date
  ORDER BY pp.valid_from DESC LIMIT 1
) p ON TRUE
WHERE p.price IS NULL;

-- 4. Add-to-cart / views sanity (5–40%)
WITH v AS (SELECT COUNT(*) views FROM user_events WHERE event_type='view_product'),
     a AS (SELECT COUNT(*) adds  FROM user_events WHERE event_type='add_to_cart')
SELECT 100.0*a.adds/NULLIF(v.views,0) AS add_rate FROM v,a;
""")

# --------------------------
# MAIN
# --------------------------
def main():
    random.seed(SEED)
    ensure_dir(OUT_DIR)

    print("🚀 Генерация тестовых данных для Customer Data Analytics...")
    print(f"📊 Параметры: {CONFIG}")

    # 1) categories
    print("📁 Генерация категорий...")
    categories = gen_categories(CONFIG["CATEGORIES_N"])
    write_categories_sql(os.path.join(OUT_DIR, "01_categories.sql"), categories)

    # 2) products
    print("📦 Генерация товаров...")
    products = gen_products(CONFIG["PRODUCTS_N"], categories, CONFIG["CURRENCY"])
    write_products_csv(os.path.join(OUT_DIR, "02_products.csv"), products)

    # 3) price history
    print("💰 Генерация истории цен...")
    price_hist = gen_price_history(products, CONFIG["MONTHS_HISTORY"], CONFIG["PRICE_STEP"], CONFIG["CURRENCY"])
    write_product_prices_csv(os.path.join(OUT_DIR, "03_product_prices.csv"), price_hist)

    # 4) users
    print("👥 Генерация пользователей...")
    users = gen_users(CONFIG["USERS_N"], CONFIG["MONTHS_HISTORY"])
    write_users_csv(os.path.join(OUT_DIR, "04_users.csv"), users)

    # 5) orders & items
    print("🛒 Генерация заказов...")
    orders, items = gen_orders_and_items(CONFIG["ORDERS_N"], users, products, price_hist, CONFIG["CURRENCY"])
    write_orders_csv(os.path.join(OUT_DIR, "05_orders.csv"), orders)
    write_order_items_csv(os.path.join(OUT_DIR, "06_order_items.csv"), items)

    # 6) events
    print("📊 Генерация событий...")
    events = gen_events(orders, items, users, products)
    write_events_csv(os.path.join(OUT_DIR, "07_user_events.csv"), events)

    # 7) COPY & validate SQL
    print("📝 Создание SQL скриптов...")
    write_copy_sql(os.path.join(OUT_DIR, "00_copy.sql"))
    write_validate_sql(os.path.join(OUT_DIR, "99_validate.sql"))

    print(f"✅ Готово! Файлы в ./{OUT_DIR}")
    print("\n📋 Порядок загрузки:")
    print("  psql -d customer_data -f seed/01_categories.sql")
    print("  psql -d customer_data -f seed/00_copy.sql")
    print("\n🔍 Проверка:")
    print("  psql -d customer_data -f seed/99_validate.sql")

if __name__ == "__main__":
    main()
