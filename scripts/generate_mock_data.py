#!/usr/bin/env python3
"""
Generate synthetic (completely fake) Thornquist Outfitters data for the
data-lake project.

It creates the three source types described in the project brief:

  mock_data/pos/        daily point-of-sale CSV files      pos_YYYY-MM-DD.csv
  mock_data/ecommerce/  daily e-commerce order files       orders_YYYY-MM-DD.json
  mock_data/customers/  weekly legacy customer extracts    customers_YYYY-MM-DD.txt

The data is deliberately messy, the way the brief describes: duplicate rows,
malformed dates, orphaned customer IDs, placeholder values. The three sources
also disagree with each other on purpose (date formats, ID formats, category
names, units, column naming) so there are real schema conflicts to reconcile.

Everything is random but seeded, so the same seed always produces identical
files. Give your partner the same seed (or the same zip) and you both work
on the same data. A summary of every problem that was injected is written to
mock_data/_INJECTED_ISSUES.txt so you can check your quality rules catch them.

No real people or companies are involved. Emails use example.com.

Usage:
  python3 generate_mock_data.py
  python3 generate_mock_data.py --out ./mock_data --seed 42 --days 14
"""

import argparse
import copy
import csv
import json
import random
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

START = date(2026, 9, 17)
SNAPSHOT_A = date(2026, 9, 21)   # first weekly customer extract
SNAPSHOT_B = date(2026, 9, 28)   # second weekly customer extract
USD_RATE = 0.73                  # used for the few e-commerce orders in USD

PROVINCES = {
    "ON": "Ontario", "QC": "Quebec", "BC": "British Columbia",
    "AB": "Alberta", "MB": "Manitoba", "NS": "Nova Scotia",
}
PROVINCE_WEIGHTS = [38, 24, 16, 12, 5, 5]

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

FIRST_NAMES = ["Alex", "Sam", "Jordan", "Taylor", "Morgan", "Casey", "Riley",
               "Jamie", "Avery", "Quinn", "Dana", "Robin", "Drew", "Skyler",
               "Reese", "Cameron", "Harper", "Rowan", "Elliot", "Sasha"]
LAST_NAMES = ["Tremblay", "Singh", "Campbell", "Roy", "Wilson", "Chen",
              "MacDonald", "Gagnon", "Patel", "Murphy", "Fraser", "Lavoie",
              "Nguyen", "Stewart", "Bouchard", "Kaur", "Clarke", "Ouellet",
              "Morin", "Reid"]

# (sku number, product name, POS category, e-commerce category path, price in CAD)
# The two category columns use DIFFERENT naming on purpose.
CATALOG = [
    (1001, "Trailblazer Hiking Boots", "Footwear", "Shoes > Hiking Boots", 149.99),
    (1002, "Summit Trail Runners", "Footwear", "Shoes > Trail Running", 119.99),
    (1003, "Camp Moccasin Slippers", "Footwear", "Shoes > Camp Footwear", 39.99),
    (1101, "Alpine Shell Jacket", "Apparel", "Clothing > Jackets", 229.00),
    (1102, "Merino Base Layer Top", "Apparel", "Clothing > Base Layers", 89.50),
    (1103, "Fleece Pullover", "Apparel", "Clothing > Midlayers", 79.99),
    (1104, "Quick-Dry Hiking Pants", "Apparel", "Clothing > Pants", 99.00),
    (1105, "Wool Toque", "Apparel", "Clothing > Headwear", 24.99),
    (1201, "Basecamp 4-Person Tent", "Camping", "Camp & Hike > Tents", 399.00),
    (1202, "Ultralight 2-Person Tent", "Camping", "Camp & Hike > Tents", 349.00),
    (1203, "Three-Season Sleeping Bag", "Camping", "Camp & Hike > Sleep Systems", 189.99),
    (1204, "Insulated Sleeping Pad", "Camping", "Camp & Hike > Sleep Systems", 94.99),
    (1205, "Single-Burner Camp Stove", "Camping", "Camp & Hike > Cooking", 64.99),
    (1206, "Titanium Cookset", "Camping", "Camp & Hike > Cooking", 74.95),
    (1301, "Daypack 24L", "Packs", "Bags > Daypacks", 89.99),
    (1302, "Overnight Pack 55L", "Packs", "Bags > Backpacking Packs", 259.00),
    (1303, "Waterproof Dry Bag 20L", "Packs", "Bags > Dry Bags", 29.99),
    (1401, "Headlamp 400 Lumen", "Accessories", "Gear > Lighting", 44.99),
    (1402, "Insulated Steel Bottle", "Accessories", "Gear > Hydration", 34.99),
    (1403, "Trekking Poles (Pair)", "Accessories", "Gear > Trekking Poles", 109.00),
    (1404, "Compass and Map Set", "Accessories", "Gear > Navigation", 27.50),
    (1405, "First Aid Kit", "Accessories", "Gear > Safety", 36.99),
]


# ----------------------------------------------------------------- helpers
def fmt_pos_date(d):
    """POS uses MM/DD/YYYY."""
    return f"{d.month:02d}/{d.day:02d}/{d.year}"


def fmt_legacy_date(d):
    """The legacy customer system uses DD-MON-YY, e.g. 05-MAR-21."""
    return f"{d.day:02d}-{MONTHS[d.month - 1]}-{d.year % 100:02d}"


def day_volume(rng, day, low, high):
    """Busier on weekends."""
    n = rng.randint(low, high)
    return int(n * 1.3) if day.weekday() >= 5 else n


def make_stores(rng):
    codes = list(PROVINCES)
    return {f"S{i:03d}": rng.choices(codes, PROVINCE_WEIGHTS)[0] for i in range(1, 41)}


# --------------------------------------------------------------------- POS
POS_HEADER = ["txn_id", "store_id", "province", "txn_date", "sku", "product_name",
              "category", "quantity", "unit_price", "payment_type", "loyalty_id"]


def write_pos(rng, out_dir, days, stores, n_cust, stats, bad_files):
    out_dir.mkdir(parents=True, exist_ok=True)
    store_ids = list(stores)

    for day in days:
        rows = []
        for seq in range(1, day_volume(rng, day, 140, 220) + 1):
            store = rng.choice(store_ids)
            sku_num, name, category, _web_path, price = rng.choice(CATALOG)
            qty = str(rng.choices([1, 2, 3, 4], [70, 20, 7, 3])[0])
            sale_price = round(price * 0.9, 2) if rng.random() < 0.15 else price
            unit_price = f"{sale_price:.2f}"
            txn_date = fmt_pos_date(day)

            # Loyalty ID: blank is normal (walk-in customer). A few IDs point
            # at customers that do not exist, which are the "orphans".
            r = rng.random()
            if r < 0.45:
                loyalty = ""
            elif r < 0.49:
                loyalty = f"L{rng.randint(n_cust + 1, n_cust + 40):06d}"
            else:
                loyalty = f"L{rng.randint(1, n_cust):06d}"

            # Injected problems
            if rng.random() < 0.012:
                unit_price = rng.choice(["N/A", "-1.00", "0.00"])
            if rng.random() < 0.010:
                qty = str(rng.choice([0, -2]))
            if rng.random() < 0.008:
                category = rng.choice(["UNKNOWN", ""])
            if rng.random() < 0.012:
                txn_date = rng.choice(["2026-13-45", "31/09/2026", "00/00/0000", ""])

            rows.append([
                f"{store}-{day:%Y%m%d}-{seq:05d}", store, stores[store], txn_date,
                f"TH-{sku_num}", name, category, qty, unit_price,
                rng.choice(["credit", "debit", "cash", "mobile"]), loyalty,
            ])

        # Exact duplicate rows
        for _ in range(max(1, int(len(rows) * 0.015))):
            dup = list(rng.choice(rows))
            rows.insert(rng.randint(0, len(rows)), dup)
            stats["pos_duplicate_rows"] += 1

        count_pos_issues(rows, n_cust, stats)
        stats["pos_rows_total"] += len(rows)
        path = out_dir / f"pos_{day:%Y-%m-%d}.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(POS_HEADER)
            w.writerows(rows)

    # Two broken files, so the upload script's arrival check has something to reject.
    if bad_files:
        last = days[-1]
        (out_dir / f"pos_{last + timedelta(days=1):%Y-%m-%d}.csv").write_text("")
        (out_dir / f"pos_{last + timedelta(days=2):%Y-%m-%d}.csv").write_text(
            ",".join(POS_HEADER) + "\n")
        stats["pos_empty_file"] = 1
        stats["pos_header_only_file"] = 1


# Problems are counted from the FINISHED rows, not when they are injected.
# Duplicate rows are exact copies, so a duplicated bad row carries its problem twice.
def _valid_pos_date(s):
    try:
        datetime.strptime(s, "%m/%d/%Y")
        return True
    except ValueError:
        return False


def count_pos_issues(rows, n_cust, stats):
    for r in rows:
        if not _valid_pos_date(r[3]):
            stats["pos_bad_date"] += 1
        if r[6] in ("UNKNOWN", ""):
            stats["pos_missing_category"] += 1
        if r[7] in ("0", "-2"):
            stats["pos_bad_quantity"] += 1
        if r[8] in ("N/A", "-1.00", "0.00"):
            stats["pos_bad_price"] += 1
        if r[10] and int(r[10][1:]) > n_cust:
            stats["pos_orphan_customer_ids"] += 1


def count_ecom_issues(orders, n_cust, stats):
    for o in orders:
        try:
            datetime.fromisoformat(o["orderTimestamp"])
        except ValueError:
            stats["ecom_bad_timestamp"] += 1
        if o["shippingAddress"]["province"] == "UNKNOWN":
            stats["ecom_unknown_province"] += 1
        if (o["customer"]["customerId"] or 0) > n_cust:
            stats["ecom_orphan_customer_ids"] += 1
        stats["ecom_bad_price"] += sum(1 for i in o["items"] if i["priceCents"] <= 0)


# --------------------------------------------------------------- e-commerce
def write_ecommerce(rng, out_dir, days, n_cust, stats):
    """One order per line (JSON Lines), saved with a .json extension.

    One-object-per-line is what Glue crawlers handle best. A file that is one
    giant JSON array is a classic way to confuse a crawler.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    codes = list(PROVINCES)
    counter = 100000

    for day in days:
        orders = []
        for _ in range(day_volume(rng, day, 55, 95)):
            counter += 1
            currency = "USD" if rng.random() < 0.12 else "CAD"

            items = []
            for _ in range(rng.choices([1, 2, 3, 4], [55, 28, 12, 5])[0]):
                sku_num, name, _pos_cat, web_path, price = rng.choice(CATALOG)
                cents = int(round(price * 100 * (USD_RATE if currency == "USD" else 1)))
                if rng.random() < 0.008:
                    cents = rng.choice([-1, 0])
                items.append({
                    "sku": f"TH{sku_num}",
                    "title": name,
                    "categoryPath": web_path,
                    "qty": rng.choices([1, 2, 3], [80, 15, 5])[0],
                    "priceCents": cents,
                })

            # Customer: guests have no ID (normal), a few IDs are orphans.
            r = rng.random()
            if r < 0.12:
                customer = {"customerId": None, "guest": True}
            elif r < 0.16:
                customer = {"customerId": rng.randint(n_cust + 1, n_cust + 40), "guest": False}
            else:
                customer = {"customerId": rng.randint(1, n_cust), "guest": False}

            province = rng.choices(codes, PROVINCE_WEIGHTS)[0]
            if rng.random() < 0.01:
                province = "UNKNOWN"

            timestamp = (f"{day:%Y-%m-%d}T{rng.randint(0, 23):02d}:"
                         f"{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}-04:00")
            if rng.random() < 0.012:
                timestamp = rng.choice(["2026-09-31T10:15:00-04:00", "yesterday",
                                        "17/09/2026 10:15", ""])

            orders.append({
                "orderId": f"EC-{counter:06d}",
                "orderTimestamp": timestamp,
                "channel": rng.choice(["web", "app"]),
                "currency": currency,
                "customer": customer,
                "shippingAddress": {"province": province, "country": "CA"},
                "items": items,
            })

        for _ in range(max(1, int(len(orders) * 0.015))):
            dup = copy.deepcopy(rng.choice(orders))
            orders.insert(rng.randint(0, len(orders)), dup)
            stats["ecom_duplicate_orders"] += 1

        count_ecom_issues(orders, n_cust, stats)
        stats["ecom_orders_total"] += len(orders)
        path = out_dir / f"orders_{day:%Y-%m-%d}.json"
        with path.open("w", encoding="utf-8") as f:
            for order in orders:
                f.write(json.dumps(order, separators=(",", ":")) + "\n")


# ---------------------------------------------------------------- customers
CUST_HEADER = ["cust_no", "first_name", "last_name", "email", "signup_dt",
               "province", "segment_cd", "opt_in"]


def make_customers(rng, n_cust):
    """Customers 1..n. The last 25 sign up in the week between the two extracts."""
    base = date(2019, 1, 1)
    span = (date(2026, 9, 20) - base).days
    new_from = n_cust - 25
    customers = []
    for n in range(1, n_cust + 1):
        if n <= new_from:
            signup = base + timedelta(days=rng.randint(0, span))
        else:
            signup = SNAPSHOT_A + timedelta(days=rng.randint(0, 6))
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        if (SNAPSHOT_B - signup).days < 90:
            segment = "NEW"
        else:
            segment = rng.choices(["LYL", "OCC", "LPS"], [30, 45, 25])[0]
        customers.append({
            "cust_no": n,
            "first": first,
            "last": last,
            "email": f"{first.lower()}.{last.lower()}{n}@example.com",
            "signup": signup,
            "province": rng.choices(list(PROVINCES), PROVINCE_WEIGHTS)[0],
            "segment": segment,
            "opt_in": rng.choice(["Y", "N"]),
        })
    return customers


def write_customer_snapshot(rng, path, records, stats):
    """Pipe-delimited flat file with a header row."""
    rows = []
    for c in records:
        email = c["email"]
        signup = fmt_legacy_date(c["signup"])
        last = c["last"]

        r = rng.random()
        if r < 0.03:
            email = "N/A"
        elif r < 0.05:
            email = ""
        if rng.random() < 0.015:
            signup = rng.choice(["01-JAN-00", "99-DEC-99"])
        if rng.random() < 0.01:
            last = "UNKNOWN"

        rows.append([
            f"{c['cust_no']:06d}", c["first"], last, email, signup,
            PROVINCES[c["province"]], c["segment"], c["opt_in"],
        ])

    # Stale duplicate versions of a customer (same ID, different province)
    for _ in range(max(1, int(len(rows) * 0.01))):
        dup = list(rng.choice(rows))
        dup[5] = rng.choice([p for p in PROVINCES.values() if p != dup[5]])
        rows.insert(rng.randint(0, len(rows)), dup)
        stats["customer_duplicate_keys"] += 1

    stats["customer_missing_email"] += sum(1 for r in rows if r[3] in ("N/A", ""))
    stats["customer_bad_signup_date"] += sum(1 for r in rows if r[4] in ("01-JAN-00", "99-DEC-99"))
    stats["customer_placeholder_name"] += sum(1 for r in rows if r[2] == "UNKNOWN")
    stats["customer_rows_total"] += len(rows)
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("|".join(CUST_HEADER) + "\n")
        for row in rows:
            f.write("|".join(row) + "\n")


def write_customers(rng, out_dir, n_cust, stats):
    out_dir.mkdir(parents=True, exist_ok=True)
    customers = make_customers(rng, n_cust)

    snapshot_a = [c for c in customers if c["signup"] < SNAPSHOT_A]

    # Second extract: everyone, plus a few existing customers who moved province.
    snapshot_b = copy.deepcopy(customers)
    for i in rng.sample(range(n_cust - 25), 8):
        snapshot_b[i]["province"] = rng.choice(
            [p for p in PROVINCES if p != snapshot_b[i]["province"]])

    write_customer_snapshot(rng, out_dir / f"customers_{SNAPSHOT_A:%Y-%m-%d}.txt",
                            snapshot_a, stats)
    write_customer_snapshot(rng, out_dir / f"customers_{SNAPSHOT_B:%Y-%m-%d}.txt",
                            snapshot_b, stats)


# ------------------------------------------------------------------ summary
ISSUES = [
    ("pos_duplicate_rows", "POS", "exact duplicate rows (uniqueness check on txn_id)"),
    ("pos_bad_date", "POS", "txn_date is impossible or blank (e.g. 2026-13-45, 31/09/2026)"),
    ("pos_bad_price", "POS", "unit_price is N/A, -1.00 or 0.00 (placeholder / range check)"),
    ("pos_bad_quantity", "POS", "quantity is 0 or negative (range check)"),
    ("pos_missing_category", "POS", "category is UNKNOWN or blank (completeness check)"),
    ("pos_orphan_customer_ids", "POS", "loyalty_id with no matching customer (referential integrity)"),
    ("pos_empty_file", "POS", "completely empty file (rejected by the upload script)"),
    ("pos_header_only_file", "POS", "header row but no data (rejected by the upload script)"),
    ("ecom_duplicate_orders", "E-commerce", "same orderId appears twice (uniqueness check)"),
    ("ecom_bad_timestamp", "E-commerce", "orderTimestamp is impossible, blank or the wrong format"),
    ("ecom_bad_price", "E-commerce", "item priceCents is -1 or 0 (range check)"),
    ("ecom_unknown_province", "E-commerce", "shipping province is UNKNOWN (placeholder)"),
    ("ecom_orphan_customer_ids", "E-commerce", "customerId with no matching customer (referential integrity)"),
    ("customer_duplicate_keys", "Customers", "same cust_no twice with different provinces (uniqueness)"),
    ("customer_missing_email", "Customers", "email is N/A or blank (completeness)"),
    ("customer_bad_signup_date", "Customers", "signup_dt is 01-JAN-00 or 99-DEC-99 (placeholder date)"),
    ("customer_placeholder_name", "Customers", "last_name is UNKNOWN (placeholder)"),
]


def write_summary(out_dir, stats, n_cust, n_days):
    lines = []
    add = lines.append
    add("THORNQUIST MOCK DATA - WHAT WAS GENERATED")
    add("=" * 60)
    add("All data is synthetic. Same seed = identical files.")
    add("")
    add("FILES")
    add(f"  pos/        {n_days} daily CSVs (+ the broken ones listed below)   {stats['pos_rows_total']} data rows in total")
    add(f"  ecommerce/  {n_days} daily JSON-lines files                        {stats['ecom_orders_total']} orders in total")
    add(f"  customers/  2 weekly pipe-delimited extracts                       {stats['customer_rows_total']} rows in total")
    add(f"  Customer IDs that really exist: 1 to {n_cust}. Anything above that is an orphan.")
    add("")
    add("INJECTED PROBLEMS (counted from the finished files)")
    add("  Duplicate rows are exact copies, so a duplicated bad row counts twice for its problem.")
    for key, source, text in ISSUES:
        add(f"  {stats.get(key, 0):>5}  [{source}] {text}")
    add("")
    add("NOT PROBLEMS (normal business cases your rules should NOT reject)")
    add("  - POS rows with a blank loyalty_id are walk-in customers.")
    add("  - E-commerce orders with customerId null and guest true are guest checkouts.")
    add("  - About 12% of e-commerce orders are in USD. That is a conversion job, not an error.")
    add("")
    add("SCHEMA CONFLICTS BUILT IN (for your reconciliation table)")
    add("  1. Dates:      POS MM/DD/YYYY | e-commerce ISO 8601 with timezone | customers DD-MON-YY")
    add("  2. Customer ID: POS 'L000123' | e-commerce integer 123 | customers '000123'")
    add("  3. Categories: POS 'Footwear' | e-commerce 'Shoes > Hiking Boots' (different taxonomy)")
    add("  4. SKU:        POS 'TH-1001' | e-commerce 'TH1001'")
    add("  5. Price:      POS dollars as text | e-commerce integer cents, some orders in USD")
    add("  6. Province:   POS and e-commerce 'ON' | customers 'Ontario'")
    add("  7. Naming:     POS snake_case (txn_id) | e-commerce camelCase (orderId)")
    add("  8. Structure:  POS flat rows | e-commerce nested (customer, shippingAddress, items[])")
    add("  9. Format:     CSV with header | JSON lines | pipe-delimited text file")
    add("")
    (out_dir / "_INJECTED_ISSUES.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


# --------------------------------------------------------------------- main
def parse_args():
    p = argparse.ArgumentParser(description="Generate fake Thornquist Outfitters source data.")
    p.add_argument("--out", default="mock_data", help="Output folder (default: mock_data)")
    p.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    p.add_argument("--days", type=int, default=14, help="Days of POS/e-commerce data (default: 14)")
    p.add_argument("--customers", type=int, default=500, help="Number of customers (default: 500)")
    p.add_argument("--no-bad-files", action="store_true",
                   help="Skip the empty and header-only POS files")
    return p.parse_args()


def main():
    args = parse_args()
    if args.customers < 100:
        raise SystemExit("--customers must be at least 100")

    out = Path(args.out)
    days = [START + timedelta(days=i) for i in range(args.days)]
    stats = Counter()

    # A separate random generator per source, so changing one never shifts the others.
    stores = make_stores(random.Random(args.seed))
    write_pos(random.Random(args.seed + 1), out / "pos", days, stores,
              args.customers, stats, not args.no_bad_files)
    write_ecommerce(random.Random(args.seed + 2), out / "ecommerce", days,
                    args.customers, stats)
    write_customers(random.Random(args.seed + 3), out / "customers", args.customers, stats)
    write_summary(out, stats, args.customers, args.days)

    print((out / "_INJECTED_ISSUES.txt").read_text(encoding="utf-8"))
    print(f"Done. Files are in: {out.resolve()}")


if __name__ == "__main__":
    main()
