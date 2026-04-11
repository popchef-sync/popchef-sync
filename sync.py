import os
import requests
from datetime import datetime, date, timedelta
import re

# ─── CONFIG ───────────────────────────────────────────────
METABASE_URL = os.environ["METABASE_URL"].strip()
METABASE_EMAIL = os.environ["METABASE_EMAIL"].strip()
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"].strip()
SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()

FRENCH_MONTHS = {
    "janvier":1,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12
}
FRENCH_DAYS = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]

# Période de synchronisation : 90 derniers jours
END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=90)
DATE_RANGE = f"{START_DATE}~{END_DATE}"

print(f"📅 Période : {START_DATE} → {END_DATE}")

# ─── HELPERS ──────────────────────────────────────────────

def parse_french_date(s):
    if not s:
        return None
    s = str(s).strip().strip('"')

    # Format A: "1 janvier, 2026, 13:01"
    m = re.match(r"(\d+)\s+(\w+),\s*(\d{4}),?\s*(\d{2}):(\d{2})", s)
    if m:
        day, month_str, year, hour, minute = m.groups()
        month = FRENCH_MONTHS.get(month_str.lower())
        if month:
            return datetime(int(year), month, int(day), int(hour), int(minute))

    # Format B: DD/MM/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        day, month, year = m.groups()
        return datetime(int(year), int(month), int(day))

    # Format C: "1 janvier, 2026"
    m = re.match(r"(\d+)\s+(\w+),\s*(\d{4})", s)
    if m:
        day, month_str, year = m.groups()
        month = FRENCH_MONTHS.get(month_str.lower())
        if month:
            return datetime(int(year), month, int(day))

    print(f"  ⚠️  Date non reconnue : {s}")
    return None

def parse_number(s):
    if s is None or s == "":
        return None
    try:
        return float(str(s).replace(",", "."))
    except:
        return None

def iso_week(dt):
    if not dt:
        return None
    return dt.isocalendar()[1]

def french_day(dt):
    if not dt:
        return None
    return FRENCH_DAYS[dt.weekday()]

def category2(product_name, category):
    name = (product_name or "").lower()
    if any(x in name for x in ["cookie", "yaourt brassé", "mousse au chocolat"]):
        return "Permanent"
    return category or ""

def epd_flag(cat2):
    return "EPD" if cat2 in ["Starter","Snacking","Dish","Dessert"] else ""

def supabase_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

def upsert(table, rows):
    if not rows:
        print(f"  Aucune ligne pour {table}")
        return
    batch_size = 200
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=supabase_headers(),
            json=batch
        )
        if r.status_code not in [200, 201]:
            print(f"  ❌ Erreur Supabase {table}: {r.status_code} {r.text[:300]}")
            return
        total += len(batch)
    print(f"  ✅ {total} lignes insérées dans {table}")

def log_import(data_type, row_count, status="success"):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/import_logs",
        headers=supabase_headers(),
        json=[{
            "data_type": data_type,
            "imported_at": datetime.utcnow().isoformat(),
            "row_count": row_count,
            "status": status
        }]
    )
    if r.status_code not in [200, 201]:
        print(f"  ⚠️  Log non enregistré: {r.status_code} {r.text[:100]}")

# ─── METABASE ─────────────────────────────────────────────

def metabase_token():
    print("🔐 Connexion à Metabase...")
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD},
        timeout=30
    )
    r.raise_for_status()
    token = r.json()["id"]
    print("  ✅ Connecté")
    return token

def fetch_question(token, question_id, extra_params=None):
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    params = [
        {"type": "date/range", "target": ["variable", ["template-tag", "Date"]], "value": DATE_RANGE},
        {"type": "date/range", "target": ["variable", ["template-tag", "DATE"]], "value": DATE_RANGE},
        {"type": "date/range", "target": ["variable", ["template-tag", "DATE_RANGE"]], "value": DATE_RANGE},
    ]
    if extra_params:
        params += extra_params

    r = requests.post(
        f"{METABASE_URL}/api/card/{question_id}/query/json",
        headers=headers,
        json={"parameters": params},
        timeout=300
    )
    if r.status_code != 200:
        print(f"  ❌ Erreur Metabase {question_id}: {r.status_code} {r.text[:200]}")
        return []
    try:
        return r.json()
    except Exception as e:
        print(f"  ❌ Erreur parsing JSON question {question_id}: {e}")
        return []

# ─── TRANSFORMATIONS ──────────────────────────────────────

def transform_dispatched(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        cat = r.get("catégorie") or r.get("categorie") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Quantités dispatchées") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({
            "date": dt.date().isoformat() if dt else None,
            "site": r.get("Emplacement") or r.get("emplacement"),
            "category": cat,
            "product_name": pname,
            "quantity": qty,
            "unit_cost": cost,
            "week_number": iso_week(dt),
            "category2": cat2,
            "epd": epd_flag(cat2),
            "value": round((qty or 0) * (cost or 0), 4),
            "day_name": french_day(dt),
        })
    return out

def transform_consumed(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        cat = r.get("catégorie") or r.get("categorie") or ""
        pname = r.get("Nom du produit") or r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Nombre de consommations") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({
            "week_number": int(parse_number(r.get("n° semaine") or iso_week(dt) or 0) or 0),
            "date": dt.date().isoformat() if dt else None,
            "site": r.get("Emplacement") or r.get("emplacement"),
            "product_name": pname,
            "category": cat,
            "protein_type": r.get("Type") or r.get("type"),
            "quantity": qty,
            "unit_cost": cost,
            "category2": cat2,
            "epd": epd_flag(cat2),
            "value": round((qty or 0) * (cost or 0), 4),
            "day_name": french_day(dt),
        })
    return out

def transform_delivered(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("date de livraison"))
        dlc = parse_french_date(r.get("DLC") or r.get("dlc"))
        cat = r.get("catégorie du produit") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit livré détecté") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({
            "delivery_date": dt.date().isoformat() if dt else None,
            "site_name": r.get("nom de l'emplacement"),
            "fridge_name": r.get("nom du frigo"),
            "product_category": cat,
            "unit_cost": cost,
            "product_name": pname,
            "protein_type": r.get("Type") or r.get("type"),
            "dlc_date": dlc.date().isoformat() if dlc else None,
            "quantity_detected": qty,
            "category2": cat2,
            "epd": epd_flag(cat2),
            "value": round((qty or 0) * (cost or 0), 4),
            "week_number": iso_week(dt),
            "day_name": french_day(dt),
        })
    return out

def transform_stock(rows):
    out = []
    for r in rows:
        ts = parse_french_date(r.get("instant_t"))
        dlc = parse_french_date(r.get("dlc") or r.get("DLC"))
        cat = r.get("Catégorie du produit") or ""
        pname = r.get("Nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit") or 0) or 0)
        out.append({
            "timestamp": ts.isoformat() if ts else None,
            "site": r.get("Nom de l'emplacement"),
            "category": cat,
            "product_name": pname,
            "quantity": qty,
            "dlc_date": dlc.date().isoformat() if dlc else None,
            "category2": cat2,
            "epd": epd_flag(cat2),
            "week_number": iso_week(ts),
            "day_name": french_day(ts),
            "dlc_day_name": french_day(dlc),
        })
    return out

def transform_proofs(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        out.append({
            "date": dt.date().isoformat() if dt else None,
            "site": r.get("Emplacement") or r.get("emplacement"),
            "preparation_url": r.get("Préparation") or r.get("preparation"),
            "delivery_url": r.get("Livraison") or r.get("livraison"),
            "week_number": iso_week(dt),
            "day_name": french_day(dt),
        })
    return out

# ─── MAIN ─────────────────────────────────────────────────

def main():
    print("🚀 Démarrage synchronisation Metabase → Supabase")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n")

    token = metabase_token()

    # Dispatché
    print("📦 Dispatché...")
    try:
        rows = fetch_question(token, 1684)
        print(f"  {len(rows)} lignes reçues")
        data = transform_dispatched(rows)
        upsert("dispatched", data)
        log_import("dispatched", len(data))
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("dispatched", 0, "error")

    # Consommé
    print("🍽️  Consommé...")
    try:
        rows = fetch_question(token, 1683)
        print(f"  {len(rows)} lignes reçues")
        data = transform_consumed(rows)
        upsert("consumed", data)
        log_import("consumed", len(data))
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("consumed", 0, "error")

    # Livré
    print("🚚 Livré...")
    try:
        rows = fetch_question(token, 1687)
        print(f"  {len(rows)} lignes reçues")
        data = transform_delivered(rows)
        upsert("delivered", data)
        log_import("delivered", len(data))
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("delivered", 0, "error")

    # Stock 12h30
    print("📊 Stock 12h30...")
    try:
        rows = fetch_question(token, 1682, extra_params=[
            {"type": "category", "target": ["variable", ["template-tag", "HEURE"]], "value": "12:31"}
        ])
        print(f"  {len(rows)} lignes reçues")
        data = transform_stock(rows)
        upsert("stock_12h30", data)
        log_import("stock_12h30", len(data))
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("stock_12h30", 0, "error")

    # Stock 6h
    print("📊 Stock 6h...")
    try:
        rows = fetch_question(token, 1682, extra_params=[
            {"type": "category", "target": ["variable", ["template-tag", "HEURE"]], "value": "06:01"}
        ])
        print(f"  {len(rows)} lignes reçues")
        data = transform_stock(rows)
        upsert("stock_6h", data)
        log_import("stock_6h", len(data))
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("stock_6h", 0, "error")

    # Preuves de livraison
    print("📸 Preuves livraison...")
    try:
        rows = fetch_question(token, 1673)
        print(f"  {len(rows)} lignes reçues")
        data = transform_proofs(rows)
        upsert("delivery_proofs", data)
        log_import("delivery_proofs", len(data))
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("delivery_proofs", 0, "error")

    print("\n✅ Synchronisation terminée !")

if __name__ == "__main__":
    main()
