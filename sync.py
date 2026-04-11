import os
import requests
import json
from datetime import datetime, date
import re

# ─── CONFIG ───────────────────────────────────────────────
METABASE_URL = os.environ["METABASE_URL"]
METABASE_EMAIL = os.environ["METABASE_EMAIL"]
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

QUESTIONS = {
    "dispatched":      1684,
    "consumed":        1683,
    "delivery_proofs": 1673,
    "delivered":       1687,
    "stock_12h30":     1682,
    "stock_6h":        1682,
}

FRENCH_MONTHS = {
    "janvier":1,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12
}

FRENCH_DAYS = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]

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
    return float(str(s).replace(",", "."))

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
        print(f"  Aucune ligne à insérer dans {table}")
        return
    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=supabase_headers(),
            json=batch
        )
        if r.status_code not in [200, 201]:
            print(f"  ❌ Erreur Supabase {table}: {r.status_code} {r.text[:200]}")
        else:
            total += len(batch)
    print(f"  ✅ {total} lignes insérées dans {table}")

def log_import(data_type, row_count, status="success"):
    upsert("import_logs", [{
        "data_type": data_type,
        "imported_at": datetime.utcnow().isoformat(),
        "row_count": row_count,
        "status": status
    }])

# ─── METABASE AUTH ────────────────────────────────────────

def metabase_token():
    print("🔐 Connexion à Metabase...")
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD}
    )
    r.raise_for_status()
    token = r.json()["id"]
    print("  ✅ Connecté à Metabase")
    return token

def fetch_question(token, question_id, params=None):
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    body = {"parameters": params or []}
    r = requests.post(
        f"{METABASE_URL}/api/card/{question_id}/query/json",
        headers=headers,
        json=body,
        timeout=120
    )
    r.raise_for_status()
    return r.json()

# ─── TRANSFORMATIONS ──────────────────────────────────────

def transform_dispatched(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        cat = r.get("catégorie") or r.get("categorie") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Quantités dispatchées") or r.get("quantites_dispatches") or 0) or 0)
        cost = parse_number(r.get("PA Produit") or r.get("pa_produit"))
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
        cost = parse_number(r.get("PA Produit") or r.get("pa_produit"))
        out.append({
            "week_number": int(r.get("n° semaine") or r.get("semaine") or iso_week(dt) or 0),
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
        cat = r.get("catégorie du produit") or r.get("categorie_du_produit") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit livré détecté") or 0) or 0)
        cost = parse_number(r.get("PA Produit") or r.get("pa_produit"))
        out.append({
            "delivery_date": dt.date().isoformat() if dt else None,
            "site_name": r.get("nom de l'emplacement") or r.get("nom_emplacement"),
            "fridge_name": r.get("nom du frigo") or r.get("nom_frigo"),
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

def transform_stock(rows, label):
    out = []
    for r in rows:
        ts = parse_french_date(r.get("instant_t"))
        dlc = parse_french_date(r.get("dlc") or r.get("DLC"))
        cat = r.get("Catégorie du produit") or r.get("categorie_du_produit") or ""
        pname = r.get("Nom du produit") or r.get("nom_du_produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit") or r.get("nombre_de_produit") or 0) or 0)
        out.append({
            "timestamp": ts.isoformat() if ts else None,
            "site": r.get("Nom de l'emplacement") or r.get("nom_emplacement"),
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
    print("🚀 Démarrage de la synchronisation Metabase → Supabase")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print()

    token = metabase_token()

    # ── Dispatché ──
    print("📦 Récupération Dispatché...")
    try:
        rows = fetch_question(token, QUESTIONS["dispatched"])
        data = transform_dispatched(rows)
        upsert("dispatched", data)
        log_import("dispatched", len(data))
    except Exception as e:
        print(f"  ❌ Erreur Dispatché: {e}")
        log_import("dispatched", 0, "error")

    # ── Consommé ──
    print("🍽️  Récupération Consommé...")
    try:
        rows = fetch_question(token, QUESTIONS["consumed"])
        data = transform_consumed(rows)
        upsert("consumed", data)
        log_import("consumed", len(data))
    except Exception as e:
        print(f"  ❌ Erreur Consommé: {e}")
        log_import("consumed", 0, "error")

    # ── Livré ──
    print("🚚 Récupération Livré...")
    try:
        rows = fetch_question(token, QUESTIONS["delivered"])
        data = transform_delivered(rows)
        upsert("delivered", data)
        log_import("delivered", len(data))
    except Exception as e:
        print(f"  ❌ Erreur Livré: {e}")
        log_import("delivered", 0, "error")

    # ── Stock 12h30 ──
    print("📊 Récupération Stock 12h30...")
    try:
        rows = fetch_question(token, QUESTIONS["stock_12h30"], params=[
            {"type": "category", "target": ["variable", ["template-tag", "HEURE"]], "value": "12:31"},
        ])
        data = transform_stock(rows, "12h30")
        upsert("stock_12h30", data)
        log_import("stock_12h30", len(data))
    except Exception as e:
        print(f"  ❌ Erreur Stock 12h30: {e}")
        log_import("stock_12h30", 0, "error")

    # ── Stock 6h ──
    print("📊 Récupération Stock 6h...")
    try:
        rows = fetch_question(token, QUESTIONS["stock_6h"], params=[
            {"type": "category", "target": ["variable", ["template-tag", "HEURE"]], "value": "06:01"},
        ])
        data = transform_stock(rows, "6h")
        upsert("stock_6h", data)
        log_import("stock_6h", len(data))
    except Exception as e:
        print(f"  ❌ Erreur Stock 6h: {e}")
        log_import("stock_6h", 0, "error")

    # ── Preuves de livraison ──
    print("📸 Récupération Preuves de livraison...")
    try:
        rows = fetch_question(token, QUESTIONS["delivery_proofs"])
        data = transform_proofs(rows)
        upsert("delivery_proofs", data)
        log_import("delivery_proofs", len(data))
    except Exception as e:
        print(f"  ❌ Erreur Preuves: {e}")
        log_import("delivery_proofs", 0, "error")

    print()
    print("✅ Synchronisation terminée !")

if __name__ == "__main__":
    main()
