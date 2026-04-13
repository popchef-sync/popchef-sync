import os
import requests
from datetime import datetime, date, timedelta
import re
import json

METABASE_URL = os.environ["METABASE_URL"].strip()
METABASE_EMAIL = os.environ["METABASE_EMAIL"].strip()
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"].strip()
SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()

# FULL_IMPORT = True  → vide tout et réimporte depuis le 1er jan 2026
# FULL_IMPORT = False → ajoute seulement les 8 derniers jours (mode nuit auto)
FULL_IMPORT = True
FULL_START = date(2026, 1, 1)

END_DATE = date.today()
START_DATE = FULL_START if FULL_IMPORT else (END_DATE - timedelta(days=8))

print(f"📅 Mode : {'COMPLET depuis ' + str(FULL_START) if FULL_IMPORT else 'INCRÉMENTAL 8 jours'}")
print(f"📅 Période : {START_DATE} → {END_DATE}")

FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
}
FRENCH_DAYS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]

def parse_french_date(s):
    if not s: return None
    s = str(s).strip().strip('"')
    m = re.match(r"(\d+)\s+(\w+),\s*(\d{4}),?\s*(\d{2}):(\d{2})", s)
    if m:
        day, ms, year, hour, minute = m.groups()
        mo = FRENCH_MONTHS.get(ms.lower())
        if mo: return datetime(int(year), mo, int(day), int(hour), int(minute))
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        day, month, year = m.groups()
        return datetime(int(year), int(month), int(day))
    m = re.match(r"(\d+)\s+(\w+),\s*(\d{4})", s)
    if m:
        day, ms, year = m.groups()
        mo = FRENCH_MONTHS.get(ms.lower())
        if mo: return datetime(int(year), mo, int(day))
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        year, month, day = m.groups()
        return datetime(int(year), int(month), int(day))
    return None

def parse_number(s):
    if s is None or s == "": return None
    try: return float(str(s).replace(",", "."))
    except: return None

def iso_week(dt):
    if not dt: return None
    d = dt if isinstance(dt, datetime) else datetime.combine(dt, datetime.min.time())
    tmp = d + timedelta(days=4 - (d.isoweekday() or 7))
    y = datetime(tmp.year, 1, 1)
    return int((tmp - y).days / 7) + 1

def french_day(dt):
    if not dt: return None
    d = dt if isinstance(dt, datetime) else datetime.combine(dt, datetime.min.time())
    return FRENCH_DAYS[d.weekday()]

def category2(product_name, category):
    name = (product_name or "").lower()
    if any(x in name for x in ["cookie", "yaourt brassé", "mousse au chocolat"]):
        return "Permanent"
    return category or ""

def epd_flag(cat2):
    return "EPD" if cat2 in ["Starter", "Snacking", "Dish", "Dessert"] else ""

def supa_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

def truncate_all():
    tables = ["dispatched", "delivered", "consumed", "stock_12h30", "stock_6h", "delivery_proofs", "import_logs"]
    for t in tables:
        r = requests.delete(f"{SUPABASE_URL}/rest/v1/{t}?id=gte.0", headers=supa_headers())
        print(f"  🗑️  {t} vidée (status: {r.status_code})")

def delete_since(table, date_col, since_date):
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?{date_col}=gte.{since_date}", headers=supa_headers())
    print(f"  🗑️  {table} depuis {since_date} (status: {r.status_code})")

def insert(table, rows):
    if not rows:
        print(f"  Aucune ligne pour {table}")
        return 0
    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=supa_headers(), json=batch)
        if r.status_code not in [200, 201]:
            print(f"  ❌ Erreur Supabase {table}: {r.status_code} {r.text[:200]}")
            return total
        total += len(batch)
        if total % 10000 == 0:
            print(f"  ... {total}/{len(rows)}")
    print(f"  ✅ {total} lignes insérées dans {table}")
    return total

def log_import(data_type, row_count, status="success"):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/import_logs",
        headers=supa_headers(),
        json=[{"data_type": data_type, "imported_at": datetime.utcnow().isoformat(), "row_count": row_count, "status": status}]
    )

def metabase_token():
    print("🔐 Connexion à Metabase...")
    r = requests.post(f"{METABASE_URL}/api/session", json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD}, timeout=30)
    r.raise_for_status()
    print("  ✅ Connecté")
    return r.json()["id"]

def fetch_question(token, question_id, params=None):
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    r = requests.post(f"{METABASE_URL}/api/card/{question_id}/query/json", headers=headers, json={"parameters": params or []}, timeout=600)
    if r.status_code != 200:
        print(f"  ❌ Erreur Metabase {question_id}: {r.status_code} {r.text[:400]}")
        return []
    try:
        data = r.json()
        if isinstance(data, list): return data
        print(f"  ❌ Réponse inattendue: {str(data)[:300]}")
        return []
    except json.JSONDecodeError:
        print(f"  ⚠️  JSON partiel détecté, récupération partielle...")
        try:
            text = r.text
            last = text.rfind('},')
            if last > 0:
                data = json.loads(text[:last + 1] + ']')
                print(f"  ⚠️  {len(data)} lignes récupérées (JSON partiel)")
                return data
        except: pass
        return []

def param_date_range(tag_name, start, end):
    return {"type": "date/range", "target": ["dimension", ["template-tag", tag_name]], "value": f"{start}~{end}"}

def params_stock(heure, start, end):
    return [
        {"type": "date/single", "target": ["dimension", ["template-tag", "DATE_RANGE.start"]], "value": str(start)},
        {"type": "date/single", "target": ["dimension", ["template-tag", "DATE_RANGE.end"]], "value": str(end)},
        {"type": "category", "target": ["variable", ["template-tag", "HEURE"]], "value": heure}
    ]

def transform_dispatched(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        if not dt or dt.date() < START_DATE: continue
        cat = r.get("catégorie") or r.get("categorie") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Quantités dispatchées") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({"date": dt.date().isoformat(), "site": r.get("Emplacement") or r.get("emplacement"), "category": cat, "product_name": pname, "quantity": qty, "unit_cost": cost, "week_number": iso_week(dt), "year": dt.year, "category2": cat2, "epd": epd_flag(cat2), "value": round((qty or 0) * (cost or 0), 4), "day_name": french_day(dt)})
    return out

def transform_consumed(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        if not dt or dt.date() < START_DATE: continue
        cat = r.get("catégorie") or r.get("categorie") or ""
        pname = r.get("Nom du produit") or r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Nombre de consommations") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({"week_number": int(parse_number(r.get("n° semaine") or iso_week(dt) or 0) or 0), "year": dt.year, "date": dt.date().isoformat(), "site": r.get("Emplacement") or r.get("emplacement"), "product_name": pname, "category": cat, "protein_type": r.get("Type") or r.get("type"), "quantity": qty, "unit_cost": cost, "category2": cat2, "epd": epd_flag(cat2), "value": round((qty or 0) * (cost or 0), 4), "day_name": french_day(dt)})
    return out

def transform_delivered(rows):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("date de livraison"))
        if not dt or dt.date() < START_DATE: continue
        dlc = parse_french_date(r.get("DLC") or r.get("dlc"))
        cat = r.get("catégorie du produit") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit livré détecté") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({"delivery_date": dt.date().isoformat(), "site_name": r.get("nom de l'emplacement"), "fridge_name": r.get("nom du frigo"), "product_category": cat, "unit_cost": cost, "product_name": pname, "protein_type": r.get("Type") or r.get("type"), "dlc_date": dlc.date().isoformat() if dlc else None, "quantity_detected": qty, "category2": cat2, "epd": epd_flag(cat2), "value": round((qty or 0) * (cost or 0), 4), "week_number": iso_week(dt), "year": dt.year, "day_name": french_day(dt)})
    return out

def transform_stock(rows):
    out = []
    for r in rows:
        ts = parse_french_date(r.get("instant_T") or r.get("instant_t"))
        if not ts or ts.date() < START_DATE: continue
        dlc = parse_french_date(r.get("dlc") or r.get("DLC"))
        cat = r.get("Catégorie du produit") or ""
        pname = r.get("Nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit") or 0) or 0)
        out.append({"timestamp": ts.isoformat(), "site": r.get("Nom de l'emplacement"), "category": cat, "product_name": pname, "quantity": qty, "dlc_date": dlc.date().isoformat() if dlc else None, "category2": cat2, "epd": epd_flag(cat2), "week_number": iso_week(ts), "year": ts.year, "day_name": french_day(ts), "dlc_day_name": french_day(dlc)})
    return out

def transform_proofs(rows):
    out, seen = [], set()
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        if not dt or dt.date() < START_DATE: continue
        site = r.get("Emplacement") or r.get("emplacement")
        key = f"{dt.date()}_{site}"
        if key in seen: continue
        seen.add(key)
        out.append({"date": dt.date().isoformat(), "site": site, "preparation_url": r.get("Préparation") or r.get("preparation"), "delivery_url": r.get("Livraison") or r.get("livraison"), "week_number": iso_week(dt), "year": dt.year, "day_name": french_day(dt)})
    return out

def main():
    print("🚀 Démarrage synchronisation Metabase → Supabase")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n")

    token = metabase_token()

    if FULL_IMPORT:
        print("🗑️  Import complet — vidage de toutes les tables...")
        truncate_all()
    else:
        print(f"♻️  Import incrémental — suppression depuis {START_DATE}...")
        delete_since("dispatched", "date", START_DATE)
        delete_since("delivered", "delivery_date", START_DATE)
        delete_since("consumed", "date", START_DATE)
        delete_since("stock_12h30", "timestamp", START_DATE)
        delete_since("stock_6h", "timestamp", START_DATE)
        delete_since("delivery_proofs", "date", START_DATE)
    print()

    # Dispatché
    print("📦 Dispatché (1684)...")
    try:
        rows = fetch_question(token, 1684, [param_date_range("Date", START_DATE, END_DATE)])
        print(f"  {len(rows)} lignes reçues")
        n = insert("dispatched", transform_dispatched(rows))
        log_import("dispatched", n)
    except Exception as e:
        print(f"  ❌ {e}"); log_import("dispatched", 0, "error")

    # Consommé
    print("🍽️  Consommé (1683)...")
    try:
        rows = fetch_question(token, 1683, [param_date_range("DATE", START_DATE, END_DATE)])
        print(f"  {len(rows)} lignes reçues")
        n = insert("consumed", transform_consumed(rows))
        log_import("consumed", n)
    except Exception as e:
        print(f"  ❌ {e}"); log_import("consumed", 0, "error")

    # Livré
    print("🚚 Livré (1687)...")
    try:
        rows = fetch_question(token, 1687, [param_date_range("DATE", START_DATE, END_DATE)])
        print(f"  {len(rows)} lignes reçues")
        n = insert("delivered", transform_delivered(rows))
        log_import("delivered", n)
    except Exception as e:
        print(f"  ❌ {e}"); log_import("delivered", 0, "error")

    # Stock 12h30
    print("📊 Stock 12h30 (1682)...")
    try:
        rows = fetch_question(token, 1682, params_stock("12:31", START_DATE, END_DATE))
        print(f"  {len(rows)} lignes reçues")
        n = insert("stock_12h30", transform_stock(rows))
        log_import("stock_12h30", n)
    except Exception as e:
        print(f"  ❌ {e}"); log_import("stock_12h30", 0, "error")

    # Stock 6h
    print("📊 Stock 6h (1682)...")
    try:
        rows = fetch_question(token, 1682, params_stock("06:01", START_DATE, END_DATE))
        print(f"  {len(rows)} lignes reçues")
        n = insert("stock_6h", transform_stock(rows))
        log_import("stock_6h", n)
    except Exception as e:
        print(f"  ❌ {e}"); log_import("stock_6h", 0, "error")

    # Preuves
    print("📸 Preuves livraison (1673)...")
    try:
        rows = fetch_question(token, 1673, [param_date_range("Date", START_DATE, END_DATE)])
        print(f"  {len(rows)} lignes reçues")
        n = insert("delivery_proofs", transform_proofs(rows))
        log_import("delivery_proofs", n)
    except Exception as e:
        print(f"  ❌ {e}"); log_import("delivery_proofs", 0, "error")

    print("\n✅ Synchronisation terminée !")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

if __name__ == "__main__":
    main()
