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

# ─────────────────────────────────────────────────────────
# FIRST_RUN = True  → importe TOUT depuis IMPORT_FROM
# FIRST_RUN = False → met à jour mois précédent + mois en cours
# ─────────────────────────────────────────────────────────
FIRST_RUN = False
IMPORT_FROM = date(2025, 1, 1)

TODAY = date.today()

FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
}
FRENCH_DAYS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]

# ─── DATES ────────────────────────────────────────────────

def month_chunks(start, end):
    chunks = []
    s = date(start.year, start.month, 1)
    while s <= end:
        if s.month == 12:
            e = date(s.year + 1, 1, 1)
        else:
            e = date(s.year, s.month + 1, 1)
        e = min(e - timedelta(days=1), end)
        chunks.append((s, e))
        s = e + timedelta(days=1)
    return chunks

# ─── PARSE ────────────────────────────────────────────────

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

# ─── SUPABASE ─────────────────────────────────────────────

def supa_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

def delete_month(table, date_col, year, month):
    if month == 12:
        end_d = date(year + 1, 1, 1)
    else:
        end_d = date(year, month + 1, 1)
    start_d = date(year, month, 1)
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}?{date_col}=gte.{start_d}&{date_col}=lt.{end_d}",
        headers=supa_headers()
    )

def truncate_table(table):
    requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?id=gte.0", headers=supa_headers())
    print(f"  🗑️  {table} vidée")

def insert(table, rows):
    if not rows: return 0
    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=supa_headers(), json=batch)
        if r.status_code not in [200, 201]:
            print(f"    ❌ Erreur insert {table}: {r.status_code} {r.text[:150]}")
            return total
        total += len(batch)
    return total

def log_import(data_type, year, month, row_count, status="success"):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/import_logs",
        headers=supa_headers(),
        json=[{
            "data_type": data_type, "year": year, "month": month,
            "imported_at": datetime.utcnow().isoformat(),
            "row_count": row_count, "status": status
        }]
    )

def already_imported(data_type, year, month):
    if FIRST_RUN: return False
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/import_logs?data_type=eq.{data_type}&year=eq.{year}&month=eq.{month}&status=eq.success&limit=1",
        headers=supa_headers()
    )
    try: return len(r.json()) > 0
    except: return False

# ─── METABASE ─────────────────────────────────────────────

def metabase_token():
    print("🔐 Connexion à Metabase...")
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD},
        timeout=30
    )
    r.raise_for_status()
    print("  ✅ Connecté\n")
    return r.json()["id"]

def fetch_question(token, question_id, params):
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    r = requests.post(
        f"{METABASE_URL}/api/card/{question_id}/query/json",
        headers=headers,
        json={"parameters": params},
        timeout=300
    )
    if r.status_code != 200:
        print(f"    ❌ Erreur Metabase {question_id}: {r.status_code} {r.text[:300]}")
        return []
    try:
        data = r.json()
        if isinstance(data, list): return data
        print(f"    ❌ Réponse inattendue: {str(data)[:200]}")
        return []
    except json.JSONDecodeError:
        try:
            last = r.text.rfind('},')
            if last > 0:
                data = json.loads(r.text[:last + 1] + ']')
                print(f"    ⚠️  JSON partiel: {len(data)} lignes")
                return data
        except: pass
        return []

# ─── PARAMÈTRES ───────────────────────────────────────────

def p_range(tag, s, e):
    """Paramètre date/range pour les questions avec {{ Date }} ou {{ DATE }}"""
    return {"type": "date/range", "target": ["dimension", ["template-tag", tag]], "value": f"{s}~{e}"}

def p_stock(heure, start, end):
    """
    Question 1682 — stock frigo openit
    DATE_RANGE.start et DATE_RANGE.end = date/single (calendrier)
    HEURE = category (texte)
    EMPLACEMENT = Field Filter optionnel — NE PAS l'inclure = tous les emplacements
    """
    return [
        {"type": "date/single", "target": ["dimension", ["template-tag", "DATE_RANGE.start"]], "value": str(start)},
        {"type": "date/single", "target": ["dimension", ["template-tag", "DATE_RANGE.end"]], "value": str(end)},
        {"type": "category", "target": ["variable", ["template-tag", "HEURE"]], "value": heure},
    ]

# ─── TRANSFORMATIONS ──────────────────────────────────────

def t_dispatched(rows, s, e):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        if not dt or not (s <= dt.date() <= e): continue
        cat = r.get("catégorie") or r.get("categorie") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Quantités dispatchées") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({
            "date": dt.date().isoformat(), "year": dt.year, "week_number": iso_week(dt),
            "site": r.get("Emplacement") or r.get("emplacement"),
            "category": cat, "product_name": pname, "quantity": qty, "unit_cost": cost,
            "value": round((qty or 0) * (cost or 0), 4),
            "category2": cat2, "epd": epd_flag(cat2), "day_name": french_day(dt),
        })
    return out

def t_consumed(rows, s, e):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        if not dt or not (s <= dt.date() <= e): continue
        cat = r.get("catégorie") or r.get("categorie") or ""
        pname = r.get("Nom du produit") or r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Nombre de consommations") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({
            "date": dt.date().isoformat(), "year": dt.year,
            "week_number": int(parse_number(r.get("n° semaine") or iso_week(dt) or 0) or 0),
            "site": r.get("Emplacement") or r.get("emplacement"),
            "product_name": pname, "category": cat,
            "protein_type": r.get("Type") or r.get("type"),
            "quantity": qty, "unit_cost": cost,
            "value": round((qty or 0) * (cost or 0), 4),
            "category2": cat2, "epd": epd_flag(cat2), "day_name": french_day(dt),
        })
    return out

def t_delivered(rows, s, e):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("date de livraison"))
        if not dt or not (s <= dt.date() <= e): continue
        dlc = parse_french_date(r.get("DLC") or r.get("dlc"))
        cat = r.get("catégorie du produit") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit livré détecté") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({
            "delivery_date": dt.date().isoformat(), "year": dt.year, "week_number": iso_week(dt),
            "site_name": r.get("nom de l'emplacement"),
            "fridge_name": r.get("nom du frigo"),
            "product_category": cat, "product_name": pname,
            "protein_type": r.get("Type") or r.get("type"),
            "unit_cost": cost, "quantity_detected": qty,
            "value": round((qty or 0) * (cost or 0), 4),
            "dlc_date": dlc.date().isoformat() if dlc else None,
            "category2": cat2, "epd": epd_flag(cat2), "day_name": french_day(dt),
        })
    return out

def t_stock(rows, s, e):
    out = []
    for r in rows:
        ts = parse_french_date(r.get("instant_T") or r.get("instant_t"))
        if not ts or not (s <= ts.date() <= e): continue
        dlc = parse_french_date(r.get("dlc") or r.get("DLC"))
        cat = r.get("Catégorie du produit") or ""
        pname = r.get("Nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit") or 0) or 0)
        out.append({
            "timestamp": ts.isoformat(), "year": ts.year, "week_number": iso_week(ts),
            "site": r.get("Nom de l'emplacement"),
            "category": cat, "product_name": pname, "quantity": qty,
            "dlc_date": dlc.date().isoformat() if dlc else None,
            "category2": cat2, "epd": epd_flag(cat2),
            "day_name": french_day(ts), "dlc_day_name": french_day(dlc),
        })
    return out

def t_proofs(rows, s, e):
    out, seen = [], set()
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        if not dt or not (s <= dt.date() <= e): continue
        site = r.get("Emplacement") or r.get("emplacement")
        key = f"{dt.date()}_{site}"
        if key in seen: continue
        seen.add(key)
        out.append({
            "date": dt.date().isoformat(), "year": dt.year, "week_number": iso_week(dt),
            "site": site,
            "preparation_url": r.get("Préparation") or r.get("preparation"),
            "delivery_url": r.get("Livraison") or r.get("livraison"),
            "day_name": french_day(dt),
        })
    return out

# ─── SYNC PAR MOIS ────────────────────────────────────────

def sync_month(token, year, month, start, end):
    print(f"\n  📅 {year}-{month:02d} ({start} → {end})")

    # Dispatché
    if not already_imported("dispatched", year, month):
        try:
            rows = fetch_question(token, 1684, [p_range("Date", start, end)])
            data = t_dispatched(rows, start, end)
            delete_month("dispatched", "date", year, month)
            n = insert("dispatched", data)
            print(f"    📦 Dispatché: {n} lignes")
            log_import("dispatched", year, month, n)
        except Exception as ex:
            print(f"    ❌ Dispatché: {ex}")
            log_import("dispatched", year, month, 0, "error")
    else:
        print(f"    📦 Dispatché: déjà importé ✓")

    # Consommé
    if not already_imported("consumed", year, month):
        try:
            rows = fetch_question(token, 1683, [p_range("DATE", start, end)])
            data = t_consumed(rows, start, end)
            delete_month("consumed", "date", year, month)
            n = insert("consumed", data)
            print(f"    🍽️  Consommé: {n} lignes")
            log_import("consumed", year, month, n)
        except Exception as ex:
            print(f"    ❌ Consommé: {ex}")
            log_import("consumed", year, month, 0, "error")
    else:
        print(f"    🍽️  Consommé: déjà importé ✓")

    # Livré
    if not already_imported("delivered", year, month):
        try:
            rows = fetch_question(token, 1687, [p_range("DATE", start, end)])
            data = t_delivered(rows, start, end)
            delete_month("delivered", "delivery_date", year, month)
            n = insert("delivered", data)
            print(f"    🚚 Livré: {n} lignes")
            log_import("delivered", year, month, n)
        except Exception as ex:
            print(f"    ❌ Livré: {ex}")
            log_import("delivered", year, month, 0, "error")
    else:
        print(f"    🚚 Livré: déjà importé ✓")

    # Stock 12h30 — par semaine pour éviter les timeouts
    # La question 1682 génère une série de dates SQL, donc limiter à 1 mois max
    if not already_imported("stock_12h30", year, month):
        try:
            rows = fetch_question(token, 1682, p_stock("12:31", start, end))
            data = t_stock(rows, start, end)
            delete_month("stock_12h30", "timestamp", year, month)
            n = insert("stock_12h30", data)
            print(f"    📊 Stock 12h30: {n} lignes")
            log_import("stock_12h30", year, month, n)
        except Exception as ex:
            print(f"    ❌ Stock 12h30: {ex}")
            log_import("stock_12h30", year, month, 0, "error")
    else:
        print(f"    📊 Stock 12h30: déjà importé ✓")

    # Stock 6h
    if not already_imported("stock_6h", year, month):
        try:
            rows = fetch_question(token, 1682, p_stock("06:01", start, end))
            data = t_stock(rows, start, end)
            delete_month("stock_6h", "timestamp", year, month)
            n = insert("stock_6h", data)
            print(f"    📊 Stock 6h: {n} lignes")
            log_import("stock_6h", year, month, n)
        except Exception as ex:
            print(f"    ❌ Stock 6h: {ex}")
            log_import("stock_6h", year, month, 0, "error")
    else:
        print(f"    📊 Stock 6h: déjà importé ✓")

    # Preuves
    if not already_imported("delivery_proofs", year, month):
        try:
            rows = fetch_question(token, 1673, [p_range("Date", start, end)])
            data = t_proofs(rows, start, end)
            delete_month("delivery_proofs", "date", year, month)
            n = insert("delivery_proofs", data)
            print(f"    📸 Preuves: {n} lignes")
            log_import("delivery_proofs", year, month, n)
        except Exception as ex:
            print(f"    ❌ Preuves: {ex}")
            log_import("delivery_proofs", year, month, 0, "error")
    else:
        print(f"    📸 Preuves: déjà importé ✓")

# ─── MAIN ─────────────────────────────────────────────────

def main():
    print("🚀 Démarrage synchronisation Metabase → Supabase")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"   Mode: {'PREMIER IMPORT COMPLET' if FIRST_RUN else 'MISE À JOUR AUTO'}\n")

    token = metabase_token()

    if FIRST_RUN:
        # Vider toutes les tables
        print("🗑️  Vidage des tables...")
        for t in ["dispatched","delivered","consumed","stock_12h30","stock_6h","delivery_proofs","import_logs"]:
            truncate_table(t)
        chunks = month_chunks(IMPORT_FROM, TODAY)
        print(f"\n📋 {len(chunks)} mois à importer ({IMPORT_FROM} → {TODAY})")
    else:
        # Seulement mois précédent + mois en cours
        if TODAY.month == 1:
            prev = date(TODAY.year - 1, 12, 1)
        else:
            prev = date(TODAY.year, TODAY.month - 1, 1)
        curr = date(TODAY.year, TODAY.month, 1)
        prev_end = curr - timedelta(days=1)
        curr_end = TODAY
        chunks = [(prev, prev_end), (curr, curr_end)]
        print(f"📋 Mise à jour: {len(chunks)} mois")

    for (start, end) in chunks:
        sync_month(token, start.year, start.month, start, end)

    print(f"\n✅ Synchronisation terminée !")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

if __name__ == "__main__":
    main()
