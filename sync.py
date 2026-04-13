import os
import requests
from datetime import datetime, date, timedelta
import re
import json

# ─── CONFIG ───────────────────────────────────────────────
METABASE_URL = os.environ["METABASE_URL"].strip()
METABASE_EMAIL = os.environ["METABASE_EMAIL"].strip()
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"].strip()
SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()

# On récupère les 6 derniers mois seulement pour éviter les timeouts
END_DATE = date.today()
START_DATE = date(END_DATE.year - 1, 10, 1)  # Octobre année précédente
print(f"📅 Période : {START_DATE} → {END_DATE}")

FRENCH_MONTHS = {
    "janvier":1,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12
}
FRENCH_DAYS = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]

# ─── HELPERS ──────────────────────────────────────────────

def parse_french_date(s):
    if not s: return None
    s = str(s).strip().strip('"')
    m = re.match(r"(\d+)\s+(\w+),\s*(\d{4}),?\s*(\d{2}):(\d{2})", s)
    if m:
        day,month_str,year,hour,minute = m.groups()
        month = FRENCH_MONTHS.get(month_str.lower())
        if month: return datetime(int(year),month,int(day),int(hour),int(minute))
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        day,month,year = m.groups()
        return datetime(int(year),int(month),int(day))
    m = re.match(r"(\d+)\s+(\w+),\s*(\d{4})", s)
    if m:
        day,month_str,year = m.groups()
        month = FRENCH_MONTHS.get(month_str.lower())
        if month: return datetime(int(year),month,int(day))
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        year,month,day = m.groups()
        return datetime(int(year),int(month),int(day))
    print(f"  ⚠️  Date non reconnue : {s}")
    return None

def parse_number(s):
    if s is None or s == "": return None
    try: return float(str(s).replace(",","."))
    except: return None

def iso_week(dt):
    if not dt: return None
    d = dt if isinstance(dt, date) else dt.date()
    tmp = datetime(d.year, d.month, d.day)
    day = tmp.isoweekday()
    tmp += timedelta(days=4-day)
    y = datetime(tmp.year, 1, 1)
    return int((tmp - y).days / 7) + 1

def french_day(dt):
    if not dt: return None
    d = dt if isinstance(dt, datetime) else datetime.combine(dt, datetime.min.time())
    return FRENCH_DAYS[d.weekday()]

def category2(product_name, category):
    name = (product_name or "").lower()
    if any(x in name for x in ["cookie","yaourt brassé","mousse au chocolat"]):
        return "Permanent"
    return category or ""

def epd_flag(cat2):
    return "EPD" if cat2 in ["Starter","Snacking","Dish","Dessert"] else ""

# ─── SUPABASE ─────────────────────────────────────────────

def supa_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

def truncate_table(table):
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}?id=gte.0",
        headers=supa_headers()
    )
    # Fallback: delete all rows
    r2 = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}?imported_at=gte.2000-01-01",
        headers=supa_headers()
    )
    print(f"  🗑️  Table {table} vidée")

def insert(table, rows):
    if not rows:
        print(f"  Aucune ligne pour {table}")
        return 0
    batch_size = 300
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=supa_headers(),
            json=batch
        )
        if r.status_code not in [200,201]:
            print(f"  ❌ Erreur Supabase {table}: {r.status_code} {r.text[:200]}")
            return total
        total += len(batch)
        if total % 5000 == 0:
            print(f"  ... {total}/{len(rows)}")
    print(f"  ✅ {total} lignes insérées dans {table}")
    return total

def log_import(data_type, row_count, status="success"):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/import_logs",
        headers=supa_headers(),
        json=[{"data_type":data_type,"imported_at":datetime.utcnow().isoformat(),"row_count":row_count,"status":status}]
    )

# ─── METABASE ─────────────────────────────────────────────

def metabase_token():
    print("🔐 Connexion à Metabase...")
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username":METABASE_EMAIL,"password":METABASE_PASSWORD},
        timeout=30
    )
    r.raise_for_status()
    print("  ✅ Connecté")
    return r.json()["id"]

def fetch_question(token, question_id, params=None):
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    r = requests.post(
        f"{METABASE_URL}/api/card/{question_id}/query/json",
        headers=headers,
        json={"parameters": params or []},
        timeout=600
    )
    if r.status_code != 200:
        print(f"  ❌ Erreur Metabase {question_id}: {r.status_code} {r.text[:400]}")
        return []
    try:
        # Essayer de parser le JSON même s'il est tronqué
        text = r.text
        data = json.loads(text)
        if isinstance(data, list):
            return data
        print(f"  ❌ Réponse inattendue: {str(data)[:200]}")
        return []
    except json.JSONDecodeError as e:
        print(f"  ⚠️  JSON partiel détecté, tentative de récupération...")
        # Essayer de récupérer les lignes complètes
        try:
            text = r.text
            last_bracket = text.rfind('},')
            if last_bracket > 0:
                partial = text[:last_bracket+1] + ']'
                data = json.loads(partial)
                print(f"  ⚠️  {len(data)} lignes récupérées (JSON partiel)")
                return data
        except:
            pass
        print(f"  ❌ Impossible de parser le JSON: {e}")
        return []

# ─── PARAMS METABASE ──────────────────────────────────────

def date_param(tag, value):
    return {"type":"date/single","target":["dimension",["template-tag",tag]],"value":str(value)}

def range_param(start, end):
    return [
        {"type":"date/single","target":["dimension",["template-tag","DATE_RANGE.start"]],"value":str(start)},
        {"type":"date/single","target":["dimension",["template-tag","DATE_RANGE.end"]],"value":str(end)},
    ]

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
        if not dt: continue
        out.append({
            "date": dt.date().isoformat(),
            "site": r.get("Emplacement") or r.get("emplacement"),
            "category": cat,
            "product_name": pname,
            "quantity": qty,
            "unit_cost": cost,
            "week_number": iso_week(dt),
            "year": dt.year,
            "category2": cat2,
            "epd": epd_flag(cat2),
            "value": round((qty or 0)*(cost or 0),4),
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
        if not dt: continue
        out.append({
            "week_number": int(parse_number(r.get("n° semaine") or iso_week(dt) or 0) or 0),
            "year": dt.year,
            "date": dt.date().isoformat(),
            "site": r.get("Emplacement") or r.get("emplacement"),
            "product_name": pname,
            "category": cat,
            "protein_type": r.get("Type") or r.get("type"),
            "quantity": qty,
            "unit_cost": cost,
            "category2": cat2,
            "epd": epd_flag(cat2),
            "value": round((qty or 0)*(cost or 0),4),
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
        if not dt: continue
        out.append({
            "delivery_date": dt.date().isoformat(),
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
            "value": round((qty or 0)*(cost or 0),4),
            "week_number": iso_week(dt),
            "year": dt.year,
            "day_name": french_day(dt),
        })
    return out

def transform_stock(rows):
    out = []
    for r in rows:
        ts = parse_french_date(r.get("instant_T") or r.get("instant_t"))
        dlc = parse_french_date(r.get("dlc") or r.get("DLC"))
        cat = r.get("Catégorie du produit") or ""
        pname = r.get("Nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit") or 0) or 0)
        if not ts: continue
        out.append({
            "timestamp": ts.isoformat(),
            "site": r.get("Nom de l'emplacement"),
            "category": cat,
            "product_name": pname,
            "quantity": qty,
            "dlc_date": dlc.date().isoformat() if dlc else None,
            "category2": cat2,
            "epd": epd_flag(cat2),
            "week_number": iso_week(ts),
            "year": ts.year,
            "day_name": french_day(ts),
            "dlc_day_name": french_day(dlc),
        })
    return out

def transform_proofs(rows):
    out = []
    seen = set()
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        site = r.get("Emplacement") or r.get("emplacement")
        if not dt: continue
        key = f"{dt.date()}_{site}"
        if key in seen: continue
        seen.add(key)
        out.append({
            "date": dt.date().isoformat(),
            "site": site,
            "preparation_url": r.get("Préparation") or r.get("preparation"),
            "delivery_url": r.get("Livraison") or r.get("livraison"),
            "week_number": iso_week(dt),
            "year": dt.year,
            "day_name": french_day(dt),
        })
    return out

# ─── MAIN ─────────────────────────────────────────────────

def main():
    print("🚀 Démarrage synchronisation Metabase → Supabase")
    print(f"   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n")

    token = metabase_token()

    # Vider les tables d'abord
    print("🗑️  Vidage des tables...")
    for t in ["dispatched","delivered","consumed","stock_12h30","stock_6h","delivery_proofs","import_logs"]:
        truncate_table(t)
    print()

    # ── Dispatché ──
    print("📦 Dispatché (question 1684)...")
    try:
        rows = fetch_question(token, 1684, [])
        print(f"  {len(rows)} lignes reçues")
        data = transform_dispatched(rows)
        n = insert("dispatched", data)
        log_import("dispatched", n)
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("dispatched", 0, "error")

    # ── Consommé ──
    print("🍽️  Consommé (question 1683)...")
    try:
        rows = fetch_question(token, 1683, [])
        print(f"  {len(rows)} lignes reçues")
        data = transform_consumed(rows)
        n = insert("consumed", data)
        log_import("consumed", n)
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("consumed", 0, "error")

    # ── Livré — par chunks de 3 mois pour éviter timeout ──
    print("🚚 Livré (question 1687) — par trimestres...")
    total_livré = 0
    try:
        chunks = []
        s = date(2025, 1, 1)
        while s < END_DATE:
            e = min(date(s.year, s.month+3 if s.month<=9 else 1, 1) if s.month<=9
                    else date(s.year+1, 1, 1), END_DATE)
            chunks.append((s, e))
            s = e

        for (s, e) in chunks:
            print(f"  Chunk {s} → {e}...")
            params = [date_param("DATE", f"{s}~{e}")]
            rows = fetch_question(token, 1687, params)
            print(f"    {len(rows)} lignes")
            data = transform_delivered(rows)
            n = insert("delivered", data)
            total_livré += n
        log_import("delivered", total_livré)
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("delivered", 0, "error")

    # ── Stock 12h30 — par chunks ──
    print("📊 Stock 12h30 (question 1682)...")
    total_stock12 = 0
    try:
        chunks = []
        s = date(2025, 1, 1)
        while s < END_DATE:
            e = min(date(s.year, s.month+1, 1) if s.month < 12
                    else date(s.year+1, 1, 1), END_DATE)
            chunks.append((s, e))
            s = e

        for (s, e) in chunks:
            print(f"  Chunk {s} → {e}...")
            params = range_param(s, e) + [
                {"type":"category","target":["variable",["template-tag","HEURE"]],"value":"12:31"}
            ]
            rows = fetch_question(token, 1682, params)
            print(f"    {len(rows)} lignes")
            data = transform_stock(rows)
            n = insert("stock_12h30", data)
            total_stock12 += n
        log_import("stock_12h30", total_stock12)
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("stock_12h30", 0, "error")

    # ── Stock 6h ──
    print("📊 Stock 6h (question 1682)...")
    total_stock6 = 0
    try:
        chunks = []
        s = date(2025, 1, 1)
        while s < END_DATE:
            e = min(date(s.year, s.month+1, 1) if s.month < 12
                    else date(s.year+1, 1, 1), END_DATE)
            chunks.append((s, e))
            s = e

        for (s, e) in chunks:
            print(f"  Chunk {s} → {e}...")
            params = range_param(s, e) + [
                {"type":"category","target":["variable",["template-tag","HEURE"]],"value":"06:01"}
            ]
            rows = fetch_question(token, 1682, params)
            print(f"    {len(rows)} lignes")
            data = transform_stock(rows)
            n = insert("stock_6h", data)
            total_stock6 += n
        log_import("stock_6h", total_stock6)
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("stock_6h", 0, "error")

    # ── Preuves ──
    print("📸 Preuves livraison (question 1673)...")
    try:
        rows = fetch_question(token, 1673, [])
        print(f"  {len(rows)} lignes reçues")
        data = transform_proofs(rows)
        n = insert("delivery_proofs", data)
        log_import("delivery_proofs", n)
    except Exception as e:
        print(f"  ❌ {e}")
        log_import("delivery_proofs", 0, "error")

    print("\n✅ Synchronisation terminée !")

if __name__ == "__main__":
    main()
