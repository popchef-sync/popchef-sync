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

# FIRST_RUN = True  → importe TOUT depuis IMPORT_FROM
# FIRST_RUN = False → met a jour mois precedent + mois en cours
FIRST_RUN = True
IMPORT_FROM = date(2025, 1, 1)
TODAY = date.today()

FRENCH_MONTHS = {
    "janvier": 1, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "decembre": 12,
    "f\xe9vrier": 2, "ao\xfbt": 8, "d\xe9cembre": 12
}
FRENCH_DAYS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]

def month_chunks(start, end):
    chunks = []
    s = date(start.year, start.month, 1)
    while s <= end:
        e = date(s.year + 1, 1, 1) if s.month == 12 else date(s.year, s.month + 1, 1)
        e = min(e - timedelta(days=1), end)
        chunks.append((s, e))
        s = e + timedelta(days=1)
    return chunks

def parse_french_date(s):
    if not s: return None
    s = str(s).strip().strip('"')
    # Format ISO avec timezone: 2026-03-01T12:31:00+01:00 ou 2026-03-01T12:31:00+00
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})", s)
    if m:
        year, month, day, hour, minute, second = m.groups()
        return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    # Format ISO simple: 2026-03-01 12:31:00+00
    m = re.match(r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})", s)
    if m:
        year, month, day, hour, minute, second = m.groups()
        return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
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
    if any(x in name for x in ["cookie", "yaourt brass\xe9", "mousse au chocolat"]):
        return "Permanent"
    return category or ""

def epd_flag(cat2):
    return "EPD" if cat2 in ["Starter", "Snacking", "Dish", "Dessert"] else ""

def supa_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": "Bearer " + SUPABASE_KEY,
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
        SUPABASE_URL + "/rest/v1/" + table + "?" + date_col + "=gte." + str(start_d) + "&" + date_col + "=lt." + str(end_d),
        headers=supa_headers()
    )

def truncate_table(table):
    requests.delete(SUPABASE_URL + "/rest/v1/" + table + "?id=gte.0", headers=supa_headers())
    print("  Table " + table + " videe")

def insert(table, rows):
    if not rows: return 0
    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        r = requests.post(SUPABASE_URL + "/rest/v1/" + table, headers=supa_headers(), json=batch)
        if r.status_code not in [200, 201]:
            print("    Erreur insert " + table + ": " + str(r.status_code) + " " + r.text[:150])
            return total
        total += len(batch)
    return total

def log_import(data_type, year, month, row_count, status="success"):
    requests.post(
        SUPABASE_URL + "/rest/v1/import_logs",
        headers=supa_headers(),
        json=[{"data_type": data_type, "year": year, "month": month,
               "imported_at": datetime.utcnow().isoformat(),
               "row_count": row_count, "status": status}]
    )

def already_imported(data_type, year, month):
    if FIRST_RUN: return False
    r = requests.get(
        SUPABASE_URL + "/rest/v1/import_logs?data_type=eq." + data_type + "&year=eq." + str(year) + "&month=eq." + str(month) + "&status=eq.success&limit=1",
        headers=supa_headers()
    )
    try: return len(r.json()) > 0
    except: return False

def metabase_token():
    print("Connexion a Metabase...")
    r = requests.post(
        METABASE_URL + "/api/session",
        json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD},
        timeout=30
    )
    r.raise_for_status()
    print("  Connecte")
    return r.json()["id"]

def fetch_question(token, question_id, params):
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    r = requests.post(
        METABASE_URL + "/api/card/" + str(question_id) + "/query/json",
        headers=headers,
        json={"parameters": params},
        timeout=300
    )
    if r.status_code != 200:
        print("    Erreur Metabase " + str(question_id) + ": " + str(r.status_code) + " " + r.text[:300])
        return []
    try:
        data = r.json()
        if isinstance(data, list): return data
        print("    Reponse inattendue: " + str(data)[:400])
        return []
    except Exception:
        try:
            last = r.text.rfind('},')
            if last > 0:
                data = json.loads(r.text[:last + 1] + ']')
                print("    JSON partiel: " + str(len(data)) + " lignes")
                return data
        except Exception:
            pass
        return []

def fetch_stock(token, heure, start, end):
    """
    Execute directement le SQL du stock via /api/dataset/json
    avec les valeurs de date substituees directement dans le SQL.
    Le SQL est copie exactement depuis la question Metabase 1682.
    """
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}

    sql = (
        "WITH date_range AS (\n"
        "    SELECT \n"
        "        generate_series(\n"
        "            '" + str(start) + "'::date,\n"
        "            '" + str(end) + "'::date,\n"
        "            interval '1 day'\n"
        "        )::date AS jour\n"
        "),\n"
        "snapshots AS (\n"
        "    SELECT\n"
        "        date_range.jour,\n"
        "        (date_range.jour::timestamp + make_interval(\n"
        "            hours => split_part('" + heure + "', ':', 1)::int,\n"
        "            mins  => split_part('" + heure + "', ':', 2)::int\n"
        "        )) AS instant_T\n"
        "    FROM date_range\n"
        ")\n"
        "SELECT\n"
        "    canteen_locations.\"name\" AS \"Nom de l'emplacement\",\n"
        "    CASE \n"
        "        WHEN products.\"subCategory\" IS NOT NULL THEN products.\"subCategory\"\n"
        "        ELSE products.\"category\"\n"
        "    END AS \"Cat\\u00e9gorie du produit\",\n"
        "    products.\"name\" AS \"Nom du produit\",\n"
        "    snapshots.instant_T,\n"
        "    COUNT(*) AS \"nombre de produit\",\n"
        "    openit_fridge_products.\"expiresAt\" AS dlc\n"
        "FROM snapshots\n"
        "LEFT JOIN openit_fridge_products\n"
        "    ON openit_fridge_products.\"inputDate\" <= snapshots.instant_T\n"
        "    AND (openit_fridge_products.\"outputDate\" > snapshots.instant_T \n"
        "         OR openit_fridge_products.\"outputDate\" IS NULL)\n"
        "LEFT JOIN products\n"
        "    ON products.id = openit_fridge_products.\"productId\"\n"
        "LEFT JOIN canteen_fridges\n"
        "    ON canteen_fridges.id = openit_fridge_products.\"canteenFridgeId\"\n"
        "LEFT JOIN canteen_locations\n"
        "    ON canteen_locations.id = canteen_fridges.\"canteenLocationId\"\n"
        "WHERE canteen_fridges.\"isActive\" = true\n"
        "GROUP BY\n"
        "    snapshots.instant_T,\n"
        "    canteen_locations.\"name\",\n"
        "    \"Cat\\u00e9gorie du produit\",\n"
        "    products.\"name\",\n"
        "    dlc\n"
        "ORDER BY\n"
        "    snapshots.instant_T ASC,\n"
        "    canteen_locations.\"name\",\n"
        "    \"Cat\\u00e9gorie du produit\",\n"
        "    products.\"name\",\n"
        "    dlc"
    )

    # Format correct pour /api/dataset
    payload = {
        "database": 2,
        "type": "native",
        "native": {
            "query": sql,
            "template-tags": {}
        },
        "middleware": {
            "js-int-to-string?": True,
            "add-default-userland-constraints?": True
        }
    }

    # Essai 1: endpoint /api/dataset (retourne JSON avec rows/cols)
    r = requests.post(
        METABASE_URL + "/api/dataset",
        headers=headers,
        json=payload,
        timeout=300
    )
    
    print("    Stock API status: " + str(r.status_code))
    
    if r.status_code == 200:
        try:
            result = r.json()
            data_section = result.get("data", {})
            rows = data_section.get("rows", [])
            cols = data_section.get("cols", [])
            if rows is not None and cols:
                col_names = []
                for c in cols:
                    name = c.get("display_name") or c.get("name") or ""
                    col_names.append(name)
                converted = [dict(zip(col_names, row)) for row in rows]
                print("    OK via /api/dataset: " + str(len(converted)) + " lignes")
                return converted
            print("    Reponse dataset: " + str(result)[:300])
        except Exception as ex:
            print("    Exception: " + str(ex))
    else:
        print("    Erreur /api/dataset: " + r.text[:300])

    # Essai 2: endpoint /api/dataset/json avec format query wrappé
    payload2 = {
        "query": {
            "database": 2,
            "type": "native",
            "native": {
                "query": sql,
                "template-tags": {}
            }
        }
    }
    r2 = requests.post(
        METABASE_URL + "/api/dataset/json",
        headers=headers,
        json=payload2,
        timeout=300
    )
    print("    Stock API2 status: " + str(r2.status_code))
    if r2.status_code == 200:
        try:
            data = r2.json()
            if isinstance(data, list) and data:
                if isinstance(data[0], dict):
                    # Afficher les cles pour debug
                    return data
                elif isinstance(data[0], list):
                    # Liste de listes - 1ere ligne = headers
                    headers_row = data[0]
                    print("    Headers retournes: " + str(headers_row))
                    return [dict(zip(headers_row, row)) for row in data[1:]]
            print("    Reponse API2 vide ou inattendue: " + str(type(data)))
        except Exception as ex2:
            print("    Exception API2: " + str(ex2))
    else:
        print("    Erreur API2: " + r2.text[:200])

    return []

def p_range(tag, s, e):
    return {"type": "date/range", "target": ["dimension", ["template-tag", tag]], "value": str(s) + "~" + str(e)}

def t_dispatched(rows, s, e):
    out = []
    for r in rows:
        dt = parse_french_date(r.get("Date") or r.get("date"))
        if not dt or not (s <= dt.date() <= e): continue
        cat = r.get("cat\xe9gorie") or r.get("categorie") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Quantit\xe9s dispatchées") or 0) or 0)
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
        cat = r.get("cat\xe9gorie") or r.get("categorie") or ""
        pname = r.get("Nom du produit") or r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("Nombre de consommations") or 0) or 0)
        cost = parse_number(r.get("PA Produit"))
        out.append({
            "date": dt.date().isoformat(), "year": dt.year,
            "week_number": int(parse_number(r.get("n\xb0 semaine") or iso_week(dt) or 0) or 0),
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
        cat = r.get("cat\xe9gorie du produit") or ""
        pname = r.get("nom du produit") or ""
        cat2 = category2(pname, cat)
        qty = int(parse_number(r.get("nombre de produit livr\xe9 d\xe9tect\xe9") or 0) or 0)
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
        if not isinstance(r, dict):
            continue
        # Accepter tous les formats de noms de colonnes possibles
        ts_raw = (r.get("instant_T") or r.get("instant_t") or
                  r.get("Instant T") or r.get("instant T"))
        ts = parse_french_date(ts_raw)
        if not ts or not (s <= ts.date() <= e): continue
        
        dlc_raw = (r.get("dlc") or r.get("DLC") or r.get("Dlc"))
        dlc = parse_french_date(dlc_raw)
        
        # Noms de colonnes possibles pour la categorie
        cat = (r.get("Catégorie du produit") or
               r.get("Categorie du produit") or
               r.get("catégorie du produit") or
               r.get("product_category") or "")
        
        pname = (r.get("Nom du produit") or
                 r.get("nom du produit") or
                 r.get("product_name") or "")
        
        site = (r.get("Nom de l'emplacement") or
                r.get("Nom de l'emplacement") or
                r.get("site") or "")
        
        qty_raw = (r.get("nombre de produit") or
                   r.get("Nombre de produit") or
                   r.get("quantity") or 0)
        qty = int(parse_number(qty_raw) or 0)
        
        cat2 = category2(pname, cat)
        
        out.append({
            "timestamp": ts.isoformat(), "year": ts.year, "week_number": iso_week(ts),
            "site": site,
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
        key = str(dt.date()) + "_" + str(site)
        if key in seen: continue
        seen.add(key)
        out.append({
            "date": dt.date().isoformat(), "year": dt.year, "week_number": iso_week(dt),
            "site": site,
            "preparation_url": r.get("Pr\xe9paration") or r.get("preparation"),
            "delivery_url": r.get("Livraison") or r.get("livraison"),
            "day_name": french_day(dt),
        })
    return out

def sync_month(token, year, month, start, end):
    print("  " + str(year) + "-" + str(month).zfill(2) + " (" + str(start) + " -> " + str(end) + ")")

    if not already_imported("dispatched", year, month):
        try:
            rows = fetch_question(token, 1684, [p_range("Date", start, end)])
            data = t_dispatched(rows, start, end)
            delete_month("dispatched", "date", year, month)
            n = insert("dispatched", data)
            print("    Dispatche: " + str(n) + " lignes")
            log_import("dispatched", year, month, n)
        except Exception as ex:
            print("    ERREUR Dispatche: " + str(ex))
            log_import("dispatched", year, month, 0, "error")
    else:
        print("    Dispatche: deja importe")

    if not already_imported("consumed", year, month):
        try:
            rows = fetch_question(token, 1683, [p_range("DATE", start, end)])
            data = t_consumed(rows, start, end)
            delete_month("consumed", "date", year, month)
            n = insert("consumed", data)
            print("    Consomme: " + str(n) + " lignes")
            log_import("consumed", year, month, n)
        except Exception as ex:
            print("    ERREUR Consomme: " + str(ex))
            log_import("consumed", year, month, 0, "error")
    else:
        print("    Consomme: deja importe")

    if not already_imported("delivered", year, month):
        try:
            rows = fetch_question(token, 1687, [p_range("DATE", start, end)])
            data = t_delivered(rows, start, end)
            delete_month("delivered", "delivery_date", year, month)
            n = insert("delivered", data)
            print("    Livre: " + str(n) + " lignes")
            log_import("delivered", year, month, n)
        except Exception as ex:
            print("    ERREUR Livre: " + str(ex))
            log_import("delivered", year, month, 0, "error")
    else:
        print("    Livre: deja importe")

    if not already_imported("stock_12h30", year, month):
        try:
            rows = fetch_stock(token, "12:31", start, end)
            data = t_stock(rows, start, end)
            delete_month("stock_12h30", "timestamp", year, month)
            n = insert("stock_12h30", data)
            print("    Stock 12h30: " + str(n) + " lignes")
            log_import("stock_12h30", year, month, n)
        except Exception as ex:
            print("    ERREUR Stock 12h30: " + str(ex))
            log_import("stock_12h30", year, month, 0, "error")
    else:
        print("    Stock 12h30: deja importe")

    if not already_imported("stock_6h", year, month):
        try:
            rows = fetch_stock(token, "06:01", start, end)
            data = t_stock(rows, start, end)
            delete_month("stock_6h", "timestamp", year, month)
            n = insert("stock_6h", data)
            print("    Stock 6h: " + str(n) + " lignes")
            log_import("stock_6h", year, month, n)
        except Exception as ex:
            print("    ERREUR Stock 6h: " + str(ex))
            log_import("stock_6h", year, month, 0, "error")
    else:
        print("    Stock 6h: deja importe")

    if not already_imported("delivery_proofs", year, month):
        try:
            rows = fetch_question(token, 1673, [p_range("Date", start, end)])
            data = t_proofs(rows, start, end)
            delete_month("delivery_proofs", "date", year, month)
            n = insert("delivery_proofs", data)
            print("    Preuves: " + str(n) + " lignes")
            log_import("delivery_proofs", year, month, n)
        except Exception as ex:
            print("    ERREUR Preuves: " + str(ex))
            log_import("delivery_proofs", year, month, 0, "error")
    else:
        print("    Preuves: deja importe")
        
        def call_update_stock_categories():
    """Appelle la fonction SQL pour mettre a jour les categories du stock"""
    r = requests.post(
        SUPABASE_URL + "/rest/v1/rpc/update_stock_categories",
        headers=supa_headers(),
        json={}
    )
    if r.status_code in [200, 204]:
        print("    Categories stock mises a jour")
    else:
        print("    Erreur update categories: " + str(r.status_code))
        # Mettre à jour les catégories du stock
    call_update_stock_categories()

def main():
    print("Demarrage synchronisation Metabase -> Supabase")
    print(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC")
    print("Mode: " + ("PREMIER IMPORT" if FIRST_RUN else "MISE A JOUR AUTO"))

    token = metabase_token()

    if FIRST_RUN:
        print("Vidage des tables...")
        for t in ["dispatched", "delivered", "consumed", "stock_12h30", "stock_6h", "delivery_proofs", "import_logs"]:
            truncate_table(t)
        chunks = month_chunks(IMPORT_FROM, TODAY)
        print(str(len(chunks)) + " mois a importer")
    else:
        if TODAY.month == 1:
            prev = date(TODAY.year - 1, 12, 1)
        else:
            prev = date(TODAY.year, TODAY.month - 1, 1)
        curr = date(TODAY.year, TODAY.month, 1)
        prev_end = curr - timedelta(days=1)
        chunks = [(prev, prev_end), (curr, TODAY)]
        print("Mise a jour: 2 mois")

    for (start, end) in chunks:
        sync_month(token, start.year, start.month, start, end)

    print("Synchronisation terminee!")
    print(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC")

if __name__ == "__main__":
    main()
