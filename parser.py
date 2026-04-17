import re
from google.cloud import bigquery
import datetime

PROJECT_ID = 'scraping-ak-drnovice'
SOURCE_TABLE = f"{PROJECT_ID}.treninkovy_plan.treninkovy_plan_data"
DEST_TABLE = f"{PROJECT_ID}.treninkovy_plan.treninkovy_plan_structured"

client = bigquery.Client(project=PROJECT_ID)

def clean_intro(full_text, title):
    intro_match = re.search(r'(.*?)(?=Vlastní týdenní tréninkový plán)', full_text, re.DOTALL | re.I)
    if not intro_match:
        return ""
    raw_intro = intro_match.group(1)
    if ">" in raw_intro:
        raw_intro = raw_intro.split(">")[-1].strip()
    elif "akdrnovice@seznam.cz" in raw_intro:
        raw_intro = raw_intro.split("akdrnovice@seznam.cz")[-1].strip()
    raw_intro = re.sub(r'^Aktuality\s*', '', raw_intro, flags=re.I).strip()
    escaped_title = re.escape(title.strip())
    clean_text = re.sub(escaped_title, '', raw_intro, flags=re.I).strip()
    return clean_text

def parse_record(row):
    text = row.content_text
    title = row.title
    structured_rows = []

    # ROZŠÍŘENÝ SLOVNÍK ČÍSLOVEK (aby to nenechávalo null u devátého atd.)
    cz_nums = {
        "první": 1, "druhý": 2, "třetí": 3, "čtvrtý": 4, "pátý": 5, "šestý": 6, "sedmý": 7, "osmý": 8, "devátý": 9, "desátý": 10, "jedenáctý": 11, "dvanáctý": 12,
        "prvního": 1, "druhého": 2, "třetího": 3, "čtvrtého": 4, "pátého": 5, "šestého": 6, "sedmého": 7, "osmého": 8, "devátého": 9, "desátého": 10, "jedenáctého": 11, "dvanáctého": 12
    }
    
    tyden_match = re.search(r'(\w+)\s+týden', title, re.I)
    cyklus_match = re.search(r'(\w+)\s+cyklu', title, re.I)
    rok_match = re.search(r'(\d{4}\s*–\s*\d{4})', title)
    
    tyden_num = cz_nums.get(tyden_match.group(1).lower()) if tyden_match else None
    cyklus_num = cz_nums.get(cyklus_match.group(1).lower()) if cyklus_match else None
    tren_rok = rok_match.group(1) if rok_match else None

    date_match = re.search(r'od\s+(\d+\.\s*\d+\.\s*\d+)\s+do\s+(\d+\.\s*\d+\.\s*\d+)', text)
    d_from, d_to = None, None
    if date_match:
        try:
            d_str_from = re.sub(r'\s+', '', date_match.group(1))
            d_str_to = re.sub(r'\s+', '', date_match.group(2))
            d_from = datetime.datetime.strptime(d_str_from, '%d.%m.%Y').date()
            d_to = datetime.datetime.strptime(d_str_to, '%d.%m.%Y').date()
        except: pass

    intro_text = clean_intro(text, title)

    days_pattern = r'\n(PO|UT|ÚT|ST|CT|ČT|PA|PÁ|SO|NE)\s*:'
    parts = re.split(days_pattern, text)
    workout_dict = {}
    last_outro = ""
    if len(parts) > 1:
        for i in range(1, len(parts), 2):
            key = parts[i].upper().replace('UT', 'ÚT').replace('CT', 'ČT').replace('PA', 'PÁ')
            day_content = parts[i+1].strip()
            if key == "NE":
                lines = day_content.split('\n')
                workout_dict[key] = lines[0].strip()
                last_outro = "\n".join(lines[1:]).strip()
            else:
                workout_dict[key] = day_content.split('\n')[0].strip()

    day_mapping = [
        ("PO", "1 PO"), ("ÚT", "2 ÚT"), ("ST", "3 ST"), 
        ("ČT", "4 ČT"), ("PÁ", "5 PÁ"), ("SO", "6 SO"), ("NE", "7 NE")
    ]

    for short_day, long_day in day_mapping:
        structured_rows.append({
            "id": f"{row.id}_{short_day}",
            "cyklus_num": cyklus_num,
            "tyden_num": tyden_num,
            "tren_rok": tren_rok,
            "intro": intro_text[:2000],
            "datum_od": d_from.isoformat() if d_from else None,
            "datum_do": d_to.isoformat() if d_to else None,
            "den_v_tydnu": long_day,
            "workout": workout_dict.get(short_day, ""),
            "outro": last_outro[:2000],
            "original_url": row.url,
            "scraped_at": row.scraped_at.isoformat()
        })
    return structured_rows

# --- EXECUTION ---
print("Čistím cílovou tabulku...")
client.query(f"TRUNCATE TABLE `{DEST_TABLE}`").result()
print("Opravuji cyklus_num a zapisuji data...")
rows = client.query(f"SELECT * FROM `{SOURCE_TABLE}`").result()
final_data = []
for r in rows:
    final_data.extend(parse_record(r))

if final_data:
    errors = client.insert_rows_json(DEST_TABLE, final_data)
    if not errors:
        print("HOTOVO! Cyklus 'devátý' a další by už měly být v pořádku.")
    else:
        print(f"Chyba: {errors}")
