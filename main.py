import functions_framework
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
import datetime
import time
import re

PROJECT_ID = 'scraping-ak-drnovice'
DATASET_ID = 'treninkovy_plan'
RAW_TABLE = 'scraping-ak-drnovice.treninkovy_plan.treninkovy_plan_data'
STR_TABLE = 'scraping-ak-drnovice.treninkovy_plan.treninkovy_plan_structured'

client = bigquery.Client(project=PROJECT_ID)

def get_existing_urls():
    query = f"SELECT url FROM `{RAW_TABLE}`"
    try:
        results = client.query(query).result()
        return set(row.url for row in results)
    except Exception:
        return set()

def parse_and_insert_structured(row_data):
    # Rozšířený slovník číslovek pro cykly a týdny
    cz_nums = {
        "první": 1, "druhý": 2, "třetí": 3, "čtvrtý": 4, "pátý": 5, "šestý": 6, 
        "sedmý": 7, "osmý": 8, "devátý": 9, "desátý": 10, "jedenáctý": 11, "dvanáctý": 12,
        "prvního": 1, "druhého": 2, "třetího": 3, "čtvrtého": 4, "pátého": 5, "šestého": 6, 
        "sedmého": 7, "osmého": 8, "devátého": 9, "desátého": 10, "jedenáctého": 11, "dvanáctého": 12
    }
    
    text = row_data['content_text']
    title = row_data['title']
    
    # Metadata z titulku
    tyden_match = re.search(r'(\w+)\s+týden', title, re.I)
    cyklus_match = re.search(r'(\w+)\s+cyklu', title, re.I)
    rok_match = re.search(r'(\d{4}\s*–\s*\d{4})', title)
    
    tyden_num = cz_nums.get(tyden_match.group(1).lower()) if tyden_match else None
    cyklus_num = cz_nums.get(cyklus_match.group(1).lower()) if cyklus_match else None
    tren_rok = rok_match.group(1) if rok_match else None

    # Datumy
    date_match = re.search(r'od\s+(\d+\.\s*\d+\.\s*\d+)\s+do\s+(\d+\.\s*\d+\.\s*\d+)', text)
    d_from, d_to = None, None
    if date_match:
        try:
            d_str_from = re.sub(r'\s+', '', date_match.group(1))
            d_str_to = re.sub(r'\s+', '', date_match.group(2))
            d_from = datetime.datetime.strptime(d_str_from, '%d.%m.%Y').date().isoformat()
            d_to = datetime.datetime.strptime(d_str_to, '%d.%m.%Y').date().isoformat()
        except: pass

    # Intro (očištěné)
    intro_match = re.search(r'(.*?)(?=Vlastní týdenní tréninkový plán)', text, re.DOTALL | re.I)
    intro_text = ""
    if intro_match:
        raw_intro = intro_match.group(1)
        if ">" in raw_intro: raw_intro = raw_intro.split(">")[-1]
        elif "akdrnovice@seznam.cz" in raw_intro: raw_intro = raw_intro.split("akdrnovice@seznam.cz")[-1]
        intro_text = re.sub(re.escape(title.strip()), '', raw_intro, flags=re.I).strip()

    # Dny a Outro
    days_pattern = r'\n(PO|UT|ÚT|ST|CT|ČT|PA|PÁ|SO|NE)\s*:'
    parts = re.split(days_pattern, text)
    workout_dict = {}
    last_outro = ""
    if len(parts) > 1:
        for i in range(1, len(parts), 2):
            key = parts[i].upper().replace('UT', 'ÚT').replace('CT', 'ČT').replace('PA', 'PÁ')
            content_lines = parts[i+1].strip().split('\n')
            workout_dict[key] = content_lines[0].strip()
            if key == "NE" and len(content_lines) > 1:
                last_outro = "\n".join(content_lines[1:]).strip()

    # Mapování na číslované dny pro správné řazení v reportech
    day_mapping = [
        ("PO", "1 PO"), ("ÚT", "2 ÚT"), ("ST", "3 ST"), 
        ("ČT", "4 ČT"), ("PÁ", "5 PÁ"), ("SO", "6 SO"), ("NE", "7 NE")
    ]

    structured_rows = []
    for short_day, long_day in day_mapping:
        structured_rows.append({
            "id": f"{row_data['id']}_{short_day}",
            "cyklus_num": cyklus_num,
            "tyden_num": tyden_num,
            "tren_rok": tren_rok,
            "intro": intro_text[:2000],
            "datum_od": d_from,
            "datum_do": d_to,
            "den_v_tydnu": long_day,
            "workout": workout_dict.get(short_day, ""),
            "outro": last_outro[:2000],
            "original_url": row_data['url'],
            "scraped_at": row_data['scraped_at']
        })
    return structured_rows

@functions_framework.http
def scrape_treninkovy_plan(request):
    try:
        base_url = 'https://www.akdrnovice.eu'
        existing_urls = get_existing_urls()
        res = requests.get(f"{base_url}/treninkovy-plan/", timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        new_links = []
        for a in soup.find_all('a', href=True):
            text = a.get_text().strip()
            if text.lower().startswith('tréninkový plán') and '/products/' in a['href']:
                url = a['href'] if a['href'].startswith('http') else f"{base_url}{a['href']}"
                if url not in existing_urls:
                    new_links.append({'url': url, 'title': text})
        
        if not new_links:
            return "Všechna data jsou aktuální.", 200
            
        raw_rows = []
        str_rows = []
        for item in new_links:
            time.sleep(2)
            det_res = requests.get(item['url'], timeout=15)
            det_soup = BeautifulSoup(det_res.text, 'html.parser')
            body = det_soup.find('body')
            content = body.get_text(separator='\n', strip=True) if body else det_res.text
            scraped_now = datetime.datetime.utcnow().isoformat()
            row_id = f"plan-{int(time.time())}-{hash(item['url']) % 1000}"
            raw_entry = {'id': row_id, 'url': item['url'], 'title': item['title'], 'content_text': content, 'scraped_at': scraped_now}
            raw_rows.append(raw_entry)
            str_rows.extend(parse_and_insert_structured(raw_entry))
            
        client.insert_rows_json(RAW_TABLE, raw_rows)
        client.insert_rows_json(STR_TABLE, str_rows)
        return f"Doplněno {len(new_links)} nových plánů s číslováním dnů.", 200
    except Exception as e:
        return f"Chyba: {str(e)}", 500
