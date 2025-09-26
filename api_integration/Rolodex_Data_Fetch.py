import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ROLODEX_API_KEY")
BASE_URL = os.getenv("ROLODEX_BASE_URL")

if not API_KEY or not BASE_URL:
    raise ValueError("ROLODEX_API_KEY or ROLODEX_BASE_URL missing in .env")

headers = {
    "Content-Type": "application/json",
    "x-rolodex-api-key": API_KEY
}

endpoints = [
    "contacts",
    "companies",
    "tags",
    "notes",
    "lists",
    "tasks",
    "custom-fields",
    "workspace-users"
]

output_folder = "rolodex_data"
os.makedirs(output_folder, exist_ok=True)

def fetch_multiple_pages(endpoint, pages=3, limit=100):
 
    all_records = []
    for page in range(pages):
        offset = page * limit
        url = f"{BASE_URL.rstrip('/')}/{endpoint}?limit={limit}&offset={offset}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching {endpoint} at offset {offset}: {response.text}")
            break
        data = response.json()
        records = data.get("data", [])
        total = data.get("pagination", {}).get("total", len(records))
        all_records.extend(records)
        print(f"Fetched {len(records)} records from page {page + 1} (total so far: {len(all_records)}/{total}) for {endpoint}")
        if len(records) < limit: 
            break
    return all_records

for ep in endpoints:
    print(f"\nFetching first 3 pages of records for endpoint: {ep}")
    records = fetch_multiple_pages(ep, pages=3)
    output_file = os.path.join(output_folder, f"{ep}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=4, ensure_ascii=False)
    print(f"Saved {len(records)} records to {output_file}")
