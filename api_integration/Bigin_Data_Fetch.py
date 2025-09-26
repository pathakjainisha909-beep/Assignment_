import requests
import json
# import json
import csv     
import os
import time
from datetime import datetime

class BiginDataExporter:
    def __init__(self, access_token: str, api_domain: str = "https://www.zohoapis.in"):
        self.access_token = access_token
        self.api_domain = api_domain
        self.headers = {"Authorization": f"Zoho-oauthtoken {access_token}", "Content-Type": "application/json"}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = f"BiginData_{timestamp}"
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"Created output directory: {self.output_dir}")

    def get_all_records_from_module(self, module_name: str):
        all_records = []
        page = 1
        per_page = 200
        while True:
            success = False
            for url in [f"{self.api_domain}/bigin/v1/{module_name}", f"{self.api_domain}/bigin/v2/{module_name}"]:
                try:
                    params = {"page": page, "per_page": per_page} if page > 1 else {"per_page": per_page}
                    response = requests.get(url, headers=self.headers, params=params)
                    if response.status_code == 200:
                        data = response.json()
                        success = True
                        break
                except requests.RequestException:
                    continue
            if not success: break
            if data.get("data"):
                all_records.extend(data["data"])
                info = data.get("info", {})
                if not info.get("more_records", False) or len(data["data"]) < per_page:
                    break
            else:
                break
            page += 1
            time.sleep(0.1)
        return all_records

    def save_to_json(self, data, filename: str):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def save_to_csv(self, data, filename: str):
        if not data: return
        filepath = os.path.join(self.output_dir, filename)
        flattened_data = [self.flatten_dict(record) for record in data]
        all_fieldnames = sorted({k for record in flattened_data for k in record.keys()})
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            writer.writeheader()
            writer.writerows(flattened_data)

    def flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, str(v)))
            else:
                items.append((new_key, v))
        return dict(items)

    def export_all_data(self):
        modules = [
            {"api_name": "Contacts", "label": "Contacts"},
            {"api_name": "Accounts", "label": "Companies"},
            {"api_name": "Pipelines", "label": "Pipelines"},
            {"api_name": "Tasks", "label": "Tasks"},
            {"api_name": "Events", "label": "Events"},
            {"api_name": "Calls", "label": "Calls"},
            {"api_name": "Products", "label": "Products"},
            {"api_name": "Social", "label": "Social"},
            {"api_name": "Associated_Products", "label": "Associated Products"},
            {"api_name": "Notes", "label": "Notes"},
            {"api_name": "Attachments", "label": "Attachments"},
            {"api_name": "Deals", "label": "Deals"}
        ]

        total_records = 0
        for module in modules:
            records = self.get_all_records_from_module(module["api_name"])
            self.save_to_json(records, f"{module['api_name']}.json")
            self.save_to_csv(records, f"{module['api_name']}.csv")
            total_records += len(records)

        summary_file = os.path.join(self.output_dir, "Export_Summary.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(f"Export completed.\nTotal records exported: {total_records}\nModules: {', '.join([m['api_name'] for m in modules])}")

if __name__ == "__main__":
    ACCESS_TOKEN = "add_your_generated_token"
    API_DOMAIN = "https://www.zohoapis.in"
    exporter = BiginDataExporter(ACCESS_TOKEN, API_DOMAIN)
    exporter.export_all_data()
