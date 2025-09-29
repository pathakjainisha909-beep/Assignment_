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

# Updated endpoints with include parameters for relationship data
endpoints = {
    # CRITICAL: Include all relationship data for contacts
    "contacts": "contacts?include=emails,phone_numbers,companies,custom_fields,tags,lists,notes",
    
    # Companies with valid includes only (no notes support)
    "companies": "companies?include=contacts,custom_fields,tags,lists",
    
    # Other endpoints probably don't need includes
    "tags": "tags",
    "notes": "notes", 
    "lists": "lists",
    "tasks": "tasks",
    "custom-fields": "custom-fields",
    "workspace-users": "workspace-users"
}

output_folder = "rolodex_complete_data"
os.makedirs(output_folder, exist_ok=True)

def fetch_multiple_pages(endpoint_url, endpoint_name, pages=3, limit=100):
    """Fetch multiple pages of data with proper include parameters"""
    all_records = []
    
    for page in range(pages):
        offset = page * limit
        
        # Add pagination parameters to the existing endpoint URL
        if "?" in endpoint_url:
            url = f"{BASE_URL.rstrip('/')}/{endpoint_url}&limit={limit}&offset={offset}"
        else:
            url = f"{BASE_URL.rstrip('/')}/{endpoint_url}?limit={limit}&offset={offset}"
            
        print(f"Fetching: {url}")
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching {endpoint_name} at offset {offset}: {response.status_code} - {response.text}")
            break
            
        data = response.json()
        records = data.get("data", [])
        total = data.get("pagination", {}).get("total", len(records))
        
        all_records.extend(records)
        print(f"Fetched {len(records)} records from page {page + 1} (total so far: {len(all_records)}/{total}) for {endpoint_name}")
        
        if len(records) < limit: 
            print(f"Reached end of data for {endpoint_name}")
            break
            
    return all_records


for endpoint_name, endpoint_url in endpoints.items():
    print(f"\n{'='*50}")
    print(f"Fetching COMPLETE data for: {endpoint_name}")
    print(f"{'='*50}")
    
    try:
        records = fetch_multiple_pages(endpoint_url, endpoint_name, pages=3)
        
        # Save to file
        output_file = os.path.join(output_folder, f"{endpoint_name}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4, ensure_ascii=False)
        
        print(f" Saved {len(records)} COMPLETE records to {output_file}")
        
        # Show sample of what we got for contacts (the crucial one)
        if endpoint_name == "contacts" and records:
            sample_contact = records[0]
            print(f"\nSample contact structure:")
            print(f"  - Name: {sample_contact.get('full_name', 'N/A')}")
            print(f"  - Has companies: {'companies' in sample_contact}")
            print(f"  - Has emails: {'emails' in sample_contact}")
            print(f"  - Has custom_fields: {'custom_fields' in sample_contact}")
            
            if 'companies' in sample_contact and sample_contact['companies']:
                print(f"  - Company example: {sample_contact['companies'][0]}")
                
    except Exception as e:
        print(f" Error processing {endpoint_name}: {str(e)}")

print(f"\n Data fetching complete! Check the '{output_folder}' folder.")
print(f"The contacts.json file should now have the company relationships that were missing!")

# Verification helper
def verify_relationships():
    """Quick check to see if we got the relationship data"""
    contacts_file = os.path.join(output_folder, "contacts.json")
    if os.path.exists(contacts_file):
        with open(contacts_file, 'r', encoding='utf-8') as f:
            contacts = json.load(f)
        
        if contacts:
            contact_with_company = None
            for contact in contacts[:10]: 
                if contact.get('companies'):
                    contact_with_company = contact
                    break
            
            if contact_with_company:
                print(f"\n SUCCESS: Found contact with company relationship!")
                print(f"   Contact: {contact_with_company['full_name']}")
                print(f"   Company: {contact_with_company['companies'][0]['name']}")
                print(f"   Title: {contact_with_company['companies'][0].get('title', 'N/A')}")
            else:
                print(f"\n  No contacts with companies found in first 10 records")
    else:
        print(f"\n contacts.json file not found")

# Run verification
verify_relationships()