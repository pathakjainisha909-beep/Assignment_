import os
import json
import pandas as pd
import uuid
import re
from typing import Dict, List, Optional, Tuple
import phonenumbers
from phonenumbers import PhoneNumberFormat, NumberParseException
import networkx as nx

def load_config(base_dir: str) -> Dict:
    config_path = os.path.join(base_dir, "config", "name_processing_config.json")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "default_country": "IN",
            "title_prefixes": {
                "architect": {"patterns": ["ar", "ar.", "arc", "arc.", "arch", "arch.", "architect"], "title": "Architect"},
                "interior_designer": {"patterns": ["id", "id.", "interior", "designer"], "title": "Interior Designer"},
                "client": {"patterns": ["cl", "cl.", "client"], "title": "Client"},
                "doctor": {"patterns": ["dr", "dr.", "doctor"], "title": "Doctor"},
                "chartered_accountant": {"patterns": ["ca", "ca.", "chartered accountant"], "title": "Chartered Accountant"}
            },
            "honorifics_to_remove": ["mr", "mr.", "mrs", "mrs.", "ms", "ms."],
            "company_indicators": ["ltd", "pvt", "llc", "inc", "corp"]
        }

def safe_get_value(record: Dict, path: str) -> Optional:
    if not path or not record:
        return None
   
    try:
        if '.split' in path:
            base_path, op = path.split('.split', 1)
            value = safe_get_value(record, base_path)
            if value is None:
                return None
            match = re.match(r"\(', '\)\[\s*(\d+)\s*\]", op)
            if match:
                index = int(match.group(1))
                parts = str(value).split(', ')
                return parts[index] if len(parts) > index else None
        else:
            current = record
            for part in path.split('.'):
                if '[' in part and part.endswith(']'):
                    base, index_str = part.split('[', 1)
                    index_str = index_str.rstrip(']')
                    if base:
                        if isinstance(current, dict) and base in current:
                            current = current[base]
                        else:
                            return None
                    if index_str.isdigit():
                        index = int(index_str)
                        if isinstance(current, list) and 0 <= index < len(current):
                            current = current[index]
                        else:
                            return None
                    else:
                        return None
                else:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return None
            return current if current not in [None, '', []] else None
    except:
        return None

def normalize_phone(phone: str, country: str = None, config: Dict = None) -> Optional[str]:
    if not phone:
        return None
    phone = str(phone).strip()
    if not phone:
        return None
    default_country = config.get("default_country", "IN") if config else "IN"
    country_code_map = {
        'india': 'IN', 'united states': 'US', 'united kingdom': 'GB',
        'canada': 'CA', 'australia': 'AU'
    }
    region_code = default_country
    if country:
        region_code = country_code_map.get(country.lower().strip(), default_country)
    try:
        parsed_number = phonenumbers.parse(phone, region_code)
        if phonenumbers.is_valid_number(parsed_number):
            return phonenumbers.format_number(parsed_number, PhoneNumberFormat.E164)
    except NumberParseException:
        pass
    return None

def clean_name_and_extract_title(name: str, config: Dict) -> Tuple[str, Optional[str]]:
    if not name or not isinstance(name, str):
        return "", None
   
    name = name.strip()
    if not name:
        return "", None
   
    title_prefixes = config.get("title_prefixes", {})
    for category, data in title_prefixes.items():
        patterns = data.get("patterns", [])
        title = data.get("title", "")
       
        for pattern in patterns:
            regex = rf'^{re.escape(pattern)}\s+'
            if re.match(regex, name, re.IGNORECASE):
                clean_name = re.sub(regex, '', name, flags=re.IGNORECASE).strip()
                if clean_name:
                    return clean_name, title
   
    honorifics = config.get("honorifics_to_remove", [])
    for honorific in honorifics:
        regex = rf'^{re.escape(honorific)}\s+'
        if re.match(regex, name, re.IGNORECASE):
            name = re.sub(regex, '', name, flags=re.IGNORECASE).strip()
            break
   
    suffixes = config.get("suffixes_to_remove", [])
    for suffix in suffixes:
        regex = rf'\s+{re.escape(suffix)}\s*$'
        name = re.sub(regex, '', name, flags=re.IGNORECASE).strip()
   
    return name, None

def process_names(first_name: str, last_name: str, full_name: str, config: Dict) -> Dict:
    result = {'first_name': None, 'last_name': None, 'full_name': None, 'inferred_title': None}
   
    if full_name and str(full_name).strip():
        clean_full, title = clean_name_and_extract_title(full_name, config)
        if clean_full:
            parts = clean_full.split()
            if len(parts) >= 2:
                result['first_name'] = parts[0]
                result['last_name'] = ' '.join(parts[1:])
            elif len(parts) == 1:
                result['first_name'] = parts[0]
            result['full_name'] = clean_full
            result['inferred_title'] = title
   
    if not result['full_name']:
        clean_first, title_first = clean_name_and_extract_title(first_name or '', config)
        clean_last, title_last = clean_name_and_extract_title(last_name or '', config)
       
        result['first_name'] = clean_first if clean_first else None
        result['last_name'] = clean_last if clean_last else None
        result['inferred_title'] = title_first or title_last
       
        if result['first_name'] and result['last_name']:
            result['full_name'] = f"{result['first_name']} {result['last_name']}"
        elif result['first_name']:
            result['full_name'] = result['first_name']
        elif result['last_name']:
            result['full_name'] = result['last_name']
   
    return result

def infer_title_from_name(name: str, current_title: str, config: Dict) -> Optional[str]:
    if current_title and str(current_title).strip():
        return current_title
   
    if not name:
        return None
   
    name_lower = str(name).lower().strip()
    title_prefixes = config.get("title_prefixes", {})
   
    for category, data in title_prefixes.items():
        patterns = data.get("patterns", [])
        title = data.get("title", "")
       
        for pattern in patterns:
            if pattern in name_lower or name_lower.startswith(pattern.strip()):
                return title
   
    return None

def clean_job_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return None
   
    title = title.strip()
    if not title:
        return None
   
    title = title.split(' at ')[0]
    title = title.split(' |')[0]
    title = title.split('|')[0]
    title = re.sub(r'\([^)]*\)', '', title)
    title = re.sub(r'\[[^\]]*\]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
   
    if not title:
        return None
   
    return title

def is_valid_person_record(first_name: str, last_name: str, full_name: str) -> bool:
    return any([
        first_name and str(first_name).strip(),
        last_name and str(last_name).strip(),
        full_name and str(full_name).strip()
    ])

def extract_company_info_from_rolodex(record: Dict) -> Dict:
    companies = record.get('companies', [])
    if not companies:
        return {
            'primary_company_name': None,
            'primary_company_id': None,
            'all_companies': None,
            'all_company_ids': None,
            'works_at_multiple': []
        }
   
    company_names = [c.get('name') for c in companies if c.get('name')]
    company_ids = [c.get('id') for c in companies if c.get('id')]
   
    result = {
        'primary_company_name': company_names[0] if company_names else None,
        'primary_company_id': company_ids[0] if company_ids else None,
        'all_companies': ', '.join(company_names) if company_names else None,
        'all_company_ids': ', '.join(company_ids) if company_ids else None,
        'works_at_multiple': company_names if len(company_names) > 1 else []
    }
   
    return result

def simple_person_match(rec1: Dict, rec2: Dict, config: Dict) -> bool:
    rolodex_id1 = rec1.get('rolodex_id')
    rolodex_id2 = rec2.get('rolodex_id')

    # Never match if different non-null rolodex_ids
    if rolodex_id1 and rolodex_id2 and rolodex_id1 != rolodex_id2:
        return False

    # Match if same rolodex_id (non-null)
    if rolodex_id1 and rolodex_id2 and rolodex_id1 == rolodex_id2:
        return True

    # For cross-system: if one has rolodex_id and the other references it
    if rolodex_id1 and not rolodex_id2:
        return rec2.get('rolodex_id') == rolodex_id1
    if rolodex_id2 and not rolodex_id1:
        return rec1.get('rolodex_id') == rolodex_id2

    return False

def merge_records(records: List[Dict]) -> Dict:
    if len(records) == 1:
        single_rec = records[0].copy()
        companies = single_rec.get('all_companies', '') or ''
        companies_list = [c.strip() for c in companies.split(',') if c.strip()] if companies else []
        single_rec['works_at_multiple'] = companies_list if len(companies_list) > 1 else []
        return single_rec
   
    # Priority: Rolodex > ColourCoats > Metalia (most complete data first)
    def record_priority(record):
        source = record.get('data_source', '')
        if 'Rolodex' in source: return 3
        elif 'ColourcoatsBigin' in source: return 2
        elif 'MetaliaBigin' in source: return 1
        return 0
   
    sorted_records = sorted(records, key=record_priority, reverse=True)
    merged = sorted_records[0].copy()
   
    # Set data_source to show all sources
    sources = set(r['data_source'] for r in records)
    merged['data_source'] = '+'.join(sorted(sources))
   
    # Collect all company data
    all_companies = set()
    all_company_ids = set()
    all_emails = []
   
    for record in sorted_records:
        # Merge companies
        companies = record.get('all_companies', '') or ''
        if companies:
            for company in companies.split(','):
                if company.strip():
                    all_companies.add(company.strip())
       
        company_ids = record.get('all_company_ids', '') or ''
        if company_ids:
            for cid in company_ids.split(','):
                if cid.strip():
                    all_company_ids.add(cid.strip())
       
        # Merge emails
        if record.get('email'):
            all_emails.append(record['email'])
        all_emails.extend(record.get('all_emails', []))
       
        # Merge other fields - prefer non-null values
        for field, value in record.items():
            if field in ['person_id', 'data_source', 'primary_company_name', 'primary_company_id',
                        'all_companies', 'all_company_ids', 'works_at_multiple', 'email', 'all_emails']:
                continue
           
            if merged.get(field) is None and value is not None:
                merged[field] = value
   
    # Set final company and email data
    sorted_companies = sorted(list(all_companies))
    merged['primary_company_name'] = sorted_companies[0] if sorted_companies else None
    merged['all_companies'] = ', '.join(sorted_companies) if sorted_companies else None
    merged['all_company_ids'] = ', '.join(sorted(all_company_ids)) if all_company_ids else None
    merged['works_at_multiple'] = sorted_companies if len(sorted_companies) > 1 else []
   
    unique_emails = dict.fromkeys(all_emails)
    sorted_emails = list(unique_emails.keys())
    merged['email'] = sorted_emails[0] if sorted_emails else None
    merged['all_emails'] = sorted_emails if len(sorted_emails) > 1 else []
   
    return merged

def find_matches(records: List[Dict], config: Dict) -> List[List[Dict]]:
    G = nx.Graph()
    for i, rec in enumerate(records):
        G.add_node(i)
    for i in range(len(records)):
        for j in range(i + 1, len(records)):
            if simple_person_match(records[i], records[j], config):
                G.add_edge(i, j)
    groups = []
    for component in nx.connected_components(G):
        group = [records[idx] for idx in component]
        groups.append(group)
    return groups

def load_data(file_path: str) -> List[Dict]:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else data.get('data', [])
    except:
        return []

def load_mapper(base_dir: str) -> Dict:
    mapper_path = os.path.join(base_dir, "mapper", "unified_Personnel_mapper.json")
    try:
        with open(mapper_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data["Unified_Personnel"]["columns"]
    except FileNotFoundError as e:
        print(f"Error: Mapper file not found at {mapper_path}")
        return {}
    except KeyError as e:
        print(f"Error: Missing key in mapper file - {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in mapper file - {e}")
        return {}
    except Exception as e:
        print(f"Error loading mapper: {e}")
        return {}

def create_unified_record(raw_record: Dict, mappings: Dict, source_config: Dict, config: Dict) -> Optional[Dict]:
    source_name = source_config['name']
   
    unified = {
        'person_id': str(uuid.uuid4()),
        'data_source': source_name,
    }
   
    # Set system-specific IDs
    if source_name == 'Rolodex':
        unified['rolodex_id'] = str(raw_record.get('id', ''))
        unified['colourcoats_bigin_id'] = None
        unified['metalia_bigin_id'] = None
    elif source_name == 'ColourcoatsBigin':
        unified['colourcoats_bigin_id'] = str(raw_record.get('id', ''))
        unified['rolodex_id'] = raw_record.get('Rolodex_Contact_ID')
        unified['metalia_bigin_id'] = None
    elif source_name == 'MetaliaBigin':
        unified['metalia_bigin_id'] = str(raw_record.get('id', ''))
        unified['rolodex_id'] = None  # Explicitly None, as no field
        unified['colourcoats_bigin_id'] = None
   
    # Map other fields
    for field, mapping in mappings.items():
        if field in ['person_id', 'data_source', 'rolodex_id', 'colourcoats_bigin_id', 'metalia_bigin_id',
                    'primary_company_name', 'primary_company_id', 'all_companies', 'all_company_ids', 'works_at_multiple']:
            continue
       
        path = mapping.get(source_name)
        if path and path != 'system generated':
            if source_config['type'] == 'bigin':
                clean_path = path.replace('Contacts.', '')
            elif source_config['type'] == 'rolodex':
                clean_path = path.replace('Rolodex_contacts.', '')
            else:
                clean_path = path
           
            unified[field] = safe_get_value(raw_record, clean_path)
   
    # Handle company information
    if source_name == 'Rolodex':
        company_info = extract_company_info_from_rolodex(raw_record)
        unified.update(company_info)
       
        # Handle emails
        emails = [e.get('email_address') for e in raw_record.get('emails', []) if e.get('email_address')]
        unified['all_emails'] = emails if len(emails) > 1 else []
        unified['email'] = emails[0] if emails else None
       
        # Handle phone numbers
        phones = [p.get('phone_number') for p in raw_record.get('phone_numbers', []) if p.get('phone_number')]
        norm_phones = [normalize_phone(p, unified.get('country'), config) for p in phones]
        norm_phones = [p for p in norm_phones if p]
        unified['all_phones'] = norm_phones
        unified['mobile'] = norm_phones[0] if norm_phones else None
        unified['alternate_mobile'] = norm_phones[1] if len(norm_phones) > 1 else None
    else:
        # Bigin systems
        account_name = safe_get_value(raw_record, 'Account_Name')
        name_str = None
        id_str = None
        if isinstance(account_name, dict):
            name_str = account_name.get('name')
            id_str = account_name.get('id')
        else:
            name_str = account_name
       
        unified['primary_company_name'] = name_str
        unified['primary_company_id'] = id_str
        unified['all_companies'] = name_str
        unified['all_company_ids'] = id_str
        unified['works_at_multiple'] = []
       
        # Emails
        email = unified.get('email')
        unified['all_emails'] = [email] if email else []
       
        # Phones
        mobile = unified.get('mobile')
        alt_mobile = unified.get('alternate_mobile')
        all_phones = []
        if mobile:
            all_phones.append(mobile)
        if alt_mobile:
            all_phones.append(alt_mobile)
        unified['all_phones'] = all_phones
   
    # Process names
    name_result = process_names(
        unified.get('first_name', ''),
        unified.get('last_name', ''),
        unified.get('full_name', ''),
        config
    )
   
    unified['first_name'] = name_result['first_name']
    unified['last_name'] = name_result['last_name']
    unified['full_name'] = name_result['full_name']
   
    # Process title
    current_title = unified.get('title')
    if name_result['inferred_title'] and not current_title:
        unified['title'] = name_result['inferred_title']
    else:
        combined_name = f"{unified.get('first_name', '')} {unified.get('last_name', '')} {unified.get('full_name', '')}".strip()
        inferred_title = infer_title_from_name(combined_name, current_title, config)
        if inferred_title:
            unified['title'] = inferred_title
   
    if unified.get('title'):
        unified['title'] = clean_job_title(unified['title'])
   
    # Validate record
    if not is_valid_person_record(
        unified.get('first_name'),
        unified.get('last_name'),
        unified.get('full_name')
    ):
        return None
   
    # Normalize phone numbers (already done for Rolodex above, for Bigin here if needed)
    country = unified.get('country')
    if unified.get('mobile'):
        unified['mobile'] = normalize_phone(unified['mobile'], country, config)
    if unified.get('alternate_mobile'):
        unified['alternate_mobile'] = normalize_phone(unified['alternate_mobile'], country, config)
   
    return unified

def main():
    base_dir = r"C:\Projects\Junior AI Engineer Task\Task_Assignment\schema_design"
   
    config = load_config(base_dir)
    mappings = load_mapper(base_dir)
   
    if not mappings:
        print("Error: Could not load mappings")
        return
   
    print("Starting ID-based personnel unification...")
   
    sources = [
        {'name': 'ColourcoatsBigin', 'type': 'bigin', 'dir': 'ColourCoatsBigin', 'file': 'Contacts.json'},
        {'name': 'MetaliaBigin', 'type': 'bigin', 'dir': 'MetaliaBigin', 'file': 'Contacts.json'},
        {'name': 'Rolodex', 'type': 'rolodex', 'dir': 'Rolodex_data', 'file': 'contacts.json'}
    ]
   
    all_records = []
    for source in sources:
        print(f"Processing {source['name']}...")
       
        file_path = os.path.join(base_dir, "Raw_Data", source['dir'], source['file'])
        raw_data = load_data(file_path)
       
        valid_count = 0
        for raw_record in raw_data:
            unified = create_unified_record(raw_record, mappings, source, config)
            if unified:
                all_records.append(unified)
                valid_count += 1
       
        print(f"Loaded {valid_count} records from {source['name']}")
   
    print(f"Total records before matching: {len(all_records)}")
   
    match_groups = find_matches(all_records, config)
    print(f"Found {len(match_groups)} groups to merge")
   
    # Debug matching results
    for i, group in enumerate(match_groups[:3]):
        print(f"\nMatch Group {i+1}: {len(group)} records")
        for rec in group:
            name = rec.get('full_name', 'Unknown')
            rolodex_id = rec.get('rolodex_id', 'None')
            source = rec.get('data_source', 'Unknown')
            print(f" - {name} [Rolodex ID: {rolodex_id}] from {source}")
   
    final_records = []
    processed_ids = set()
   
    for group in match_groups:
        if any(r['person_id'] in processed_ids for r in group):
            continue
       
        merged = merge_records(group)
        final_records.append(merged)
       
        for record in group:
            processed_ids.add(record['person_id'])
   
    for record in all_records:
        if record['person_id'] not in processed_ids:
            # Ensure works_at_multiple is properly formatted for unmatched records
            companies = record.get('all_companies', '') or ''
            companies_list = [c.strip() for c in companies.split(',') if c.strip()] if companies else []
            record['works_at_multiple'] = companies_list if len(companies_list) > 1 else []
            final_records.append(record)
   
    print(f"Final unified records: {len(final_records)}")
    reduction = len(all_records) - len(final_records)
    print(f"Merged {reduction} duplicate records")
   
    # Create DataFrame and save
    expected_columns = list(mappings.keys())
   
    df = pd.DataFrame(final_records)
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
   
    df = df[expected_columns]
    df = df.sort_values(['data_source', 'full_name'], na_position='last')
   
    output_path = os.path.join(base_dir, "unified_personnel.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} unified records to unified_personnel.csv")
   
    # Summary stats
    source_counts = df['data_source'].value_counts()
    print(f"\nRecords by source:")
    for source, count in source_counts.items():
        print(f" {source}: {count}")

if __name__ == "__main__":
    main()