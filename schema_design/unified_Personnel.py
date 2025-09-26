import os
import json
import pandas as pd
import uuid
import re
from typing import Dict, List, Optional, Tuple
import phonenumbers
from phonenumbers import PhoneNumberFormat, NumberParseException

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

def clean_company_name(company: str, config: Dict) -> str:
    if not company:
        return ""
    
    company = str(company).strip().lower()
    company = re.sub(r'[&+]', '', company)
    
    indicators = config.get("company_indicators", [])
    for indicator in indicators:
        patterns = [
            rf'\s+{re.escape(indicator)}\.?$',
            rf'\s+{re.escape(indicator)}\s+',
            rf'^{re.escape(indicator)}\s+'
        ]
        for pattern in patterns:
            company = re.sub(pattern, ' ', company, flags=re.IGNORECASE)
    
    company = re.sub(r'[^\w\s]', '', company)
    return re.sub(r'\s+', ' ', company.strip())

def normalize_name_for_matching(name: str) -> str:
    if not name:
        return ""
    return re.sub(r'\s+', ' ', str(name).strip().lower())

def is_valid_person_record(first_name: str, last_name: str, full_name: str) -> bool:
    return any([
        first_name and str(first_name).strip(),
        last_name and str(last_name).strip(),
        full_name and str(full_name).strip()
    ])

def extract_company_info_from_rolodex(record: Dict) -> Dict:
    companies = record.get('companies', [])
    if not companies:
        return {'Company name': None, 'Company_Id': None, 'works_at_multiple': 'no'}
    
    company_names = [c.get('name') for c in companies if c.get('name')]
    company_ids = [c.get('id') for c in companies if c.get('id')]
    
    name_str = ', '.join(company_names) if company_names else None
    id_str = ', '.join(company_ids) if company_ids else None
    multiple = 'yes' if len(company_names) > 1 else 'no'
    
    return {
        'Company name': name_str,
        'Company_Id': id_str,
        'works_at_multiple': multiple
    }

def get_all_companies(rec: Dict, config: Dict) -> set:
    companies = set()
    if rec['Company name']:
        for name in rec['Company name'].split(', '):
            cleaned = clean_company_name(name, config)
            if cleaned:
                companies.add(cleaned)
    return companies

def exact_person_match(rec1: Dict, rec2: Dict, config: Dict) -> bool:
    # Check rolodex_contact_id match
    id1 = rec1.get('rolodex_contact_id')
    id2 = rec2.get('rolodex_contact_id')
    if id1 and id2 and id1 == id2:
        return True
    
    # Check email match
    email1 = rec1.get('email')
    email2 = rec2.get('email')
    if email1 and email2 and email1.lower() == email2.lower():
        return True
    
    # Check mobile match
    mobile1 = rec1.get('mobile')
    mobile2 = rec2.get('mobile')
    if mobile1 and mobile2 and mobile1 == mobile2:
        return True
    
    # Check normalized name and overlapping companies
    name1 = rec1.get('full_name')
    if not name1:
        first1 = rec1.get('first_name') or ''
        last1 = rec1.get('last_name') or ''
        name1 = f"{first1} {last1}".strip()
    
    name2 = rec2.get('full_name')
    if not name2:
        first2 = rec2.get('first_name') or ''
        last2 = rec2.get('last_name') or ''
        name2 = f"{first2} {last2}".strip()
    
    if not name1 or not name2:
        return False
    
    norm_name1 = normalize_name_for_matching(name1)
    norm_name2 = normalize_name_for_matching(name2)
    
    if norm_name1 != norm_name2:
        return False
    
    companies1 = get_all_companies(rec1, config)
    companies2 = get_all_companies(rec2, config)
    
    if companies1 & companies2:
        return True
    
    return False

def find_matches(records: List[Dict], config: Dict) -> List[List[Dict]]:
    match_groups = []
    used_indices = set()
    
    for i, rec1 in enumerate(records):
        if i in used_indices:
            continue
            
        group = [rec1]
        used_indices.add(i)
        
        for j, rec2 in enumerate(records[i+1:], i+1):
            if j in used_indices:
                continue
                
            if exact_person_match(rec1, rec2, config):
                group.append(rec2)
                used_indices.add(j)
        
        if len(group) > 1:
            match_groups.append(group)
    
    return match_groups

def merge_records(records: List[Dict]) -> Dict:
    if len(records) == 1:
        return records[0]
    
    merged = records[0].copy()
    sources = set(r['data_source'] for r in records)
    merged['data_source'] = '+'.join(sorted(sources))
    
    # Merge ID fields
    for record in records[1:]:
        for key, value in record.items():
            if key.endswith('_id') and value and not merged.get(key):
                merged[key] = value
    
    # Merge other fields, prefer non-null
    for record in records[1:]:
        for field, value in record.items():
            if field in ['person_id', 'data_source'] or field.endswith('_id') or field in ['Company name', 'Company_Id', 'works_at_multiple']:
                continue
            if merged.get(field) is None and value is not None:
                merged[field] = value
    
    # Merge companies
    all_names = set()
    all_ids = set()
    for record in records:
        if record['Company name']:
            for name in record['Company name'].split(', '):
                if name.strip():
                    all_names.add(name.strip())
        if record['Company_Id']:
            for cid in record['Company_Id'].split(', '):
                if cid.strip():
                    all_ids.add(cid.strip())
    
    merged['Company name'] = ', '.join(sorted(all_names)) if all_names else None
    merged['Company_Id'] = ', '.join(sorted(all_ids)) if all_ids else None
    merged['works_at_multiple'] = 'yes' if len(all_names) > 1 else 'no'
    
    return merged

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
    
    if source_name == 'Rolodex':
        unified['rolodex_id'] = str(raw_record.get('id', ''))
    elif 'Bigin' in source_name:
        bigin_key = f"{source_name.lower().replace('bigin', '_bigin')}_id"
        unified[bigin_key] = str(raw_record.get('id', ''))
    
    for field, mapping in mappings.items():
        if field in ['person_id', 'data_source', 'Company name', 'Company_Id', 'works_at_multiple']:
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
    
    if source_name == 'Rolodex':
        company_info = extract_company_info_from_rolodex(raw_record)
        unified['Company name'] = company_info['Company name']
        unified['Company_Id'] = company_info['Company_Id']
        unified['works_at_multiple'] = company_info['works_at_multiple']
    else:
        account_name = safe_get_value(raw_record, 'Account_Name')
        name_str = None
        id_str = None
        multiple = 'no'
        if isinstance(account_name, dict):
            name_str = account_name.get('name')
            id_str = account_name.get('id')
        else:
            name_str = account_name
        unified['Company name'] = name_str
        unified['Company_Id'] = id_str
        unified['works_at_multiple'] = multiple
    
    name_result = process_names(
        unified.get('first_name', ''),
        unified.get('last_name', ''),
        unified.get('full_name', ''),
        config
    )
    
    unified['first_name'] = name_result['first_name']
    unified['last_name'] = name_result['last_name']
    unified['full_name'] = name_result['full_name']
    
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
    
    if not is_valid_person_record(
        unified.get('first_name'), 
        unified.get('last_name'), 
        unified.get('full_name')
    ):
        return None
    
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
    
    print("Starting personnel unification...")
    
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
    
    print(f"Total records: {len(all_records)}")
    
    match_groups = find_matches(all_records, config)
    print(f"Found {len(match_groups)} duplicate groups")
    
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
            final_records.append(record)
    
    print(f"Final unified records: {len(final_records)}")
    
    expected_columns = list(mappings.keys())
    source_id_columns = ['rolodex_id', 'colourcoats_bigin_id', 'metalia_bigin_id']
    
    final_columns = expected_columns + [col for col in source_id_columns if col not in expected_columns]
    
    df = pd.DataFrame(final_records)
    for col in final_columns:
        if col not in df.columns:
            df[col] = None
    
    df = df[final_columns]
    df = df.sort_values(['data_source', 'full_name'], na_position='last')
    
    output_path = os.path.join(base_dir, "unified_personnel.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} unified records to unified_personnel.csv")

if __name__ == "__main__":
    main()