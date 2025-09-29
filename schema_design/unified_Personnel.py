import os
import json
import pandas as pd
import uuid
import re
from typing import Dict, List, Optional, Tuple, Any
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

def safe_get_value(record: Dict, path: str) -> Any:
    """Generic path extractor that handles arrays with [*] notation"""
    if not path or not record:
        return None
   
    try:
        # Handle [*] for all array elements
        if '[*]' in path:
            parts = path.split('[*]')
            base_path = parts[0]
            
            # Get the base array
            current = record
            for part in base_path.split('.'):
                if not part:
                    continue
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
            
            # Extract from all array elements
            if not isinstance(current, list):
                return None
            
            # Get field from each element
            field_path = parts[1].lstrip('.')
            results = []
            for item in current:
                if field_path:
                    val = safe_get_value(item, field_path)
                else:
                    val = item
                if val:
                    results.append(val)
            return results if results else None
        
        # Handle .split() for location parsing
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
        
        # Standard path navigation
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

def clean_job_title(title: str) -> Optional[str]:
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
   
    return title if title else None

def is_valid_person_record(first_name: str, last_name: str, full_name: str) -> bool:
    return any([
        first_name and str(first_name).strip(),
        last_name and str(last_name).strip(),
        full_name and str(full_name).strip()
    ])

def extract_company_relationships(raw_record: Dict, person_id: str, source_name: str) -> List[Dict]:
    """Extract company relationships from source data"""
    relationships = []
    
    if source_name == 'Rolodex':
        # Rolodex has array of companies with metadata
        companies = raw_record.get('companies', [])
        for company in companies:
            if company.get('name') or company.get('id'):
                rel = {
                    'relationship_id': str(uuid.uuid4()),
                    'person_id': person_id,
                    'company_id': company.get('id'),
                    'company_name': company.get('name'),
                    'is_active': company.get('is_active'),
                    'start_date': company.get('start_date'),
                    'title_at_company': company.get('title'),
                    'data_source': source_name
                }
                relationships.append(rel)
    
    elif source_name in ['ColourcoatsBigin', 'MetaliaBigin']:
        # Bigin has single Account_Name
        account_name = raw_record.get('Account_Name')
        if account_name:
            company_name = None
            company_id = None
            
            if isinstance(account_name, dict):
                company_name = account_name.get('name')
                company_id = account_name.get('id')
            else:
                company_name = account_name
            
            if company_name or company_id:
                rel = {
                    'relationship_id': str(uuid.uuid4()),
                    'person_id': person_id,
                    'company_id': company_id,
                    'company_name': company_name,
                    'is_active': None,
                    'start_date': None,
                    'title_at_company': None,
                    'data_source': source_name
                }
                relationships.append(rel)
    
    return relationships

def simple_person_match(rec1: Dict, rec2: Dict) -> bool:
    """
    CRITICAL: Match ONLY by rolodex_id.
    Same name does NOT mean same person - MUST have matching IDs.
    Different IDs = different people, even if names match.
    """
    rolodex_id1 = rec1.get('rolodex_id')
    rolodex_id2 = rec2.get('rolodex_id')

    # Never match if different non-null rolodex_ids
    if rolodex_id1 and rolodex_id2 and str(rolodex_id1) != str(rolodex_id2):
        return False

    # Match if same rolodex_id (non-null)
    if rolodex_id1 and rolodex_id2 and str(rolodex_id1) == str(rolodex_id2):
        return True

    # No match - cannot link by IDs
    return False

def merge_records(records: List[Dict], mappings: Dict) -> Dict:
    """Merge records that matched by ID"""
    if len(records) == 1:
        return records[0].copy()
   
    # Priority: Rolodex > ColourCoats > Metalia
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
   
    # Merge all fields from all records
    for record in sorted_records[1:]:
        for field, value in record.items():
            if field in ['person_id', 'data_source']:
                continue
           
            field_config = mappings.get(field, {})
            field_type = field_config.get('type', 'string')
           
            if field_type == 'array':
                if field not in merged or not merged[field]:
                    merged[field] = value
                elif value:
                    existing = merged[field] if isinstance(merged[field], list) else []
                    new_vals = value if isinstance(value, list) else []
                    combined = existing + [v for v in new_vals if v not in existing]
                    merged[field] = combined if combined else []
            else:
                if merged.get(field) is None and value is not None:
                    merged[field] = value
   
    return merged

def merge_company_relationships(relationships: List[Dict]) -> List[Dict]:
    """Merge company relationships from multiple sources, keeping unique ones"""
    # Group by person_id + company_id/name combo
    seen = {}
    merged = []
    
    for rel in relationships:
        # Create key based on person + company
        key = (rel['person_id'], rel.get('company_id'), rel.get('company_name'))
        
        if key not in seen:
            seen[key] = rel
            merged.append(rel)
        else:
            # Merge metadata - prefer Rolodex data
            existing = seen[key]
            if rel['data_source'] == 'Rolodex':
                # Rolodex has more metadata, use it
                for field in ['is_active', 'start_date', 'title_at_company']:
                    if rel.get(field) and not existing.get(field):
                        existing[field] = rel[field]
            
            # Combine data sources
            sources = set(existing['data_source'].split('+'))
            sources.add(rel['data_source'])
            existing['data_source'] = '+'.join(sorted(sources))
    
    return merged

def find_matches(records: List[Dict]) -> List[List[Dict]]:
    """Find matching records using graph-based clustering"""
    G = nx.Graph()
    for i, rec in enumerate(records):
        G.add_node(i)
   
    for i in range(len(records)):
        for j in range(i + 1, len(records)):
            if simple_person_match(records[i], records[j]):
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
        return data
    except Exception as e:
        print(f"Error loading mapper: {e}")
        return {}

def create_unified_record(raw_record: Dict, mappings: Dict, source_name: str, config: Dict) -> Tuple[Optional[Dict], List[Dict]]:
    """Create unified personnel record AND extract company relationships"""
    
    unified = {}
   
    # Process each field from Unified_Personnel mapper
    for field, field_config in mappings.items():
        source_path = field_config.get(source_name)
       
        if source_path == 'system generated':
            if field == 'person_id':
                unified[field] = str(uuid.uuid4())
            elif field == 'data_source':
                unified[field] = source_name
            continue
       
        if source_path == 'computed':
            unified[field] = None
            continue
       
        if source_path is None or source_path == 'null':
            unified[field] = None
            continue
       
        value = safe_get_value(raw_record, source_path)
       
        field_type = field_config.get('type', 'string')
       
        if field_type == 'array':
            if isinstance(value, list):
                unified[field] = value
            elif value:
                unified[field] = [value]
            else:
                unified[field] = []
        else:
            unified[field] = value
   
    # Post-processing: names
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
        return None, []
   
    # Normalize phones
    country = unified.get('country')
    if unified.get('mobile'):
        unified['mobile'] = normalize_phone(unified['mobile'], country, config)
    if unified.get('alternate_mobile'):
        unified['alternate_mobile'] = normalize_phone(unified['alternate_mobile'], country, config)
    if unified.get('all_phones'):
        unified['all_phones'] = [normalize_phone(p, country, config) for p in unified['all_phones'] if p]
        unified['all_phones'] = [p for p in unified['all_phones'] if p]
   
    # Compute all_emails
    email = unified.get('email')
    all_emails = unified.get('all_emails', [])
    if isinstance(all_emails, list) and len(all_emails) > 1:
        pass
    elif email and (not all_emails or len(all_emails) <= 1):
        unified['all_emails'] = [email] if email else []
   
    # Extract company relationships separately
    person_id = unified['person_id']
    company_relationships = extract_company_relationships(raw_record, person_id, source_name)
   
    return unified, company_relationships

def main():
    base_dir = r"C:\Projects\Junior AI Engineer Task\Task_Assignment\schema_design"
   
    config = load_config(base_dir)
    mapper_data = load_mapper(base_dir)
   
    if not mapper_data:
        print("Error: Could not load mappings")
        return
   
    personnel_mappings = mapper_data.get("Unified_Personnel", {}).get("columns", {})
    
    if not personnel_mappings:
        print("Error: No Unified_Personnel mappings found")
        return
   
    print("Starting normalized personnel unification (2-file output)...")
   
    sources = [
        {'name': 'ColourcoatsBigin', 'dir': 'ColourCoatsBigin', 'file': 'Contacts.json'},
        {'name': 'MetaliaBigin', 'dir': 'MetaliaBigin', 'file': 'Contacts.json'},
        {'name': 'Rolodex', 'dir': 'Rolodex_data', 'file': 'contacts.json'}
    ]
   
    all_personnel_records = []
    all_company_relationships = []
    
    for source in sources:
        print(f"Processing {source['name']}...")
       
        file_path = os.path.join(base_dir, "Raw_Data", source['dir'], source['file'])
        raw_data = load_data(file_path)
       
        valid_count = 0
        for raw_record in raw_data:
            person_record, company_rels = create_unified_record(
                raw_record, personnel_mappings, source['name'], config
            )
            if person_record:
                all_personnel_records.append(person_record)
                all_company_relationships.extend(company_rels)
                valid_count += 1
       
        print(f"  Loaded {valid_count} valid records")
   
    print(f"\nTotal personnel records before matching: {len(all_personnel_records)}")
    print(f"Total company relationships extracted: {len(all_company_relationships)}")
   
    # Find and merge personnel matches
    match_groups = find_matches(all_personnel_records)
    print(f"Found {len(match_groups)} personnel groups after ID matching")
   
    final_personnel = []
    old_to_new_person_id = {}  # Map old person_ids to merged person_id
    
    for group in match_groups:
        merged = merge_records(group, personnel_mappings)
        final_personnel.append(merged)
        
        # Track ID mappings for company relationships
        new_person_id = merged['person_id']
        for record in group:
            old_to_new_person_id[record['person_id']] = new_person_id
   
    print(f"Final personnel records: {len(final_personnel)}")
    reduction = len(all_personnel_records) - len(final_personnel)
    print(f"Merged {reduction} duplicate personnel records")
   
    # Update company relationships with merged person_ids
    for rel in all_company_relationships:
        old_id = rel['person_id']
        if old_id in old_to_new_person_id:
            rel['person_id'] = old_to_new_person_id[old_id]
   
    # Merge duplicate company relationships
    final_company_relationships = merge_company_relationships(all_company_relationships)
    print(f"Final company relationships: {len(final_company_relationships)}")
   
    # Create DataFrames
    personnel_df = pd.DataFrame(final_personnel)
    for col in personnel_mappings.keys():
        if col not in personnel_df.columns:
            personnel_df[col] = None
    personnel_df = personnel_df[list(personnel_mappings.keys())]
    personnel_df = personnel_df.sort_values(['data_source', 'full_name'], na_position='last')
   
    companies_df = pd.DataFrame(final_company_relationships)
    company_columns = ['relationship_id', 'person_id', 'company_id', 'company_name', 
                       'is_active', 'start_date', 'title_at_company', 'data_source']
    for col in company_columns:
        if col not in companies_df.columns:
            companies_df[col] = None
    companies_df = companies_df[company_columns]
    companies_df = companies_df.sort_values(['person_id', 'company_name'], na_position='last')
   
    # Save both files
    personnel_path = os.path.join(base_dir, "unified_personnel.csv")
    companies_path = os.path.join(base_dir, "person_companies.csv")
    
    personnel_df.to_csv(personnel_path, index=False)
    companies_df.to_csv(companies_path, index=False)
    
    print(f"\n✓ Saved {len(personnel_df)} personnel records to unified_personnel.csv")
    print(f"✓ Saved {len(companies_df)} company relationships to person_companies.csv")
   
    # Summary
    print(f"\nPersonnel by source:")
    for source, count in personnel_df['data_source'].value_counts().items():
        print(f"  {source}: {count}")
    
    print(f"\nCompany relationships by source:")
    for source, count in companies_df['data_source'].value_counts().items():
        print(f"  {source}: {count}")
   
    print("\n✓ Normalized unification complete!")
    print("  → Query people: unified_personnel.csv")
    print("  → Query companies: person_companies.csv")
    print("  → Join on: person_id")

if __name__ == "__main__":
    main()