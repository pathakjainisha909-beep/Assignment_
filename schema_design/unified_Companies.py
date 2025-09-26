import os
import json
import pandas as pd
import uuid
from typing import Dict, List, Any, Optional, Tuple
import re

def safe_get_value(record: Dict[str, Any], path: str) -> Optional[Any]:
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
                parts = value.split(', ')
                return parts[index] if len(parts) > index else None
            return None
        else:
            keys = path.split('.')
            current = record
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            return current if current not in [None, '', []] else None
    except:
        return None

def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r'\s+', ' ', name.lower().strip())
    name = re.sub(r'\s*(pvt|private)?\s*(ltd|limited)\.?', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*llc\.?', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*inc\.?', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*corp\.?', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*group', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return re.sub(r'\s+', ' ', name.strip())

def exact_normalized_company_match(rec1: Dict, rec2: Dict) -> bool:
    name1 = rec1.get('company_name')
    name2 = rec2.get('company_name')
    if not name1 or not name2:
        return False
    return normalize_company_name(name1) == normalize_company_name(name2)

def load_data_file(file_path: str) -> List[Dict[str, Any]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'data' in data:
            return data['data']
        else:
            return []
    except:
        return []

def find_matches(list1: List[Dict], list2: List[Dict]) -> List[Tuple[int, int]]:
    matches = []
    used2 = set()
    for i, rec1 in enumerate(list1):
        for j, rec2 in enumerate(list2):
            if j in used2:
                continue
            if exact_normalized_company_match(rec1, rec2):
                matches.append((i, j))
                used2.add(j)
                break
    return matches

def merge_records(rec1: Dict, rec2: Dict, id_keys: List[str]) -> Dict:
    merged = rec1.copy()
    merged['data_source'] = f"{rec1['data_source']}+{rec2['data_source']}"
    
    for key in id_keys:
        if rec2.get(key):
            merged[key] = rec2[key]
    
    for field, value in rec2.items():
        if field in ['company_id', 'data_source'] or field in id_keys:
            continue
        if merged.get(field) is None and value is not None:
            merged[field] = value
    return merged

def create_unified_company(record: Dict, mappings: Dict, source_config: Dict) -> Dict:
    source_name = source_config['name']
    
    unified = {
        'company_id': str(uuid.uuid4()),
        'data_source': source_name,
    }
    
    if source_name == 'Rolodex':
        unified['rolodex_id'] = str(safe_get_value(record, 'id'))
    elif 'Bigin' in source_name:
        bigin_key = f"{source_name.lower().replace('bigin', '_bigin')}_id"
        unified[bigin_key] = str(safe_get_value(record, 'id'))
    
    for field, mapping in mappings.items():
        if field in ['company_id', 'data_source']:
            continue
        
        path = mapping.get(source_name)
        value = None
        
        if path:
            if source_name == 'Rolodex':
                clean_path = path.replace('companies.', '') if 'companies.' in path else path
            elif 'Bigin' in source_name:
                clean_path = path.replace('Accounts.', '') if 'Accounts.' in path else path
            else:
                clean_path = path
            value = safe_get_value(record, clean_path)
        
        unified[field] = value
    return unified

def chain_merge(group_lists: List[Tuple[str, List[Dict]]], id_keys: List[str]) -> List[Dict]:
    if not group_lists:
        return []
    
    current = group_lists[0][1]
    for _, next_list in group_lists[1:]:
        matches = find_matches(current, next_list)
        new_current = []
        matched_next = set()
        
        for i, rec in enumerate(current):
            found = False
            for c_idx, n_idx in matches:
                if c_idx == i:
                    merged = merge_records(rec, next_list[n_idx], id_keys)
                    new_current.append(merged)
                    matched_next.add(n_idx)
                    found = True
                    break
            if not found:
                new_current.append(rec)
        
        for i, rec in enumerate(next_list):
            if i not in matched_next:
                new_current.append(rec)
        
        current = new_current
    return current

def unify_companies(source_configs: List[Dict]):
    base_dir = r"C:\Projects\Junior AI Engineer Task\Task_assignment\schema_design"
    mapper_path = os.path.join(base_dir, "mapper", "Unified_Companies_mapper.json")
    
    try:
        with open(mapper_path, 'r', encoding='utf-8') as f:
            mappings_data = json.load(f)
        mappings = mappings_data["Unified_Companies"]["columns"]
    except FileNotFoundError as e:
        print(f"Error: Mapper file not found at {mapper_path}")
        return
    except KeyError as e:
        print(f"Error: Missing key in mapper file - {e}")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in mapper file - {e}")
        return
    except Exception as e:
        print(f"Error loading mapper: {e}")
        return

    print("Processing company data sources...")
    all_id_keys = ['rolodex_id', 'colourcoats_bigin_id', 'metalia_bigin_id']
    ordered_types = ['bigin', 'rolodex']
    combined = None
    total_records = 0

    for source_type in ordered_types:
        type_configs = [c for c in source_configs if c['type'] == source_type]
        if not type_configs:
            continue
        
        type_lists = []
        for config in type_configs:
            path = os.path.join(base_dir, "Raw_Data", config['dir'], config['file'])
            records = load_data_file(path)
            unified = [create_unified_company(rec, mappings, config) for rec in records if rec]
            total_records += len(unified)
            type_lists.append((config['name'], unified))
            print(f"Loaded {len(unified)} records from {config['name']}")
        
        combined_type = chain_merge(type_lists, all_id_keys)
        
        if combined is None:
            combined = combined_type
        else:
            matches = find_matches(combined, combined_type)
            new_combined = []
            matched_type = set()
            
            for i, rec in enumerate(combined):
                found = False
                for c_idx, t_idx in matches:
                    if c_idx == i:
                        merged = merge_records(rec, combined_type[t_idx], all_id_keys)
                        new_combined.append(merged)
                        matched_type.add(t_idx)
                        found = True
                        break
                if not found:
                    new_combined.append(rec)
            
            for i, rec in enumerate(combined_type):
                if i not in matched_type:
                    new_combined.append(rec)
            
            combined = new_combined

    if combined:
        schema_columns = list(mappings.keys())
        final_columns = ['company_id'] + [col for col in schema_columns if col != 'company_id'] + [key for key in all_id_keys if key not in schema_columns]
        
        df = pd.DataFrame(combined)
        for col in final_columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[final_columns]
        df = df.sort_values(['data_source', 'company_name'], na_position='last')
        
        output_path = os.path.join(base_dir, "unified_companies.csv")
        df.to_csv(output_path, index=False)
        print(f"Saved {len(df)} unified company records to {output_path}")
        print(f"Processed {total_records} total records with {len(df)} final unified records")

if __name__ == "__main__":
    source_configs = [
        {'name': 'ColourcoatsBigin', 'type': 'bigin', 'dir': 'ColourCoatsBigin', 'file': 'Accounts.json'},
        {'name': 'MetaliaBigin', 'type': 'bigin', 'dir': 'MetaliaBigin', 'file': 'Accounts.json'},
        {'name': 'Rolodex', 'type': 'rolodex', 'dir': 'Rolodex_data', 'file': 'companies.json'},
    ]
    unify_companies(source_configs)