import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os
import glob
import re
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'llm_query_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),  # This will read from .env
    'port': os.getenv('DB_PORT', '5432')
}

# Configuration for CSV file discovery and processing
CSV_FOLDER = r"C:\Projects\llm query\csvs"  # Your csvs subfolder
CSV_PATTERNS = ["*.csv"]  # Just find ANY CSV file

# Table name mappings (optional - for better naming)
TABLE_NAME_MAPPINGS = {
    'final_unified_personnel_smart_modified': 'personnel',
    'unified_companies_complete': 'companies',
    # Add more mappings as needed for specific files
    # 'my_products_export_2024': 'products',
    # 'customer_data_latest': 'customers'
}

def create_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to PostgreSQL successfully")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def discover_csv_files():
    """Automatically discover all CSV files in the project folder"""
    csv_files = []
    
    for pattern in CSV_PATTERNS:
        files = glob.glob(os.path.join(CSV_FOLDER, pattern))
        csv_files.extend(files)
    
    # Remove duplicates and sort
    csv_files = sorted(list(set(csv_files)))
    
    print(f"Discovered CSV files:")
    for i, file in enumerate(csv_files, 1):
        filename = os.path.basename(file)
        print(f"  {i}. {filename}")
    
    return csv_files

def generate_table_name(csv_path):
    """Generate a clean table name from CSV filename"""
    filename = os.path.splitext(os.path.basename(csv_path))[0]
    
    # Check if we have a custom mapping
    if filename in TABLE_NAME_MAPPINGS:
        return TABLE_NAME_MAPPINGS[filename]
    
    # Clean filename to create table name
    table_name = filename.lower()
    table_name = re.sub(r'[^a-z0-9_]', '_', table_name)
    table_name = re.sub(r'_+', '_', table_name)
    table_name = table_name.strip('_')
    
    # Remove common suffixes
    suffixes = ['_data', '_records', '_list', '_complete', '_modified', '_final']
    for suffix in suffixes:
        if table_name.endswith(suffix):
            table_name = table_name[:-len(suffix)]
            break
    
    return table_name

def analyze_csv_structure(csv_path):
    """Analyze CSV structure and determine optimal column types"""
    try:
        # Read sample of the CSV
        df_sample = pd.read_csv(csv_path, nrows=100)
        
        columns_info = {}
        
        for col in df_sample.columns:
            # Clean column name
            clean_col = col.lower().replace(' ', '_').replace('-', '_')
            clean_col = re.sub(r'[^a-z0-9_]', '_', clean_col)
            clean_col = re.sub(r'_+', '_', clean_col).strip('_')
            
            # Determine if column should be TEXT (default for safety)
            col_type = "TEXT"
            
            # Only use INTEGER for clearly numeric columns with reasonable values
            if col.lower() in ['age', 'count', 'quantity', 'year', 'month', 'day'] or 'count' in col.lower():
                non_null_values = df_sample[col].dropna()
                if len(non_null_values) > 0:
                    try:
                        numeric_vals = pd.to_numeric(non_null_values, errors='coerce').dropna()
                        if len(numeric_vals) == len(non_null_values) and all(-2147483648 <= x <= 2147483647 for x in numeric_vals):
                            col_type = "INTEGER"
                    except:
                        pass
            
            columns_info[clean_col] = {
                'original_name': col,
                'type': col_type,
                'sample_values': df_sample[col].dropna().head(3).tolist()
            }
        
        return columns_info
        
    except Exception as e:
        print(f"Error analyzing {csv_path}: {e}")
        return {}

def create_table_from_csv_structure(conn, table_name, columns_info):
    """Create table based on CSV structure analysis"""
    try:
        cursor = conn.cursor()
        
        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        
        # Build CREATE TABLE statement
        column_definitions = []
        for col_name, info in columns_info.items():
            column_definitions.append(f"{col_name} {info['type']}")
        
        # Add metadata columns
        column_definitions.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        column_definitions.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        create_sql = f"""
        CREATE TABLE {table_name} (
            {', '.join(column_definitions)}
        );
        """
        
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        
        print(f"Created table: {table_name} with {len(columns_info)} columns")
        return True
        
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        conn.rollback()
        return False

def clean_and_import_csv(conn, csv_path, table_name, columns_info):
    """Clean and import CSV data to PostgreSQL table"""
    try:
        print(f"Importing data from {os.path.basename(csv_path)} to {table_name}")
        
        # Read full CSV
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows")
        
        # Clean data
        df = df.astype(str).replace(['nan', 'NaN', 'None', '<NA>', 'null', ''], None)
        
        # Map original column names to database column names
        column_mapping = {info['original_name']: col_name for col_name, info in columns_info.items()}
        df_clean = df.rename(columns=column_mapping)
        
        # Select only columns that exist in our table
        available_columns = [col for col in columns_info.keys() if col in df_clean.columns]
        df_final = df_clean[available_columns]
        
        # Prepare insert statement
        placeholders = ', '.join(['%s'] * len(available_columns))
        columns_str = ', '.join(available_columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Convert to tuples for batch insert
        data_tuples = []
        for _, row in df_final.iterrows():
            tuple_data = []
            for val in row:
                if pd.isna(val) or val is None or str(val).lower() in ['nan', 'none', 'null']:
                    tuple_data.append(None)
                else:
                    tuple_data.append(str(val))
            data_tuples.append(tuple(tuple_data))
        
        # Batch insert
        cursor = conn.cursor()
        batch_size = 100
        total_imported = 0
        
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            try:
                execute_batch(cursor, insert_sql, batch, page_size=batch_size)
                conn.commit()
                total_imported += len(batch)
                
                if (i // batch_size + 1) % 10 == 0 or total_imported == len(data_tuples):
                    print(f"  Imported: {total_imported}/{len(data_tuples)} rows")
                    
            except Exception as batch_error:
                print(f"Error in batch: {batch_error}")
                conn.rollback()
                break
        
        cursor.close()
        print(f"Successfully imported {total_imported} rows into {table_name}")
        return total_imported
        
    except Exception as e:
        print(f"Error importing {csv_path}: {e}")
        return 0

def create_smart_indexes(conn, table_name, columns_info):
    """Create indexes on commonly searched columns"""
    try:
        cursor = conn.cursor()
        
        # Common search patterns for indexing
        index_patterns = [
            ('name', ['name', 'full_name', 'first_name', 'company_name', 'title']),
            ('location', ['city', 'state', 'country', 'billing_city', 'headquarters_location']),
            ('identifier', ['id', 'uid', 'person_id', 'company_id']),
            ('contact', ['email', 'phone', 'mobile']),
            ('category', ['type', 'category', 'status', 'industry'])
        ]
        
        indexes_created = []
        
        for pattern_name, keywords in index_patterns:
            matching_columns = []
            for col_name in columns_info.keys():
                if any(keyword in col_name.lower() for keyword in keywords):
                    matching_columns.append(col_name)
            
            # Create indexes for matching columns
            for col in matching_columns:
                index_name = f"idx_{table_name}_{col}"
                try:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col});")
                    indexes_created.append(index_name)
                except:
                    pass  # Skip if index creation fails
        
        conn.commit()
        cursor.close()
        
        if indexes_created:
            print(f"Created {len(indexes_created)} indexes for {table_name}")
        
    except Exception as e:
        print(f"Error creating indexes for {table_name}: {e}")

def get_all_tables_info(conn):
    """Get information about all tables in the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name NOT LIKE 'pg_%'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        tables_info = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table}' 
                AND column_name NOT IN ('created_at', 'updated_at')
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            tables_info[table] = {
                'record_count': count,
                'columns': columns
            }
        
        cursor.close()
        return tables_info
        
    except Exception as e:
        print(f"Error getting table info: {e}")
        return {}

def main():
    print("Dynamic Multi-CSV Migration System")
    print("=" * 50)
    
    conn = create_connection()
    if not conn:
        return
    
    try:
        # Discover all CSV files
        csv_files = discover_csv_files()
        
        if not csv_files:
            print("No CSV files found")
            return
        
        print(f"\nProcessing {len(csv_files)} CSV files...")
        
        # Process each CSV file
        for csv_path in csv_files:
            filename = os.path.basename(csv_path)
            print(f"\nProcessing: {filename}")
            
            # Generate table name
            table_name = generate_table_name(csv_path)
            print(f"Table name: {table_name}")
            
            # Analyze CSV structure
            columns_info = analyze_csv_structure(csv_path)
            if not columns_info:
                print(f"Skipping {filename} - could not analyze structure")
                continue
            
            # Create table
            if create_table_from_csv_structure(conn, table_name, columns_info):
                # Import data
                imported_count = clean_and_import_csv(conn, csv_path, table_name, columns_info)
                
                if imported_count > 0:
                    # Create indexes
                    create_smart_indexes(conn, table_name, columns_info)
        
        # Final verification
        print("\n" + "=" * 50)
        print("MIGRATION SUMMARY:")
        print("=" * 50)
        
        tables_info = get_all_tables_info(conn)
        total_records = 0
        
        for table_name, info in tables_info.items():
            print(f"{table_name}: {info['record_count']} records, {len(info['columns'])} columns")
            total_records += info['record_count']
        
        print(f"\nTotal records across all tables: {total_records}")
        print("Migration completed successfully!")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()