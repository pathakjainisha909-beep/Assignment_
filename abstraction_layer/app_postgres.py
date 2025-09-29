import os
import re
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
import google.generativeai as genai
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
from functools import lru_cache
import sqlparse
import logging
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, filename='query_api.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    logger.error("Google API key not configured")
    raise ValueError("Google API key not configured")

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'llm_query_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT', '5432')
}

try:
    genai.configure(api_key=API_KEY)
except Exception as e:
    logger.error(f"Failed to configure Gemini API: {str(e)}")
    raise ValueError(f"Failed to configure Gemini API: {str(e)}")

embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

app = FastAPI(
    title="Normalized CRM Query API",
    description="Natural language SQL with proper JOIN handling for normalized Personnel/Companies schema",
    version="4.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    question: str
    include_raw_data: Optional[bool] = False
    limit: Optional[int] = 50

class QueryResponse(BaseModel):
    success: bool
    question: str
    results: List[Dict[str, Any]]
    sql_query: Optional[str] = None
    raw_data: Optional[str] = None
    error: Optional[str] = None
    tables_used: Optional[List[str]] = None

class DatabaseInfo(BaseModel):
    tables: Dict[str, dict]
    total_records: int

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@lru_cache(maxsize=1)
def discover_all_tables():
    logger.info("Discovering database tables")
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name NOT LIKE 'pg_%'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        tables_info = {}
        for table in tables:
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = '{table}' 
                AND column_name NOT IN ('created_at', 'updated_at')
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            record_count = cursor.fetchone()[0]
            sample_data = {}
            for col_name, data_type, _ in columns[:10]:
                try:
                    # Handle different data types for sample data query
                    if data_type.lower() == 'boolean':
                        # For boolean columns, just check NOT NULL
                        cursor.execute(f"""
                            SELECT DISTINCT {col_name} 
                            FROM {table} 
                            WHERE {col_name} IS NOT NULL 
                            LIMIT 3
                        """)
                    else:
                        # For text/other columns, exclude empty strings
                        cursor.execute(f"""
                            SELECT DISTINCT {col_name} 
                            FROM {table} 
                            WHERE {col_name} IS NOT NULL 
                            AND {col_name}::text != '' 
                            LIMIT 3
                        """)
                    examples = [str(row[0]) for row in cursor.fetchall()]
                    sample_data[col_name] = examples
                except Exception as e:
                    logger.warning(f"Failed to fetch sample data for {table}.{col_name}: {str(e)}")
                    sample_data[col_name] = []
            tables_info[table] = {
                'columns': columns,
                'record_count': record_count,
                'sample_data': sample_data
            }
        cursor.close()
        conn.close()
        logger.info(f"Discovered {len(tables)} tables")
        return tables_info
    except Exception as e:
        conn.close()
        logger.error(f"Error discovering tables: {str(e)}")
        raise Exception(f"Error discovering tables: {str(e)}")

# NORMALIZED SCHEMA DEFINITIONS
TABLE_COLUMN_DEFINITIONS = {
    'unified_personnel': {
        'description': 'People/contacts - employees, architects, managers, clients. NO company fields here.',
        'columns': {
            'person_id': 'Unique person identifier (PRIMARY KEY)',
            'first_name': 'Person first name',
            'last_name': 'Person last name',
            'full_name': 'Person full name',
            'title': 'Job title/role (architect, project manager, designer, etc)',
            'contact_type': 'Type of contact (client, employee, etc)',
            'department': 'Work department',
            'manager_id': 'Manager identifier',
            'city': 'Person city',
            'state': 'Person state',
            'country': 'Person country',
            'mobile': 'Mobile phone number',
            'email': 'Email address',
            'alternate_mobile': 'Alternative phone number',
            'data_source': 'Data source system (Rolodex, ColourcoatsBigin, MetaliaBigin)',
            'rolodex_id': 'Rolodex system ID',
            'colourcoats_bigin_id': 'Colourcoats Bigin ID',
            'metalia_bigin_id': 'Metalia Bigin ID',
            'linkedin_profile': 'LinkedIn profile URL',
            'linkedin_slug': 'LinkedIn slug',
            'facebook_slug': 'Facebook slug',
            'instagram_slug': 'Instagram slug',
            'x_slug': 'X/Twitter slug',
            'youtube_slug': 'YouTube slug',
            'website_url': 'Personal website',
            'description': 'Additional notes'
        }
    },
    'person_companies': {
        'description': 'Relationship table linking people to companies. JOIN with unified_personnel for queries filtering by company.',
        'columns': {
            'relationship_id': 'Unique relationship identifier',
            'person_id': 'Foreign key to unified_personnel.person_id',
            'company_id': 'Company identifier',
            'company_name': 'Company name - USE THIS for company filtering',
            'is_active': 'Whether person currently works here (true/false)',
            'start_date': 'When person started at company',
            'title_at_company': 'Job title at this specific company',
            'data_source': 'Source system'
        }
    },
    'unified_companies': {
        'description': 'Company/business data',
        'columns': {
            'uid': 'Unique company identifier',
            'company_name': 'Official company name',
            'description': 'Company description',
            'company_type': 'Type of business',
            'project_types': 'Types of projects',
            'number_of_employees': 'Employee count',
            'billing_city': 'Company city',
            'billing_state': 'Company state',
            'headquarters_location': 'HQ address',
            'phone': 'Company phone',
            'website': 'Company website',
            'linkedin_slug': 'LinkedIn company page'
        }
    }
}

def build_dynamic_system_prompt(user_question: str, limit: int = 50) -> str:
    """Build system prompt with JOIN logic for normalized schema"""
    tables_info = discover_all_tables()
    if not tables_info:
        raise Exception("No tables found in database")
    
    schema_blocks = []
    for table_name, info in tables_info.items():
        if table_name in TABLE_COLUMN_DEFINITIONS:
            config = TABLE_COLUMN_DEFINITIONS[table_name]
            schema_lines = [
                f"Table '{table_name}': {config['description']}",
                f"Records: {info['record_count']}",
                "",
                "COLUMNS:"
            ]
            for col_name, col_description in config['columns'].items():
                if any(col_name == db_col[0] for db_col in info['columns']):
                    examples = info['sample_data'].get(col_name, [])
                    examples_str = f" | Examples: {examples[:2]}" if examples else ""
                    schema_lines.append(f"  - {col_name}: {col_description}{examples_str}")
        else:
            schema_lines = [f"Table '{table_name}' ({info['record_count']} records)"]
            for col_name, data_type, nullable in info['columns']:
                if col_name not in ['created_at', 'updated_at']:
                    examples = info['sample_data'].get(col_name, [])
                    examples_str = f" | Examples: {examples[:2]}" if examples else ""
                    schema_lines.append(f"  - {col_name}: {data_type}{examples_str}")
        schema_blocks.append("\n".join(schema_lines))
    
    schema_full = "\n\n".join(schema_blocks)
    
    system_prompt = f"""
You are a PostgreSQL SQL generator for a NORMALIZED database schema.
Generate ONLY a clean, valid SQL query. NO markdown, explanations, or comments.

DATABASE SCHEMA:
{schema_full}

USER QUESTION: {user_question}

CRITICAL RULES FOR NORMALIZED SCHEMA:

1. WHEN TO USE JOIN:
   - If query filters by COMPANY but wants PERSON details → JOIN unified_personnel with person_companies
   - If query filters by PERSON but wants COMPANY details → JOIN unified_personnel with person_companies
   - Company filtering is ONLY in person_companies.company_name (NOT in unified_personnel)

2. JOIN SYNTAX (when needed):
   SELECT p.full_name, p.title, p.email, p.mobile, pc.company_name
   FROM unified_personnel p
   INNER JOIN person_companies pc ON p.person_id = pc.person_id
   WHERE pc.company_name ILIKE '%company_name%'
   AND p.title ILIKE '%architect%'
   LIMIT {limit}

3. SIMPLE QUERIES (no JOIN needed):
   - "Find all architects in Mumbai" → Only filter unified_personnel by title and city
   - "Show me people with email..." → Only query unified_personnel
   - "Get companies in Delhi" → Only query unified_companies

4. TEXT SEARCH:
   - Always use ILIKE with % wildcards for flexible matching
   - Handle job title variations: 'Ar.', 'Arch.', 'Ar' → search '%architect%'
   - Be flexible: 'sr designer' → search '%designer%'

5. EXAMPLES:

Example 1 - Simple (no JOIN):
Q: "Find all architects in Mumbai"
A: SELECT full_name, title, city, email, mobile
   FROM unified_personnel
   WHERE title ILIKE '%architect%' AND city ILIKE '%mumbai%'
   LIMIT {limit}

Example 2 - JOIN needed (company filter + person details):
Q: "Find contact details of designer at Komal Azure"
A: SELECT p.full_name, p.title, p.email, p.mobile, pc.company_name
   FROM unified_personnel p
   INNER JOIN person_companies pc ON p.person_id = pc.person_id
   WHERE p.title ILIKE '%designer%' AND pc.company_name ILIKE '%komal%azure%'
   LIMIT {limit}

Example 3 - JOIN needed (person filter + company details):
Q: "What companies does Jay Visariya work at?"
A: SELECT p.full_name, pc.company_name, pc.is_active, pc.start_date
   FROM unified_personnel p
   INNER JOIN person_companies pc ON p.person_id = pc.person_id
   WHERE p.full_name ILIKE '%jay%visariya%'
   LIMIT {limit}

Example 4 - JOIN needed (company + location):
Q: "Find architects in Mumbai working at AllHome"
A: SELECT p.full_name, p.title, p.city, p.email, pc.company_name
   FROM unified_personnel p
   INNER JOIN person_companies pc ON p.person_id = pc.person_id
   WHERE p.title ILIKE '%architect%' 
   AND p.city ILIKE '%mumbai%'
   AND pc.company_name ILIKE '%allhome%'
   LIMIT {limit}

6. ALWAYS include LIMIT {limit}

Return ONLY the SQL query.
"""
    
    logger.debug(f"Generated system prompt for: {user_question}")
    return system_prompt

def ask_gemini(prompt: str) -> str:
    try:
        model = genai.GenerativeModel('gemini-2.0-flash')
        response = model.generate_content(prompt)
        if not response.text:
            raise ValueError("Empty response from Gemini")
        raw_text = response.text.strip()
        logger.info(f"Raw Gemini response: {raw_text}")
        clean_text = re.sub(r"^```(?:sql|python)?\s*|\s*```$", "", raw_text, flags=re.MULTILINE)
        clean_text = re.sub(r"^///.*|^--.*|^#.*", "", clean_text, flags=re.MULTILINE)
        clean_text = re.sub(r"^\*\*.*\*\*", "", clean_text, flags=re.MULTILINE)
        if not clean_text:
            raise ValueError("No valid SQL generated")
        return clean_text.strip()
    except Exception as e:
        logger.error(f"Gemini API error: {str(e)}")
        raise Exception(f"Gemini API error: {str(e)}")

def validate_sql_query(sql_query: str) -> bool:
    try:
        parsed = sqlparse.parse(sql_query)
        if not parsed or not parsed[0].tokens:
            logger.error(f"Invalid SQL syntax: {sql_query}")
            return False
        return True
    except Exception as e:
        logger.error(f"SQL validation error: {str(e)}")
        return False

def execute_sql_query(sql_query: str):
    if not validate_sql_query(sql_query):
        raise Exception("Invalid SQL query syntax")
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql_query)
        results = cursor.fetchall()
        tables_used = []
        tables_info = discover_all_tables()
        for table_name in tables_info.keys():
            if table_name.lower() in sql_query.lower():
                tables_used.append(table_name)
        df = pd.DataFrame([dict(row) for row in results]) if results else pd.DataFrame()
        cursor.close()
        conn.close()
        logger.info(f"Executed SQL: {sql_query}")
        return df, tables_used
    except Exception as e:
        conn.close()
        logger.error(f"SQL execution error: {str(e)}")
        raise Exception(f"SQL execution error: {str(e)}")

def format_results_intelligently(result_df: pd.DataFrame, user_question: str) -> List[Dict[str, Any]]:
    if result_df.empty:
        return []

    formatted_results = []
    skip_columns = [
        'person_id', 'uid', 'relationship_id', 'company_id',
        'created_at', 'updated_at', 'manager_id'
    ]

    for idx, row in result_df.iterrows():
        entry = {}
        for col, value in row.items():
            if col.lower() not in [c.lower() for c in skip_columns]:
                if value is not None and str(value).lower() not in ['nan', '', 'none', 'null']:
                    clean_col_name = col.replace('_', ' ').title()
                    clean_value = str(value)
                    if any(keyword in col.lower() for keyword in ['mobile', 'phone']):
                        clean_value = clean_value.replace('.0', '')
                    entry[clean_col_name] = clean_value
        if entry:
            formatted_results.append(entry)

    return formatted_results

def process_query(user_question: str, limit: int = 50):
    try:
        prompt = build_dynamic_system_prompt(user_question, limit)
        sql_query = ask_gemini(prompt)
        result_df, tables_used = execute_sql_query(sql_query)
        formatted_output = format_results_intelligently(result_df, user_question)
        logger.info(f"Processed query: {user_question}, Results: {len(formatted_output)}")
        return sql_query, result_df, formatted_output, tables_used
    except Exception as e:
        logger.error(f"Error processing query '{user_question}': {str(e)}")
        return None, None, [], []

@app.get("/")
async def root():
    try:
        tables_info = discover_all_tables()
        total_records = sum(info['record_count'] for info in tables_info.values())
        return {
            "message": "Normalized CRM Query API - with JOIN support",
            "status": "healthy",
            "available_tables": list(tables_info.keys()),
            "architecture": "normalized",
            "features": ["personnel", "companies", "relationships", "auto-JOIN"],
            "total_records": total_records
        }
    except Exception as e:
        logger.error(f"Root endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables", response_model=DatabaseInfo)
async def get_database_info():
    try:
        tables_info = discover_all_tables()
        total_records = sum(info['record_count'] for info in tables_info.values())
        formatted_tables = {}
        for table_name, info in tables_info.items():
            formatted_tables[table_name] = {
                "record_count": info['record_count'],
                "columns": [{"name": col[0], "type": col[1], "nullable": col[2]} for col in info['columns']],
                "sample_data": info['sample_data']
            }
        logger.info("Retrieved database info")
        return DatabaseInfo(tables=formatted_tables, total_records=total_records)
    except Exception as e:
        logger.error(f"Tables endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    if not API_KEY:
        logger.error("Gemini API key not configured")
        raise HTTPException(status_code=500, detail="Gemini API key not configured")
    if not request.question.strip():
        logger.error("Empty question provided")
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    try:
        sql_query, result_df, formatted_results, tables_used = process_query(
            request.question, 
            request.limit or 50
        )
        if sql_query is None:
            logger.warning(f"Query failed: {request.question}")
            return QueryResponse(
                success=False,
                question=request.question,
                results=[],
                error="Error processing query",
                tables_used=[]
            )
        raw_data = None
        if request.include_raw_data and result_df is not None and not result_df.empty:
            raw_data = result_df.head(10).to_string(index=False)
        logger.info(f"Query executed successfully: {request.question}")
        return QueryResponse(
            success=True,
            question=request.question,
            results=formatted_results,
            sql_query=sql_query,
            raw_data=raw_data,
            tables_used=tables_used
        )
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        return QueryResponse(
            success=False,
            question=request.question,
            results=[],
            error=str(e),
            tables_used=[]
        )

@app.post("/reset-cache")
async def reset_cache():
    try:
        discover_all_tables.cache_clear()
        logger.info("Cache cleared")
        return {"message": "Cache cleared successfully"}
    except Exception as e:
        logger.error(f"Cache reset error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)