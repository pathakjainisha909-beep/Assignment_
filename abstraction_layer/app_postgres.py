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

# ----------------------------------------------------------------------
# Setup Logging
# ----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, filename='query_api.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Load environment variables and initialize API keys
# ----------------------------------------------------------------------
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

# Load local embedding model
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

# ----------------------------------------------------------------------
# Initialize FastAPI app
# ----------------------------------------------------------------------
app = FastAPI(
    title="Dynamic AI SQL Query API",
    description="Extensible natural language to SQL system - works with unified_personnel and unified_companies",
    version="3.2.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # Add your frontend origins here, or use ["*"] for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------------------------------------------------
# Request/Response Models
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# Database Connection
# ----------------------------------------------------------------------
def get_db_connection():
    """Get a PostgreSQL connection using environment config"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# ----------------------------------------------------------------------
# Dynamic Table Discovery (Cached)
# ----------------------------------------------------------------------
@lru_cache(maxsize=1)
def discover_all_tables():
    """Discover tables in the connected database schema with column info and sample data"""
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
            for col_name, _, _ in columns[:10]:
                try:
                    cursor.execute(f"""
                        SELECT DISTINCT {col_name} 
                        FROM {table} 
                        WHERE {col_name} IS NOT NULL 
                        AND {col_name} != '' 
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

# ----------------------------------------------------------------------
# Explicit Schema Definitions
# ----------------------------------------------------------------------
TABLE_COLUMN_DEFINITIONS = {
    'unified_personnel': {
        'description': 'People/contacts data - employees, architects, managers, clients',
        'columns': {
            'person_id': 'Unique person identifier',
            'first_name': 'Person first name',
            'last_name': 'Person last name',
            'full_name': 'Person full name',
            'title': 'Job title/role (architect, project manager, etc)',
            'company_name': 'Associated company name',
            'company_id': 'Associated company identifier',
            'works_at_multiple': 'Array of company names if multiple, "No" if one, null if none',
            'contact_type': 'Type of contact (client, employee, etc)',
            'department': 'Work department',
            'manager_id': 'Manager identifier',
            'city': 'Person city',
            'state': 'Person state',
            'country': 'Person country',
            'mobile': 'Mobile phone number',
            'email': 'Email address',
            'alternate_mobile': 'Alternative phone number',
            'data_source': 'Data source system',
            'workspace_id': 'Workspace identifier',
            'rolodex_id': 'Rolodex system ID',
            'rolodex_contact_id': 'Rolodex contact ID',
            'rolodex_company_id': 'Rolodex company ID',
            'interakt_contact_id': 'Interakt contact ID',
            'colourcoats_bigin_id': 'Colourcoats Bigin ID',
            'metalia_bigin_id': 'Metalia Bigin ID',
            'photo_url': 'Profile photo URL',
            'business_card_image_url': 'Business card image URL',
            'linkedin_profile': 'LinkedIn profile URL',
            'linkedin_slug': 'LinkedIn profile slug',
            'facebook_slug': 'Facebook profile slug',
            'instagram_slug': 'Instagram profile slug',
            'x_slug': 'X/Twitter profile slug',
            'youtube_slug': 'YouTube profile slug',
            'website_url': 'Personal website URL',
            'birthday_day': 'Birth day',
            'birthday_month': 'Birth month',
            'birthday_year': 'Birth year',
            'description': 'Additional description/notes'
        }
    },
    'unified_companies': {
        'description': 'Company/business data',
        'columns': {
            'uid': 'Unique company identifier',
            'company_name': 'Official company name',
            'description': 'Company description/services',
            'company_type': 'Type of business/company',
            'project_types': 'Types of projects company works on',
            'followers': 'Number of followers/connections',
            'arr_estimate': 'Annual recurring revenue estimate',
            'number_of_employees': 'Employee count',
            'billing_city': 'Company billing city',
            'billing_state': 'Company billing state',
            'headquarters_location': 'Company headquarters address',
            'country_code': 'Country code',
            'billing_code': 'Billing code',
            'phone': 'Company phone number',
            'phone_number': 'Alternative phone number',
            'website': 'Company website URL',
            'source': 'Data source system',
            'bigin_id': 'Bigin system ID',
            'rolodex_id': 'Rolodex system ID',
            'rolodex_company_id': 'Rolodex company ID',
            'workspace_id': 'Workspace identifier',
            'lead_source': 'How company was acquired as lead',
            'linkedin_description': 'LinkedIn company description',
            'logo_url': 'Company logo URL',
            'facebook_slug': 'Facebook page slug',
            'linkedin_slug': 'LinkedIn company slug',
            'x_slug': 'X/Twitter company slug',
            'instagram_slug': 'Instagram company slug'
        }
    }
}

# ----------------------------------------------------------------------
# Prompt Builder
# ----------------------------------------------------------------------
def build_dynamic_system_prompt(user_question: str, limit: int = 50) -> str:
    """Build system prompt with explicit column definitions and query examples"""
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
                "AVAILABLE COLUMNS:"
            ]
            for col_name, col_description in config['columns'].items():
                if any(col_name == db_col[0] for db_col in info['columns']):
                    examples = info['sample_data'].get(col_name, [])
                    examples_str = f" | Examples: {examples[:2]}" if examples else ""
                    schema_lines.append(f"  - {col_name}: {col_description}{examples_str}")
            unconfigured_cols = [col[0] for col in info['columns'] 
                               if col[0] not in config['columns'] 
                               and col[0] not in ['created_at', 'updated_at']]
            if unconfigured_cols:
                schema_lines.append("  Other available columns: " + ", ".join(unconfigured_cols))
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
You are a PostgreSQL SQL generator. Generate ONLY a clean, valid SQL query.
NO markdown, explanations, or comments. Just the SQL query.

YOUR AVAILABLE TABLES AND COLUMNS:
{schema_full}

USER QUESTION: {user_question}

IMPORTANT NOTES:
- Company info for people is in 'company_name' field in unified_personnel table
- Use ILIKE with % wildcards for flexible text searching (e.g., '%architect%' for roles, '%mumbai%' for city)
- Always add LIMIT {limit}
- If no exact match expected, use broader ILIKE terms (e.g., '%designer%' instead of '%sr designer%')
- Handle prefixes like 'Ar.', 'Arch.', 'Ar' as 'architect' in title
- Example: For "Find all the Architects in Mumbai", use:
  SELECT full_name, title, company_name, city, email, mobile
  FROM unified_personnel
  WHERE title ILIKE '%architect%' AND city ILIKE '%mumbai%'
  LIMIT 50
- Example: For "Find contact details of sr designer at Komal Azure", use:
  SELECT full_name, title, company_name, email, mobile
  FROM unified_personnel
  WHERE title ILIKE '%designer%' AND company_name ILIKE '%komal%azure%'
  LIMIT 50
- Return ONLY the SQL query
"""
    logger.debug(f"Generated system prompt for query: {user_question}")
    return system_prompt

# ----------------------------------------------------------------------
# Gemini Call
# ----------------------------------------------------------------------
def ask_gemini(prompt: str) -> str:
    """Send prompt to Gemini and clean response"""
    try:
        model = genai.GenerativeModel('gemini-2.0-flash')
        response = model.generate_content(prompt)
        if not response.text:
            raise ValueError("Empty response from Gemini API")
        raw_text = response.text.strip()
        logger.info(f"Raw Gemini response: {raw_text}")
        clean_text = re.sub(r"^```(?:sql|python)?\s*|\s*```$", "", raw_text, flags=re.MULTILINE)
        clean_text = re.sub(r"^///.*|^--.*|^#.*", "", clean_text, flags=re.MULTILINE)
        clean_text = re.sub(r"^\*\*.*\*\*", "", clean_text, flags=re.MULTILINE)
        if not clean_text:
            raise ValueError("No valid SQL query generated by Gemini")
        return clean_text.strip()
    except Exception as e:
        logger.error(f"Gemini API error: {str(e)}")
        raise Exception(f"Gemini API error: {str(e)}")

# ----------------------------------------------------------------------
# SQL Validation
# ----------------------------------------------------------------------
def validate_sql_query(sql_query: str) -> bool:
    """Validate SQL query syntax using sqlparse"""
    try:
        parsed = sqlparse.parse(sql_query)
        if not parsed or not parsed[0].tokens:
            logger.error(f"Invalid SQL syntax: {sql_query}")
            return False
        return True
    except Exception as e:
        logger.error(f"SQL validation error: {str(e)}")
        return False

# ----------------------------------------------------------------------
# SQL Execution
# ----------------------------------------------------------------------
def execute_sql_query(sql_query: str):
    """Execute SQL query and return results"""
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

# ----------------------------------------------------------------------
# Result Formatter
# ----------------------------------------------------------------------
def format_results_intelligently(result_df: pd.DataFrame, user_question: str) -> List[Dict[str, Any]]:
    """Format results as structured list of dictionaries with cleaned data"""
    if result_df.empty:
        return []

    formatted_results = []
    skip_columns = [
        'person_id', 'uid', 'bigin_id', 'rolodex_id', 'workspace_id', 'account_id',
        'created_at', 'updated_at', 'source', 'lead_source', 'manager_id'
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

# ----------------------------------------------------------------------
# Processing Pipeline
# ----------------------------------------------------------------------
def process_query(user_question: str, limit: int = 50):
    """Main query pipeline"""
    try:
        prompt = build_dynamic_system_prompt(user_question, limit)
        sql_query = ask_gemini(prompt)
        result_df, tables_used = execute_sql_query(sql_query)
        formatted_output = format_results_intelligently(result_df, user_question)
        logger.info(f"Processed query: {user_question}, Results count: {len(formatted_output)}")
        return sql_query, result_df, formatted_output, tables_used
    except Exception as e:
        logger.error(f"Error processing query '{user_question}': {str(e)}")
        return None, None, [], []

# ----------------------------------------------------------------------
# FastAPI Routes
# ----------------------------------------------------------------------
@app.get("/")
async def root():
    try:
        tables_info = discover_all_tables()
        total_records = sum(info['record_count'] for info in tables_info.values())
        return {
            "message": "Dynamic AI SQL Query API is running",
            "status": "healthy",
            "available_tables": list(tables_info.keys()),
            "total_tables": len(tables_info),
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

# ----------------------------------------------------------------------
# Cache Reset Endpoint (Optional)
# ----------------------------------------------------------------------
@app.post("/reset-cache")
async def reset_cache():
    """Reset the table discovery cache"""
    try:
        discover_all_tables.cache_clear()
        logger.info("Table discovery cache cleared")
        return {"message": "Table discovery cache cleared successfully"}
    except Exception as e:
        logger.error(f"Cache reset error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------------------------------------------------------
# Run with Uvicorn
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)