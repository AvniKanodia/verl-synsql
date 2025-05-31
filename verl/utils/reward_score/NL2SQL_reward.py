"""
Custom NL2SQL reward function for verl with structured output support.
Implements all four reward components from the paper while accounting for:
- db_id, sql_complexity, question_style, and other metadata
- Maintains progressive rewards (format, execution, result)
- Length reward commented out but preserved
"""

import re
import json
from venv import logger
import sqlparse
from typing import Dict, Any, List, Optional, Tuple

def compute_score(data_source: str, solution_str: str, 
                 ground_truth: Dict[str, Any], extra_info: Optional[Dict] = None) -> float:
    """
    NL2SQL progressive reward function with structured output support.
    
    Args:
        data_source: Dataset name (e.g., 'nl2sql')
        solution_str: Generated response containing <think>, <answer>, and SQL
        ground_truth: Must contain:
            - 'expected_sql': Correct SQL query
            - 'db_id': Database identifier
            - 'sql_complexity': Complexity level (Simple/Moderate/Complex)
            - 'question_style': Style of question (Vague/Colloquial/Imperative)
            - 'db_schema': Dict of {table: [columns]} for validation
            - 'max_length': Maximum allowed response length (optional)
        extra_info: Additional context (unused)
    
    Returns:
        float: Total reward score incorporating all components
    """
    
   # Initialize all reward components
    rewards = {
        'format': 0,
        'execution': 0,
        'result': 0,
    }
    
    try:
        # Parse the JSON input to get the actual response text
        response_data = json.loads(solution_str)
        actual_response = response_data.get('cot', '')
    except json.JSONDecodeError:
        actual_response = solution_str  # Fallback if not JSON

    # --------------------------
    # 1. Format Reward (Sf)
    # --------------------------
    if not _validate_format(actual_response):
        rewards['format'] = -1
        return sum(rewards.values())
    rewards['format'] = 1
    
    # Extract SQL query
    sql_query = _extract_sql(actual_response)
    if not sql_query:
        rewards['format'] = -1
        return sum(rewards.values())
    
    # --------------------------
    # 2. Execution Reward (Se)
    # --------------------------
    is_valid, _ = _validate_sql_execution(
        sql_query, 
        ground_truth.get('db_schema'),
        ground_truth['db_id'],
        ground_truth['sql_complexity']
    )
    
    if not is_valid:
        rewards['execution'] = -2
        return sum(rewards.values())
    rewards['execution'] = 2
    
    # --------------------------
    # 3. Result Reward (Sr)
    # --------------------------
    is_correct = _validate_sql_result(
        sql_query, 
        ground_truth['expected_sql'],
        ground_truth['sql_complexity'],
        ground_truth['question_style'],
        extra_info
    )
    
    if not is_correct:
        rewards['result'] = -3
        return sum(rewards.values())
    rewards['result'] = 3
    
    return sum(rewards.values())

# --------------------------
# Helper Functions (Modified for structured output)
# --------------------------

def _validate_format(response: str) -> bool:
    """Check for required XML tags and SQL code blocks."""
    required_tags = [
        ('<think>', '</think>'),
        ('<answer>', '</answer>'),
        ('```sql', '```')
    ]
    return all(start in response and end in response for start, end in required_tags)

def _extract_sql(response: str) -> str:
    """Extract SQL query from between ```sql ``` markers."""
    try:
        sql_start = response.index('```sql') + 6
        sql_end = response.index('```', sql_start)
        return response[sql_start:sql_end].strip()
    except ValueError:
        return ""

def _validate_sql_execution(sql: str, schema: Optional[Dict], db_id: str, complexity: str) -> tuple:
    """
    Validate SQL syntax and schema compliance with metadata awareness.
    Returns (is_valid: bool, error: str)
    """
    try:
        # 1. Basic SQL syntax validation using sqlparse
        parsed = sqlparse.parse(sql)
        if not parsed or not all(stmt.get_type() == 'SELECT' for stmt in parsed):
            return False, "Only SELECT queries are supported"
        
        # 2. Schema validation if schema provided
        if schema:
            tables = _extract_tables(sql)
            for table in tables:
                if table not in schema:
                    return False, f"Table '{table}' not in schema for db_id '{db_id}'"
            
            # Additional validation based on SQL complexity
            if complexity == "Complex":
                # Check for appropriate complex query features
                if not _contains_complex_features(sql):
                    return False, "Missing complex query features for complexity level"
        
        return True, ""
    except Exception as e:
        return False, str(e)

def _validate_sql_result(generated_sql: str, expected_sql: str, 
                        complexity: str, question_style: str,
                        extra_info: Optional[Dict] = None) -> bool:
    """
    Compare if generated SQL is semantically equivalent to expected SQL,
    using execution-based comparison when possible.
    """
    # Get database connection if available
    db_conn = extra_info.get('db_connection') if extra_info else None
    
    if db_conn:
        # Use execution-based comparison when DB connection is available
        return _compare_execution_results(
            generated_sql,
            expected_sql,
            db_conn,
            question_style
        )
    else:
        # Fall back to structural comparison
        return _structural_sql_match(
            generated_sql,
            expected_sql,
            complexity,
            question_style
        )

def _compare_execution_results(generated_sql: str, expected_sql: str, 
                             db_conn, question_style: str) -> bool:
    """
    Compare queries by executing them against the database.
    Returns True if results are equivalent.
    """
    try:
        # Determine if order matters based on question style and presence of ORDER BY
        order_matters = ('order by' in expected_sql.lower()) or (question_style != "Vague")
        
        # Execute generated query
        gen_cursor = db_conn.cursor()
        gen_cursor.execute(generated_sql)
        gen_results = gen_cursor.fetchall()
        
        # Execute expected query
        exp_cursor = db_conn.cursor()
        exp_cursor.execute(expected_sql)
        exp_results = exp_cursor.fetchall()
        
        # Compare result sets using the same logic as eval_exec_match
        if len(gen_results) != len(exp_results):
            return False
            
        if order_matters:
            # Compare results exactly with order
            return gen_results == exp_results
        else:
            # Compare results as sets (order insensitive)
            gen_set = set(tuple(row) for row in gen_results)
            exp_set = set(tuple(row) for row in exp_results)
            return gen_set == exp_set
            
    except Exception as e:
        logger.error(f"Execution comparison failed: {str(e)}")
        return False

def _structural_sql_match(generated_sql: str, expected_sql: str,
                         complexity: str, question_style: str) -> bool:
    """
    Fallback structural comparison when DB connection is not available.
    Uses the same normalization approach as before but with some improvements.
    """
    def normalize_sql(sql: str) -> str:
        """Normalize SQL for comparison."""
        parsed = sqlparse.parse(sql)[0]
        return sqlparse.format(
            str(parsed),
            reindent=True,
            keyword_case='upper',
            identifier_case='lower',
            strip_comments=True
        )
    
    # For vague questions, be more lenient with matches
    if question_style == "Vague":
        return _fuzzy_sql_match(
            normalize_sql(generated_sql),
            normalize_sql(expected_sql),
            complexity
        )
    else:
        return normalize_sql(generated_sql) == normalize_sql(expected_sql)

def _fuzzy_sql_match(generated: str, expected: str, complexity: str) -> bool:
    """
    Enhanced fuzzy matching for SQL queries with complexity awareness.
    Now includes additional checks inspired by the execution-based approach.
    """
    # Basic normalization
    gen_clean = ' '.join(generated.split())
    exp_clean = ' '.join(expected.split())
    
    # For simple queries, exact match is required
    if complexity == "Simple":
        return gen_clean == exp_clean
    
    # For moderate/complex, check key components with improved extraction
    key_components = [
        ('SELECT', _extract_select_columns),
        ('FROM', _extract_tables_with_aliases),
        ('WHERE', _extract_conditions),
        ('GROUP BY', _extract_group_by),
        ('ORDER BY', _extract_order_by),
        ('HAVING', _extract_having),
        ('LIMIT', _extract_limit)
    ]
    
    for clause, extractor in key_components:
        gen_part = extractor(gen_clean)
        exp_part = extractor(exp_clean)
        if gen_part != exp_part:
            return False
    
    return True

def _extract_tables_with_aliases(sql: str) -> List[Tuple[str, Optional[str]]]:
    """
    Enhanced table extraction that handles aliases.
    Returns list of (table_name, alias) tuples.
    """
    tables = []
    # Match table references with optional aliases
    pattern = r'(?:FROM|JOIN)\s+([\w]+)(?:\s+(?:AS\s+)?([\w]+))?'
    for match in re.finditer(pattern, sql, re.IGNORECASE):
        table = match.group(1)
        alias = match.group(2) if match.group(2) and match.group(2) != table else None
        tables.append((table, alias))
    return tables

def _extract_having(sql: str) -> List[str]:
    """Extract HAVING conditions from SQL."""
    having_match = re.search(r'HAVING\s+(.*?)(?:\s+ORDER BY|\s+LIMIT|\s*$)', 
                           sql, re.IGNORECASE | re.DOTALL)
    if not having_match:
        return []
    return [cond.strip() for cond in re.split(r'\s+AND\s+|\s+OR\s+', having_match.group(1))]

def _extract_limit(sql: str) -> Optional[str]:
    """Extract LIMIT clause from SQL."""
    limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
    return limit_match.group(1) if limit_match else None

def _contains_complex_features(sql: str) -> bool:
    """Check for features expected in complex queries."""
    complex_patterns = [
        r'JOIN\s+\w+\s+ON',       # Joins
        r'GROUP BY',               # Grouping
        r'HAVING',                 # Having clauses
        r'UNION\s+(ALL\s+)?SELECT', # Unions
        r'WITH\s+\w+\s+AS\s*\(',   # CTEs
        r'CASE WHEN.*?END'         # Case statements
    ]
    return any(re.search(pattern, sql, re.IGNORECASE) for pattern in complex_patterns)

def _extract_tables(sql: str) -> list:
    """Extract table names from SQL."""
    tables = set()
    # Simple regex - would need to enhance for complex queries
    for match in re.finditer(r'(?:FROM|JOIN)\s+([\w]+)', sql, re.IGNORECASE):
        tables.add(match.group(1))
    return list(tables)

def _extract_select_columns(sql: str) -> list:
    """Extract selected columns from SQL."""
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return []
    return [col.strip() for col in select_match.group(1).split(',')]

def _extract_conditions(sql: str) -> list:
    """Extract WHERE conditions from SQL."""
    where_match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP BY|\s+ORDER BY|\s*$)', sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return []
    return [cond.strip() for cond in re.split(r'\s+AND\s+|\s+OR\s+', where_match.group(1))]

def _extract_group_by(sql: str) -> list:
    """Extract GROUP BY columns from SQL."""
    group_match = re.search(r'GROUP BY\s+(.*?)(?:\s+HAVING|\s+ORDER BY|\s*$)', sql, re.IGNORECASE | re.DOTALL)
    if not group_match:
        return []
    return [col.strip() for col in group_match.group(1).split(',')]

def _extract_order_by(sql: str) -> list:
    """Extract ORDER BY columns from SQL."""
    order_match = re.search(r'ORDER BY\s+(.*?)\s*$', sql, re.IGNORECASE | re.DOTALL)
    if not order_match:
        return []
    return [col.strip() for col in order_match.group(1).split(',')]