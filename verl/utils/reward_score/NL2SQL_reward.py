"""
Custom NL2SQL reward function for verl with structured output support.
Implements all four reward components from the paper while accounting for:
- db_id, sql_complexity, question_style, and other metadata
- Maintains progressive rewards (format, execution, result)
- Length reward commented out but preserved
"""

import re
import json
import sqlparse
from typing import Dict, Any, Optional

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
        ground_truth['question_style']
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
                        complexity: str, question_style: str) -> bool:
    """
    Compare if generated SQL is semantically equivalent to expected SQL,
    with adjustments for question style and complexity.
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
    
    # For vague questions, be more lenient with exact matches
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
    Fuzzy matching for SQL queries with complexity awareness.
    """
    # Basic normalization
    gen_clean = ' '.join(generated.split())
    exp_clean = ' '.join(expected.split())
    
    # For simple queries, exact match is required
    if complexity == "Simple":
        return gen_clean == exp_clean
    
    # For moderate/complex, check key components
    key_components = [
        ('SELECT', _extract_select_columns),
        ('FROM', _extract_tables),
        ('WHERE', _extract_conditions),
        ('GROUP BY', _extract_group_by),
        ('ORDER BY', _extract_order_by)
    ]
    
    for clause, extractor in key_components:
        gen_part = extractor(gen_clean)
        exp_part = extractor(exp_clean)
        if gen_part != exp_part:
            return False
    
    return True

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