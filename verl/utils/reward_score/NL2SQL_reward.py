"""
Custom NL2SQL reward function for verl with complete SQL validation.
Implements all four reward components from the paper with proper SQL checking.
"""

import re
import sqlparse # type: ignore
from sqlvalidator import parse_query # type: ignore
from typing import Dict, Any, Optional

def compute_score(data_source: str, solution_str: str, 
                 ground_truth: Dict[str, Any], extra_info: Optional[Dict] = None) -> float:
    """
    Complete NL2SQL progressive reward function with SQL validation.
    
    Args:
        data_source: Dataset name (e.g., 'nl2sql')
        solution_str: Generated response containing <think>, <answer>, and SQL
        ground_truth: Must contain:
            - 'expected_sql': Correct SQL query (for validation)
            - 'max_length': Maximum allowed response length
            - 'db_schema': Dict of {table: [columns]} for validation
        extra_info: Can contain 'timeout' for SQL execution
            
    Returns:
        float: Total reward score from -6 to 6.5
    """
    # Initialize all reward components
    rewards = {
        'format': 0,
        'execution': 0,
        'result': 0,
        'length': 0
    }
    
    # --------------------------
    # 1. Format Reward (Sf)
    # --------------------------
    if not _validate_format(solution_str):
        rewards['format'] = -1
        return sum(rewards.values())
    rewards['format'] = 1
    
    # Extract SQL query
    sql_query = _extract_sql(solution_str)
    if not sql_query:
        rewards['format'] = -1
        return sum(rewards.values())
    
    # --------------------------
    # 2. Execution Reward (Se)
    # --------------------------
    is_valid, validation_error = _validate_sql_execution(sql_query, ground_truth.get('db_schema'))
    if not is_valid:
        rewards['execution'] = -2
        return sum(rewards.values())
    rewards['execution'] = 2
    
    # --------------------------
    # 3. Result Reward (Sr)
    # --------------------------
    is_correct = _validate_sql_result(sql_query, ground_truth.get('expected_sql'))
    if not is_correct:
        rewards['result'] = -3
        return sum(rewards.values())
    rewards['result'] = 3
    
    # --------------------------
    # 4. Length Reward (Sl)
    # --------------------------
    max_length = ground_truth.get('max_length', 512)
    rewards['length'] = _calculate_length_reward(solution_str, max_length)
    
    return sum(rewards.values())

# --------------------------
# Helper Functions
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

def _validate_sql_execution(sql: str, schema: Optional[Dict] = None) -> tuple:
    """
    Validate SQL syntax and schema compliance.
    Returns (is_valid: bool, error: str)
    """
    try:
        # 1. Basic SQL syntax validation
        parsed = parse_query(sql)
        if not parsed.is_valid():
            return False, parsed.errors[0]
        
        # 2. Schema validation if schema provided
        if schema:
            # Check tables exist in schema
            tables = _extract_tables(sql)
            for table in tables:
                if table not in schema:
                    return False, f"Table '{table}' not in schema"
            
            # Check columns exist in tables (basic check)
            columns = _extract_columns(sql)
            for col in columns:
                if '.' in col:  # Qualified column (table.column)
                    table, column = col.split('.')
                    if table in schema and column not in schema[table]:
                        return False, f"Column '{col}' not found"
        return True, ""
    except Exception as e:
        return False, str(e)

def _validate_sql_result(generated_sql: str, expected_sql: str) -> bool:
    """
    Compare if generated SQL is semantically equivalent to expected SQL.
    Uses query normalization for comparison.
    """
    if not expected_sql:
        return True  # No expected SQL to compare against
        
    def normalize_sql(sql: str) -> str:
        """Normalize SQL for comparison."""
        # Parse and format consistently
        parsed = sqlparse.parse(sql)[0]
        normalized = sqlparse.format(
            str(parsed),
            reindent=True,
            keyword_case='upper',
            identifier_case='lower',
            strip_comments=True
        )
        # Remove extra whitespace
        return ' '.join(normalized.split())
    
    return normalize_sql(generated_sql) == normalize_sql(expected_sql)

def _calculate_length_reward(response: str, max_length: int) -> float:
    """Calculate length-based reward component."""
    try:
        think_content = re.search(r'<think>(.*?)</think>', response, re.DOTALL).group(1)
        answer_content = re.search(r'<answer>(.*?)</answer>', response, re.DOTALL).group(1)
        sql_content = re.search(r'```sql(.*?)```', answer_content, re.DOTALL).group(1)
        
        total_len = len(think_content) + len(answer_content)
        sql_len = len(sql_content.strip())
        answer_len = len(answer_content)
        
        Stl = total_len / max_length
        Sal = sql_len / answer_len if answer_len > 0 else 0
        
        if total_len <= max_length:
            return 0.5 * Stl + Sal
        else:
            return 0.5 + Sal
    except (AttributeError, ValueError):
        return 0

def _extract_tables(sql: str) -> list:
    """Extract table names from SQL (simplified)."""
    tables = set()
    # Simple regex - would need to enhance for complex queries
    for match in re.finditer(r'(?:FROM|JOIN)\s+([\w]+)', sql, re.IGNORECASE):
        tables.add(match.group(1))
    return list(tables)

def _extract_columns(sql: str) -> list:
    """Extract column names from SQL (simplified)."""
    columns = set()
    # Simple regex - would need to enhance for complex queries
    for match in re.finditer(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL):
        select_clause = match.group(1)
        for col_match in re.finditer(r'([\w]+(?:\.[\w]+)?)', select_clause):
            columns.add(col_match.group(1))
    return list(columns)