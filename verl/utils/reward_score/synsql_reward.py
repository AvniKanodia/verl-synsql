import sqlparse
import re
from typing import Dict, Any, Optional, Tuple

def compute_score(data_source: str, solution_str: str, 
                 ground_truth: Dict[str, Any], extra_info: Optional[Dict] = None) -> float:
    """General-purpose reward function with precise scoring"""
    
    # Initialize component scores
    scores = {
        'structure': 0.0,  # Basic JSON structure (1pt)
        'format': 0.0,     # SQL and XML formatting (1pt)
        'execution': 0.0,  # SQL executability (3pt)
        'result': 0.0,     # SQL correctness (3pt)
        'cot': 0.0        # Chain-of-Thought quality (2pt)
    }
    
    # 1. Validate JSON structure (1 point)
    structure_ok = _validate_structure(solution_str)[0]
    scores['structure'] = 1.0 if structure_ok else 0.0
    
    # Early return for completely invalid structure
    if not structure_ok:
        return min(sum(scores.values()), 1.0)
    
    # Extract components from JSON
    components = _extract_components(solution_str)
    
    # 2. Validate formatting (1 point)
    format_ok = _validate_format(components)[0]
    scores['format'] = 1.0 if format_ok else 0.0
    
    # 3. Validate SQL execution (3 points)
    execution_ok, _ = _validate_sql_execution(
        components['sql'],
        ground_truth.get('db_schema'),
        ground_truth['db_id'],
        ground_truth['sql_complexity']
    )
    scores['execution'] = 3.0 if execution_ok else 0.0
    
    # 4. Validate SQL results (3 points)
    if execution_ok:
        result_ok = _validate_sql_result(
            components['sql'],
            ground_truth['expected_sql'],
            ground_truth['sql_complexity'],
            ground_truth['question_style']
        )
        scores['result'] = 3.0 if result_ok else 0.0
    
    # 5. Evaluate CoT quality (2 points)
    scores['cot'] = _evaluate_cot(
        components['cot'],
        ground_truth.get('external_knowledge', ''),
        components['sql'],
        ground_truth['expected_sql'],
        execution_ok and scores['result'] > 0  # Whether SQL is fully correct
    )
    
    # Calculate total with normalization to 10-point scale
    raw_total = sum(scores.values())
    normalized_total = min(10.0, raw_total * (10.0/8.0))  # 8 raw points → 10 normalized
    
    # Apply final adjustments
    if not execution_ok:
        normalized_total = min(normalized_total, 3.0)  # Cap for execution failures
    if not format_ok:
        normalized_total = min(normalized_total, 1.0)  # Cap for format failures
    
    return round(normalized_total, 1)  # Return rounded to 1 decimal

def _evaluate_cot(cot: str, external_knowledge: str, 
                generated_sql: str, expected_sql: str,
                is_sql_correct: bool) -> float:
    """Robust CoT evaluation (0-2 points)"""
    if not cot:
        return 0.0
    
    score = 0.0
    
    # 1. Basic CoT structure (0.5 points)
    if '<think>' in cot and '<answer>' in cot:
        score += 0.5
    
    # 2. SQL component coverage (0.5 points)
    components = ['SELECT', 'FROM', 'WHERE', 'JOIN']
    found = sum(1 for c in components if c in generated_sql.upper() and c.lower() in cot.lower())
    score += 0.5 * (found / len(components))
    
    # 3. External knowledge (0.5 points)
    if external_knowledge and any(knowledge in cot for knowledge in external_knowledge.split(';')):
        score += 0.5
    
    # 4. SQL comparison (0.5 points)
    if not is_sql_correct and expected_sql in cot:
        score += 0.5
    
    return min(2.0, score)

def _validate_sql_execution(sql: str, schema: Optional[Dict], db_id: str, complexity: str) -> Tuple[bool, str]:
    """Strict SQL validation"""
    try:
        parsed = sqlparse.parse(sql)
        if not parsed:
            return False, "Empty SQL"
            
        # Check first statement is valid
        stmt = parsed[0]
        if stmt.get_type() not in ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'):
            return False, f"Invalid statement type: {stmt.get_type()}"
        
        # Schema validation
        if schema:
            tables = _extract_tables(sql)
            for table in tables:
                if table not in schema:
                    return False, f"Table '{table}' not in schema"
            
            columns = _extract_columns(sql)
            for col in columns:
                if '.' in col:
                    table, column = col.split('.')
                    if table in schema and column not in schema[table]:
                        return False, f"Column '{col}' not found"
        
        # Complexity requirements
        if complexity == "Complex":
            if not any(feat in sql.upper() for feat in ['JOIN', 'GROUP BY', 'HAVING']):
                return False, "Missing complex features"
                
        return True, ""
    except Exception as e:
        return False, str(e)

# [Keep all other helper functions the same but fix the warning]
def _extract_json_field(response: str, field: str, default=None) -> str:
    """Fixed regex warning"""
    match = re.search(rf'"{field}":\s*"([^"]+)"', response)  # raw string
    return match.group(1) if match else default

def _validate_structure(response: str) -> Tuple[bool, str]:
    """Validate overall response structure contains all required sections."""
    required_sections = {
        'question': r'"question":\s*"([^"]+)"',
        'cot': r'"cot":\s*"([^"]+)"',
        'sql': r'"sql":\s*"([^"]+)"',
        'db_id': r'"db_id":\s*"([^"]+)"'
    }
    
    for section, pattern in required_sections.items():
        if not re.search(pattern, response):
            return False, f"Missing {section} section"
    return True, ""

def _extract_components(response: str) -> Dict[str, str]:
    """Extract all components from structured response."""
    components = {
        'db_id': _extract_json_field(response, 'db_id'),
        'question': _extract_json_field(response, 'question'),
        'cot': _extract_json_field(response, 'cot'),
        'sql': _extract_json_field(response, 'sql'),
        'sql_complexity': _extract_json_field(response, 'sql_complexity', ''),
        'question_style': _extract_json_field(response, 'question_style', '')
    }
    return components

def _extract_json_field(response: str, field: str, default=None) -> str:
    """Helper to extract field from JSON-like string."""
    match = re.search(f'"{field}":\s*"([^"]+)"', response)
    return match.group(1) if match else default

def _validate_format(components: Dict) -> Tuple[bool, str]:
    """Validate SQL format and XML tagging."""
    # Check SQL formatting
    try:
        sqlparse.parse(components['sql'])
    except Exception:
        return False, "Invalid SQL syntax"
    
    # Check for required XML tags in CoT
    required_tags = ['<think>', '</think>', '<answer>', '</answer>']
    if not all(tag in components['cot'] for tag in required_tags):
        return False, "Missing required XML tags in CoT"
    
    return True, ""

def _validate_sql_result(generated: str, expected: str, complexity: str, style: str) -> bool:
    """Compare SQL results with style/complexity awareness."""
    def normalize(sql: str) -> str:
        return ' '.join(sqlparse.format(sql, reindent=True).split())
    
    # Exact match for imperative questions
    if style == "Imperative":
        return normalize(generated) == normalize(expected)
    
    # Fuzzy match for others
    gen_norm = normalize(generated)
    exp_norm = normalize(expected)
    
    # For simple queries, require exact match
    if complexity == "Simple":
        return gen_norm == exp_norm
    
    # For moderate/complex, compare key components
    key_components = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY']
    return all(
        _compare_sql_component(gen_norm, exp_norm, component)
        for component in key_components
        if component in exp_norm
    )

def _compare_sql_component(gen: str, exp: str, component: str) -> bool:
    """Compare specific SQL components with fuzzy matching."""
    gen_part = re.search(rf'{component}\s+(.*?)(?:\s+(?:WHERE|GROUP BY|ORDER BY|$))', gen, re.IGNORECASE)
    exp_part = re.search(rf'{component}\s+(.*?)(?:\s+(?:WHERE|GROUP BY|ORDER BY|$))', exp, re.IGNORECASE)
    
    if not (gen_part and exp_part):
        return gen_part == exp_part  # Both missing or one missing
    
    # Compare component contents
    return set(gen_part.group(1).split(',')) == set(exp_part.group(1).split(','))

def _has_complex_features(sql: str) -> bool:
    """Check for features expected in complex queries."""
    complex_features = [
        r'\bJOIN\b', r'\bUNION\b', r'\bWITH\b',  # Joins and CTEs
        r'\bHAVING\b', r'\bCASE\b',               # Advanced clauses
        r'\bEXISTS\s*\(', r'\bIN\s*\('            # Subqueries
    ]
    return any(re.search(feature, sql, re.IGNORECASE) for feature in complex_features)

def _extract_tables(sql: str) -> list:
    """Extract all referenced tables."""
    tables = set()
    from_match = re.search(r'FROM\s+(.*?)(?:\s+WHERE|\s+GROUP BY|\s+ORDER BY|\s*$)', sql, re.IGNORECASE)
    if from_match:
        tables.update(t.strip() for t in from_match.group(1).split(','))
    tables.update(re.findall(r'JOIN\s+(\w+)', sql, re.IGNORECASE))
    return list(tables)

def _extract_columns(sql: str) -> list:
    """Extract all referenced columns."""
    columns = set()
    # Select columns
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        columns.update(col.split()[-1] for col in select_match.group(1).split(','))
    # Where conditions
    where_match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP BY|\s+ORDER BY|\s*$)', sql, re.IGNORECASE | re.DOTALL)
    if where_match:
        columns.update(re.findall(r'(\w+\.\w+|\w+)(?=\s*[=<>])', where_match.group(1)))
    return list(columns)