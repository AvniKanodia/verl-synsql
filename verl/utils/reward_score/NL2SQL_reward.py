import sqlglot
from sqlglot import exp, ParseError
import json
from typing import Dict, Any, Optional

def compute_score(data_source: str, solution_str: str, 
                 ground_truth: Dict[str, Any], extra_info: Optional[Dict] = None) -> float:
    """
    Final optimized NL2SQL reward function with enhanced complex query handling
    """
    try:
        # Parse response and extract SQL
        response = json.loads(solution_str)
        cot = response.get('cot', '')
        sql = _extract_sql(cot)
        
        # Format validation (1 point)
        if not sql or not _validate_format(cot):
            return 0.0
        score = 1.0
        
        # SQL Validation (2 points)
        try:
            parsed = sqlglot.parse(sql, read="sqlite")[0]
            
            # Basic SELECT validation - allow complex queries through
            if not isinstance(parsed, exp.Select):
                return score
                
            # Schema validation - more permissive for complex queries
            if ground_truth.get('db_schema'):
                schema_valid = _validate_schema(parsed, ground_truth['db_schema'])
                if not schema_valid:
                    if ground_truth['sql_complexity'] != "Complex":
                        return score
                    # For complex queries, try fuzzy schema matching
                    if not _fuzzy_validate_schema(parsed, ground_truth['db_schema']):
                        return score
                        
            score += 2.0  # Always award execution points if we got this far
            
            # Special handling for complex query comparison
            expected_parsed = sqlglot.parse(ground_truth['expected_sql'], read="sqlite")[0]
            if ground_truth['sql_complexity'] == "Complex":
                if _compare_complex_queries(parsed, expected_parsed):
                    score += 3.0
            else:
                if parsed.sql(normalize=True) == expected_parsed.sql(normalize=True):
                    score += 3.0
                    
        except ParseError:
            return score
            
        return score
        
    except json.JSONDecodeError:
        return 0.0

def _validate_schema(parsed: exp.Expression, schema: Dict[str, list]) -> bool:
    """Enhanced schema validation with proper table/column checking"""
    # Get all referenced tables with aliases
    table_refs = {}
    for table in parsed.find_all(exp.Table):
        table_refs[table.alias_or_name.lower()] = table.name.lower()
    
    # Check all base tables exist
    schema_tables = {t.lower() for t in schema.keys()}
    if not {t.lower() for t in table_refs.values()}.issubset(schema_tables):
        return False
        
    # Check columns
    for column in parsed.find_all(exp.Column):
        table_name = None
        
        # Resolve table reference through aliases
        if column.table:
            table_name = table_refs.get(column.table.lower(), column.table.lower())
        
        # Validate column exists in its table
        if table_name in schema:
            valid_columns = {c.lower() for c in schema[table_name]}
            if column.name.lower() not in valid_columns:
                return False
                
    return True

def _fuzzy_validate_schema(parsed: exp.Expression, schema: Dict[str, list]) -> bool:
    """More permissive schema validation for complex queries"""
    # Get all referenced tables
    tables = {t.name.lower() for t in parsed.find_all(exp.Table)}
    schema_tables = {t.lower() for t in schema.keys()}
    
    # Allow partial matches for complex queries
    if not tables.issubset(schema_tables):
        # Check if at least one table matches
        return len(tables & schema_tables) > 0
        
    return True

def _sql_equivalence(actual: exp.Expression, expected: exp.Expression,
                    question_style: str, complexity: str) -> bool:
    """
    Enhanced SQL comparison with special handling for complex JOIN queries
    """
    # For complex queries, use specialized JOIN-aware comparison
    if complexity == "Complex":
        return _compare_complex_queries(actual, expected)
    elif question_style == "Vague":
        return _compare_ast_component(actual, expected, 'where')
    else:
        return actual.sql(normalize=True) == expected.sql(normalize=True)

def _compare_complex_queries(actual: exp.Expression, expected: exp.Expression) -> bool:
    """Specialized comparison for complex queries with JOINs"""
    # Compare SELECT clauses
    if not _compare_selects(actual, expected):
        return False
        
    # Compare JOIN structures
    if not _compare_join_structures(actual, expected):
        return False
        
    # Compare WHERE clauses if present
    if not _compare_where_clauses(actual, expected):
        return False
        
    return True

def _compare_selects(a: exp.Expression, b: exp.Expression) -> bool:
    """Compare SELECT clauses with column order insensitivity"""
    a_selects = {col.sql(normalize=True) for col in a.find_all(exp.Column)}
    b_selects = {col.sql(normalize=True) for col in b.find_all(exp.Column)}
    return a_selects == b_selects

def _compare_join_structures(a: exp.Expression, b: exp.Expression) -> bool:
    """Compare JOIN structures with order insensitivity"""
    a_joins = {}
    for join in a.find_all(exp.Join):
        key = join.this.sql(normalize=True)
        a_joins[key] = join.args.get("on", "").sql(normalize=True) if join.args.get("on") else ""
    
    b_joins = {}
    for join in b.find_all(exp.Join):
        key = join.this.sql(normalize=True)
        b_joins[key] = join.args.get("on", "").sql(normalize=True) if join.args.get("on") else ""
    
    return a_joins == b_joins

def _compare_where_clauses(a: exp.Expression, b: exp.Expression) -> bool:
    """Compare WHERE clauses with condition order insensitivity"""
    a_where = a.find(exp.Where)
    b_where = b.find(exp.Where)
    
    if not a_where and not b_where:
        return True
    if bool(a_where) != bool(b_where):
        return False
        
    # Split conditions and compare sets
    a_conditions = {cond.sql(normalize=True) for cond in a_where.find_all(exp.Condition)}
    b_conditions = {cond.sql(normalize=True) for cond in b_where.find_all(exp.Condition)}
    
    return a_conditions == b_conditions

def _compare_joins(a: exp.Expression, b: exp.Expression) -> bool:
    """Robust JOIN comparison that handles different JOIN orders"""
    a_tables = {}
    b_tables = {}
    
    # Extract all table references with their join conditions
    for join in a.find_all(exp.Join):
        a_tables[join.this.sql()] = join.args.get("on")
    for join in b.find_all(exp.Join):
        b_tables[join.this.sql()] = join.args.get("on")
    
    # Compare table sets and join conditions
    if set(a_tables.keys()) != set(b_tables.keys()):
        return False
    
    # Compare join conditions (normalized)
    for table, a_cond in a_tables.items():
        b_cond = b_tables[table]
        if a_cond and b_cond:
            if a_cond.sql(normalize=True) != b_cond.sql(normalize=True):
                return False
        elif a_cond or b_cond:
            return False
    
    return True

def _compare_ast_component(a: exp.Expression, b: exp.Expression, 
                          component: str) -> bool:
    """Robust component comparison with JOIN handling"""
    a_comp = a.find(getattr(exp, component.capitalize()))
    b_comp = b.find(getattr(exp, component.capitalize()))
    
    if not a_comp and not b_comp:
        return True
    if bool(a_comp) != bool(b_comp):
        return False
        
    return a_comp.sql(normalize=True) == b_comp.sql(normalize=True)

def _extract_sql(response: str) -> str:
    """Extract SQL from markdown code block"""
    try:
        start = response.index('```sql') + 6
        end = response.index('```', start)
        return response[start:end].strip()
    except ValueError:
        return ""

def _validate_format(response: str) -> bool:
    """Validate required XML tags exist"""
    return ('<think>' in response and 
            '</think>' in response and
            '<answer>' in response and
            '</answer>' in response)