from NL2SQL_reward import compute_score
import json
import sqlite3
import os

def create_mock_db():
    """Create an in-memory SQLite database with test schema and data"""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    
    # Create tables and insert test data
    cursor.execute("""
    CREATE TABLE code_snippets (
        snippet_id INTEGER PRIMARY KEY,
        description TEXT,
        complexity INTEGER,
        is_public INTEGER,
        uploaded_by INTEGER
    )""")
    
    cursor.execute("""
    CREATE TABLE snippet_usage (
        usage_id INTEGER PRIMARY KEY,
        snippet_id INTEGER,
        user_id INTEGER,
        usage_type TEXT,
        is_successful INTEGER
    )""")
    
    cursor.execute("""
    CREATE TABLE snippet_comments (
        comment_id INTEGER PRIMARY KEY,
        snippet_id INTEGER,
        content TEXT
    )""")
    
    cursor.execute("""
    CREATE TABLE quality_scores (
        score_id INTEGER PRIMARY KEY,
        snippet_id INTEGER,
        explanation_quality INTEGER
    )""")
    
    cursor.execute("""
    CREATE TABLE snippet_ratings (
        rating_id INTEGER PRIMARY KEY,
        snippet_id INTEGER,
        rating_value INTEGER
    )""")
    
    # Insert test data - using proper executemany format
    code_snippets_data = [
        (1, 'Hello World', 3, 1, 1),
        (2, 'Sort algorithm', 7, 1, 1),
        (3, 'Database connection', 6, 0, 2),
        (4, 'Web server', 8, 1, 2)
    ]
    cursor.executemany(
        "INSERT INTO code_snippets VALUES (?, ?, ?, ?, ?)",
        code_snippets_data
    )
    
    snippet_usage_data = [
        (1, 1, 0, 'view', 1),
        (2, 2, 0, 'view', 1),
        (3, 2, 1, 'view', 1),
        (4, 4, 0, 'view', 1)
    ]
    cursor.executemany(
        "INSERT INTO snippet_usage VALUES (?, ?, ?, ?, ?)",
        snippet_usage_data
    )
    
    snippet_ratings_data = [
        (1, 1, 4),
        (2, 2, 5),
        (3, 4, 3)
    ]
    cursor.executemany(
        "INSERT INTO snippet_ratings VALUES (?, ?, ?)",
        snippet_ratings_data
    )
    
    quality_scores_data = [
        (1, 1, 1),
        (2, 2, 0),
        (3, 4, 1)
    ]
    cursor.executemany(
        "INSERT INTO quality_scores VALUES (?, ?, ?)",
        quality_scores_data
    )
    
    conn.commit()
    return conn

def run_tests():
    # Create mock database connection
    db_conn = create_mock_db()
    
    # Define test database schema
    schemas = {
        "code_snippet_management_and_evaluation": {
            "code_snippets": ["snippet_id", "description", "complexity", "is_public", "uploaded_by"],
            "snippet_usage": ["usage_id", "snippet_id", "user_id", "usage_type", "is_successful"],
            "snippet_comments": ["comment_id", "snippet_id", "content"],
            "quality_scores": ["score_id", "snippet_id", "explanation_quality"],
            "snippet_ratings": ["rating_id", "snippet_id", "rating_value"]
        }
    }

    # Test cases - now with proper extra_info handling
    test_cases = [
        {
            "name": "Perfect response - Simple query",
            "input": {
                "data_source": "nl2sql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "question": "What are the descriptions and complexity scores of public code snippets with complexity > 5?",
                    "cot": "<think>Query public snippets with complexity > 5</think><answer>```sql\nSELECT description, complexity\nFROM code_snippets\nWHERE complexity > 5 AND is_public = 1\n```</answer>",
                    "sql": "SELECT description, complexity FROM code_snippets WHERE complexity > 5 AND is_public = 1"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "expected_sql": "SELECT description, complexity FROM code_snippets WHERE complexity > 5 AND is_public = 1",
                    "db_schema": schemas["code_snippet_management_and_evaluation"],
                    "max_length": 1000
                },
                "extra_info": {"db_connection": db_conn}  # Add db connection
            },
            "expected_score": 6.0  # Format(1) + Execution(2) + Result(3)
        },
        {
            "name": "Correct format but wrong result - Moderate query",
            "input": {
                "data_source": "nl2sql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Moderate",
                    "question_style": "Colloquial",
                    "question": "Show unique IDs of snippets viewed by user 0 with quality scores and ratings 4-5",
                    "cot": "<think>Join tables incorrectly</think><answer>```sql\nSELECT DISTINCT su.snippet_id\nFROM snippet_usage su\nJOIN quality_scores qs ON su.snippet_id = qs.snippet_id\nWHERE su.usage_type = 'view'\n```</answer>",
                    "sql": "SELECT DISTINCT su.snippet_id FROM snippet_usage su JOIN quality_scores qs ON su.snippet_id = qs.snippet_id WHERE su.usage_type = 'view'"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Moderate",
                    "question_style": "Colloquial",
                    "expected_sql": "SELECT DISTINCT su.snippet_id FROM snippet_usage su JOIN quality_scores qs ON su.snippet_id = qs.snippet_id JOIN snippet_ratings sr ON su.snippet_id = sr.snippet_id WHERE su.user_id = 0 AND su.usage_type = 'view' AND sr.rating_value IN (4, 5)",
                    "db_schema": schemas["code_snippet_management_and_evaluation"],
                    "max_length": 1000
                },
                "extra_info": {"db_connection": db_conn}  # Add db connection
            },
            "expected_score": 3.0  # Format(1) + Execution(2) + Result(0)
        },
        {
            "name": "Invalid table reference",
            "input": {
                "data_source": "nl2sql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "question": "Show public code snippets",
                    "cot": "<think>Use wrong table</think><answer>```sql\nSELECT description\nFROM public_snippets\nWHERE is_public = 1\n```</answer>",
                    "sql": "SELECT description FROM public_snippets WHERE is_public = 1"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "expected_sql": "SELECT description FROM code_snippets WHERE is_public = 1",
                    "db_schema": schemas["code_snippet_management_and_evaluation"],
                    "max_length": 1000
                },
                "extra_info": None  # No DB needed for schema validation
            },
            "expected_score": 1.0  # Format(1) + Execution(-2) + Result(0)
        },
        {
            "name": "Complex query with correct JOINs",
            "input": {
                "data_source": "nl2sql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Complex",
                    "question_style": "Imperative",
                    "question": "Get descriptions of public snippets successfully used with low quality from users with >1 snippet",
                    "cot": "<think>Complex multi-table query</think><answer>```sql\nWITH ActiveUsers AS (\n    SELECT uploaded_by\n    FROM code_snippets\n    GROUP BY uploaded_by\n    HAVING COUNT(snippet_id) > 1\n)\nSELECT description\nFROM code_snippets cs\nJOIN ActiveUsers au ON cs.uploaded_by = au.uploaded_by\nJOIN snippet_usage su ON cs.snippet_id = su.snippet_id\nJOIN quality_scores qs ON cs.snippet_id = qs.snippet_id\nWHERE cs.is_public = 1 AND su.is_successful = 1 AND qs.explanation_quality = 0\n```</answer>",
                    "sql": "WITH ActiveUsers AS (SELECT uploaded_by FROM code_snippets GROUP BY uploaded_by HAVING COUNT(snippet_id) > 1) SELECT description FROM code_snippets cs JOIN ActiveUsers au ON cs.uploaded_by = au.uploaded_by JOIN snippet_usage su ON cs.snippet_id = su.snippet_id JOIN quality_scores qs ON cs.snippet_id = qs.snippet_id WHERE cs.is_public = 1 AND su.is_successful = 1 AND qs.explanation_quality = 0"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Complex",
                    "question_style": "Imperative",
                    "expected_sql": "WITH ActiveUsers AS (SELECT uploaded_by FROM code_snippets GROUP BY uploaded_by HAVING COUNT(snippet_id) > 1) SELECT description FROM code_snippets cs JOIN ActiveUsers au ON cs.uploaded_by = au.uploaded_by JOIN snippet_usage su ON cs.snippet_id = su.snippet_id JOIN quality_scores qs ON cs.snippet_id = qs.snippet_id WHERE cs.is_public = 1 AND su.is_successful = 1 AND qs.explanation_quality = 0",
                    "db_schema": schemas["code_snippet_management_and_evaluation"],
                    "max_length": 1000
                },
                "extra_info": {"db_connection": db_conn}  # Add db connection
            },
            "expected_score": 6.0  # Format(1) + Execution(2) + Result(3)
        },
        {
            "name": "Malformed SQL syntax",
            "input": {
                "data_source": "nl2sql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "question": "Show public snippets",
                    "cot": "<think>Bad SQL</think><answer>```sql\nSELECT description FROM code_snippets WHERE is_public =\n```</answer>",
                    "sql": "SELECT description FROM code_snippets WHERE is_public ="
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "expected_sql": "SELECT description FROM code_snippets WHERE is_public = 1",
                    "db_schema": schemas["code_snippet_management_and_evaluation"],
                    "max_length": 1000
                },
                "extra_info": None  # No DB needed for syntax validation
            },
            "expected_score": 1.0  # Format(1) + Execution(-2) + Result(0)
        }
    ]

    print("🧪 Starting NL2SQL Reward Function Tests\n")
    print(f"{'Test Case':<50} | {'Expected':<8} | {'Actual':<8} | {'Status':<6}")
    print("-" * 80)

    for case in test_cases:
        try:
            actual_score = compute_score(
                data_source=case["input"]["data_source"],
                solution_str=case["input"]["solution_str"],
                ground_truth=case["input"]["ground_truth"],
                extra_info=case["input"]["extra_info"]
            )
            status = "PASS" if abs(actual_score - case["expected_score"]) < 0.5 else "FAIL"
            
            print(f"{case['name']:<50} | {case['expected_score']:<8.1f} | {actual_score:<8.1f} | {status:<6}")
        except Exception as e:
            print(f"{case['name']:<50} | ERROR: {str(e)}")

    # Clean up
    db_conn.close()
    try:
        os.unlink(db_conn.path)
    except:
        pass
        
    print("\n✅ Testing complete")

if __name__ == "__main__":
    run_tests()