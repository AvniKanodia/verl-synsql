from NL2SQL_reward import compute_score
import json
from pprint import pprint

def run_tests():
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

    # Test cases
    test_cases = [
        {
            "name": "Perfect response - Simple query",
            "input": {
                "data_source": "nl2sql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "question": "What are the descriptions and complexity scores of those complicated public code snippets?",
                    "external_knowledge": "\"Complicated code snippets\" refers to code snippets with a complexity score greater than 5; 'is_public' equals 1 indicates that the code snippet is publicly available.",
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
                "extra_info": None
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
                    "question": "Can you show me a list of the unique IDs of code snippets that have been viewed and have at least one quality score, and have also been rated with either 4 or 5 stars?",
                    "external_knowledge": "",
                    "cot": "<think>Join tables incorrectly</think><answer>```sql\nSELECT DISTINCT su.snippet_id\nFROM snippet_usage su\nJOIN quality_scores qs ON su.snippet_id = qs.snippet_id\nWHERE su.usage_type = 'view'\n```</answer>",
                    "sql": "SELECT DISTINCT su.snippet_id FROM snippet_usage su JOIN quality_scores qs ON su.snippet_id = qs.snippet_id WHERE su.usage_type = 'view'"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management_and_evaluation",
                    "sql_complexity": "Moderate",
                    "question_style": "Colloquial",
                    "expected_sql": "SELECT DISTINCT su.snippet_id FROM snippet_usage su JOIN quality_scores qs ON su.snippet_id = qs.snippet_id JOIN snippet_ratings sr ON su.snippet_id = sr.snippet_id WHERE su.usage_type = 'view' AND sr.rating_value IN (4, 5)",
                    "db_schema": schemas["code_snippet_management_and_evaluation"],
                    "max_length": 1000
                },
                "extra_info": None
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
                    "external_knowledge": "",
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
                "extra_info": None
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
                    "question": "Could you please gather the descriptions of all public code snippets that were successfully used, have low explanation quality, and were uploaded by users who have uploaded more than one snippet?",
                    "external_knowledge": "",
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
                "extra_info": None
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
                    "external_knowledge": "",
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
                "extra_info": None
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

    print("\n✅ Testing complete")

if __name__ == "__main__":
    run_tests()