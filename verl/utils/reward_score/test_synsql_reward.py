from synsql_reward import compute_score
import json
from pprint import pprint

def run_tests():
    # Define test database schema
    schemas = {
        "code_snippet_management": {
            "code_snippets": ["id", "description", "complexity", "is_public"],
            "snippet_usage": ["usage_id", "snippet_id", "user_id", "is_successful"],
            "snippet_comments": ["comment_id", "snippet_id", "content"]
        }
    }

    # Test cases
    test_cases = [
        {
            "name": "Perfect response",
            "input": {
                "data_source": "synsql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "question": "Show public code snippets with complexity > 5",
                    "external_knowledge": "",
                    "cot": "<think>Query public snippets with complexity > 5</think><answer>```sql\nSELECT description, complexity\nFROM code_snippets\nWHERE complexity > 5 AND is_public = 1\n```</answer>",
                    "sql": "SELECT description, complexity FROM code_snippets WHERE complexity > 5 AND is_public = 1"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "expected_sql": "SELECT description, complexity FROM code_snippets WHERE complexity > 5 AND is_public = 1",
                    "db_schema": schemas["code_snippet_management"],
                    "external_knowledge": "",
                    "max_length": 1000
                }
            },
            "expected_score": 10.0
        },
        {
            "name": "Missing complexity condition",
            "input": {
                "data_source": "synsql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "question": "Show public code snippets with complexity > 5",
                    "external_knowledge": "",
                    "cot": "<think>Just get public snippets</think><answer>```sql\nSELECT description\nFROM code_snippets\nWHERE is_public = 1\n```</answer>",
                    "sql": "SELECT description FROM code_snippets WHERE is_public = 1"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "expected_sql": "SELECT description, complexity FROM code_snippets WHERE complexity > 5 AND is_public = 1",
                    "db_schema": schemas["code_snippet_management"],
                    "external_knowledge": "",
                    "max_length": 1000
                }
            },
            "expected_score": 4.0
        },
        {
            "name": "Invalid table reference",
            "input": {
                "data_source": "synsql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Moderate",
                    "question_style": "Colloquial",
                    "question": "Get popular snippets",
                    "external_knowledge": "",
                    "cot": "<think>Check popularity table</think><answer>```sql\nSELECT snippet_id\nFROM popularity\nWHERE score > 10\n```</answer>",
                    "sql": "SELECT snippet_id FROM popularity WHERE score > 10"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Moderate",
                    "question_style": "Colloquial",
                    "expected_sql": "SELECT s.id FROM code_snippets s JOIN snippet_usage u ON s.id = u.snippet_id GROUP BY s.id HAVING COUNT(u.usage_id) > 10",
                    "db_schema": schemas["code_snippet_management"],
                    "external_knowledge": "",
                    "max_length": 1000
                }
            },
            "expected_score": 1.0
        },
        {
            "name": "Complex query with JOIN",
            "input": {
                "data_source": "synsql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Complex",
                    "question_style": "Imperative",
                    "question": "Show snippets with comments by user 0",
                    "external_knowledge": "User 0 is admin",
                    "cot": "<think>Join snippets with comments</think><answer>```sql\nSELECT s.description\nFROM code_snippets s\nJOIN snippet_comments c ON s.id = c.snippet_id\nWHERE c.user_id = 0\n```</answer>",
                    "sql": "SELECT s.description FROM code_snippets s JOIN snippet_comments c ON s.id = c.snippet_id WHERE c.user_id = 0"
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Complex",
                    "question_style": "Imperative",
                    "expected_sql": "SELECT s.description FROM code_snippets s JOIN snippet_comments c ON s.id = c.snippet_id WHERE c.user_id = 0",
                    "db_schema": schemas["code_snippet_management"],
                    "external_knowledge": "User 0 is admin",
                    "max_length": 1000
                }
            },
            "expected_score": 8.5
        },
        {
            "name": "Malformed SQL",
            "input": {
                "data_source": "synsql",
                "solution_str": json.dumps({
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "question": "Show public snippets",
                    "external_knowledge": "",
                    "cot": "<think>Get public snippets</think><answer>SELECT description FROM code_snippets WHERE is_public =</answer>",
                    "sql": "SELECT description FROM code_snippets WHERE is_public ="
                }),
                "ground_truth": {
                    "db_id": "code_snippet_management",
                    "sql_complexity": "Simple",
                    "question_style": "Vague",
                    "expected_sql": "SELECT description FROM code_snippets WHERE is_public = 1",
                    "db_schema": schemas["code_snippet_management"],
                    "external_knowledge": "",
                    "max_length": 1000
                }
            },
            "expected_score": 1.0
        }
    ]

    print("🧪 Starting SynSQL Reward Function Tests\n")
    print(f"{'Test Case':<25} | {'Expected':<8} | {'Actual':<8} | {'Status':<6}")
    print("-" * 60)

    for case in test_cases:
        actual_score = compute_score(**case["input"])
        status = "PASS" if abs(actual_score - case["expected_score"]) < 0.5 else "FAIL"
        
        print(f"{case['name']:<25} | {case['expected_score']:<8.1f} | {actual_score:<8.1f} | {status:<6}")

    print("\n✅ Testing complete")

if __name__ == "__main__":
    run_tests()