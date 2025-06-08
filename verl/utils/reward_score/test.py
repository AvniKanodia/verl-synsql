import sqlparse
import sqlglot

# sqlparse.parse("SELECT description FROM code_snippets WHERE is_public =")
# print(sqlparse.parse("SELECT description FROM code_snippets WHERE is_public ="))

# print(sqlglot.parse("SELECT description FROM code_snippets WHERE is_public ="))

try:
    sqlglot.parse("SELECT description FROM code_snippets WHERE is_public =")
    parse = True
except:
    parse = False
print(parse)