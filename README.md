# Text to SQL Conversion

This repository contains Jupyter notebooks for natural language to SQL conversion, with a focus on Snowflake SQL dialect.

## Contents

1. **Snowflake_Natural_Language_To_SQL.ipynb** - Implementation of a natural language to SQL conversion system for Snowflake
2. **Natural_Language_To_SQL_With_Spider_Eval.ipynb** - Evaluation of the NL2SQL system using the Spider benchmark
3. **SQL_Langchain_Agent.ipynb** - LangChain agent implementation for SQL generation

## Features

- Natural language query conversion to SQL
- Snowflake dialect support
- Error analysis and handling
- Comprehensive evaluation framework
- LangChain integration

## Getting Started

To use these notebooks, you'll need:

1. Python 3.8+
2. Jupyter or VS Code with Jupyter extension
3. Required libraries: langchain, langchain-groq, langchain-community, snowflake-connector-python

## Connecting to Snowflake

To connect to Snowflake from the notebooks:

```python
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load credentials from a JSON file or environment variables
conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT_IDENTIFIER',  # Format: orgname-accountname
    warehouse='COMPUTE_WH',  # Your warehouse name
    database='YOUR_DATABASE',  # Target database
    schema='YOUR_SCHEMA'  # Target schema
)

# Create a cursor object
cursor = conn.cursor()

# Example query
cursor.execute('SELECT current_version()')
result = cursor.fetchone()
print(f'Snowflake Version: {result[0]}')

# Close connections when done
cursor.close()
conn.close()
```


