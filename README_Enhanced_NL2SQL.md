# Enhanced Snowflake Natural Language to SQL with Spider2 Architecture

This project implements an advanced Natural Language to SQL (NL2SQL) system for Snowflake databases, enhanced with components inspired by Spider2's agent architecture. The system translates natural language questions into valid Snowflake SQL queries with improved schema understanding, relationship inference, and query generation capabilities.

## Features

### Enhanced Schema Processing

- **Advanced Schema Representation**: Improved extraction and representation of database schemas, including tables, columns, data types, and relationships.
- **Relationship Inference**: Automatically infers relationships between tables based on foreign keys and naming conventions.
- **Schema Analysis**: Analyzes the schema to understand table connections, suggest join paths, and optimize query generation.

### Spider2-Inspired Agent Architecture

- **Multi-Step Planning**: Breaks down complex query generation into logical steps for better query construction.
- **Enhanced SQL Generation**: Improved SQL generation with better handling of joins, filters, and aggregations.
- **Validation and Refinement**: Automatically validates and refines SQL queries to ensure correctness.

### Evaluation and Metrics

- **Comprehensive Evaluation**: Evaluates the system against Spider2-Snow benchmark queries.
- **Multiple Metrics**: Tracks execution accuracy, syntax correctness, and semantic accuracy.
- **Comparative Analysis**: Provides detailed comparison between original and enhanced systems.

## Components

### Main Modules

1. **Schema Processor** (`schema_processor.py`): Enhanced schema understanding and relationship extraction.
2. **SQL Agent** (`sql_agent.py`): Spider2-inspired agent architecture for SQL generation.
3. **Spider2 Integration** (`spider2_integration.py`): Functions to integrate Spider2 components with the original workflow.

### Notebooks

1. **Snowflake_Natural_Language_To_SQL.ipynb**: Original NL2SQL implementation.
2. **Enhanced_Snowflake_NL2SQL_With_Spider2.ipynb**: Enhanced implementation with Spider2 architecture.

## How It Works

1. **Schema Processing**: The `SchemaProcessor` extracts and analyzes the database schema, including tables, columns, and relationships.
2. **Question Analysis**: The system analyzes the natural language question to identify intent, required tables, and operations.
3. **Query Planning**: The `SQLGenerationAgent` creates a step-by-step plan for constructing the SQL query.
4. **SQL Generation**: Based on the plan, the agent generates a SQL query that corresponds to the user's question.
5. **Validation and Refinement**: The system validates the generated SQL and refines it if necessary.
6. **Execution and Response**: The SQL query is executed against the Snowflake database, and results are presented to the user.

## Usage

### Basic Usage

```python
from schema_processor import SchemaProcessor
from sql_agent import SQLGenerationAgent
from langchain_groq import ChatGroq

# Initialize components
llm = ChatGroq(model='llama3-70b-8192')
schema_processor = SchemaProcessor(conn)  # conn is a Snowflake connection
schema_processor.extract_schema_from_connection(database_name, schema_name)
schema_processor.infer_relationships()

# Initialize the SQL agent
sql_agent = SQLGenerationAgent(llm, schema_processor)

# Generate SQL for a natural language question
agent_state = sql_agent.run("How many products are there in each category?")
sql_query = agent_state.final_sql

# Execute the query
# ...
```

### Integration with Existing Workflow

```python
from spider2_integration import convert_nl_to_sql_spider2, enhanced_workflow_with_spider2

# Create enhanced workflow
enhanced_app = enhanced_workflow_with_spider2(app, llm, schema_info, conn, examples)

# Use the enhanced workflow
result = enhanced_app.invoke({
    "question": "List all products with price over $100",
    "sql_query": "",
    "query_result": "",
    "attempts": 0,
    "relevance": "",
    "final_answer": "",
    "sql_error": False,
})
```

## Evaluation Results

The enhanced system shows significant improvements over the original:

- **Execution Success Rate**: Higher rate of successful query execution
- **SQL Syntax Correctness**: Improved syntax correctness in generated queries
- **Semantic Accuracy**: Better understanding of query intent and desired results

## Future Improvements

1. **Fine-tuned Models**: Use Spider2-Snow examples to fine-tune LLMs specifically for Snowflake SQL generation.
2. **Advanced Schema Exploration**: Further enhance schema exploration capabilities for complex enterprise databases.
3. **Error Recovery**: Implement more sophisticated error correction mechanisms.
4. **Integration with Snowflake Functions**: Better support for Snowflake-specific functions and features.

## Acknowledgments

This project builds upon the Spider2 agent architecture and evaluation methodology, adapting it for Snowflake databases.
