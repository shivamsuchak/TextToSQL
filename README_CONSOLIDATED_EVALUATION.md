# Snowflake NL2SQL Evaluation Framework

This repository contains a consolidated evaluation framework for Snowflake NL2SQL systems.

## Files Overview

The framework has been consolidated into three primary files:

1. **`snowflake_nl2sql_evaluation.py`**: Core SQL parsing, component matching, and evaluation functions
   - SQL component extraction and normalization
   - Precision, recall, and F1 score calculations
   - Visualization utilities for evaluation results
   - System comparison utilities

2. **`snowflake_nl2sql_spider2_integration.py`**: Spider2 execution-based evaluation integration
   - Query execution and result comparison functions
   - Integration between component-level and execution-based evaluation

3. **`test_snowflake_nl2sql_evaluation.py`**: Comprehensive test suite
   - Component matching tests
   - Visualization tests
   - Integration tests

## Key Features

### Component-Level Matching

The evaluation framework provides detailed component-level matching for SQL queries:
- SELECT clause comparison
- FROM clause comparison
- WHERE clause comparison
- GROUP BY, ORDER BY, LIMIT, and JOIN comparisons
- Table reference matching

### Comprehensive Metrics

Metrics provided include:
- Precision, recall, and F1 scores for each SQL component
- Overall precision, recall, and F1 scores
- Execution success rate
- Syntax correctness rate
- Semantic success rate
- Table similarity metrics

### Visualization

The framework includes visualization utilities for:
- Bar charts of success rates
- Component matching rates
- Precision, recall, F1 metrics
- System comparison visualizations

### Spider2 Integration

The framework integrates with Spider2's execution-based evaluation approach:
- Query execution against a database
- Result comparison (data equivalence checking)
- Combined metrics from both component and execution evaluation

## Usage

### Basic Component Matching

```python
from snowflake_nl2sql_evaluation import calculate_sql_similarity

pred_sql = "SELECT customer_name FROM customers WHERE age > 30"
gold_sql = "SELECT customers.customer_name FROM customers WHERE customers.age >= 30"

results = calculate_sql_similarity(pred_sql, gold_sql)
print(f"Overall F1: {results['overall_metrics']['f1']}")
```

### Full System Evaluation

```python
from snowflake_nl2sql_evaluation import advanced_spider2_evaluation
from snowflake_nl2sql_spider2_integration import enhanced_spider2_evaluation

# Basic evaluation (without execution)
results = advanced_spider2_evaluation(app, examples, num_samples=10)

# Enhanced evaluation (with execution)
results = enhanced_spider2_evaluation(app, examples, conn, database_name, num_samples=10)

# Visualize results
from snowflake_nl2sql_evaluation import visualize_evaluation_results
visualize_evaluation_results(results, title="Evaluation Results", save_path="eval_results.png")
```

## Notes on Consolidation

This codebase was consolidated from the following original files:
- `sql_component_matcher.py`
- `sql_evaluation.py`
- `spider2_eval_integration.py`
- `test_sql_evaluation.py`
- `test_spider2_integration.py`

The consolidation maintains all the original functionality while simplifying the codebase structure.
