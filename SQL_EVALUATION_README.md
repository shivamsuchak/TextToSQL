# SQL Evaluation Framework for Snowflake NL2SQL with Spider2

This framework provides comprehensive evaluation metrics for SQL generation systems like the Snowflake NL2SQL with Spider2.

## Components

1. **sql_component_matcher.py** - Provides utility functions for:
   - Parsing SQL queries into components (SELECT, FROM, WHERE, etc.)
   - Normalizing SQL components for comparison
   - Calculating similarity metrics between generated and gold-standard SQL

2. **sql_evaluation.py** - Provides the main evaluation framework:
   - `advanced_spider2_evaluation()` - Enhanced evaluation function for comprehensive metrics
   - `visualize_evaluation_results()` - Creates visualizations for evaluation results
   - `compare_systems()` - Creates visualizations comparing two systems

3. **spider2_eval_integration.py** - Integrates Spider2's execution-based evaluation:
   - `integrate_spider2_execution_eval()` - Combines execution-based and component-level evaluation
   - `enhanced_spider2_evaluation()` - Comprehensive evaluation with Spider2 integration
   - Functions for executing and comparing SQL query results

4. **test_sql_evaluation.py** - Test script to verify the functionality of evaluation components

## Evaluation Metrics

### Basic Metrics
- Execution success rate
- SQL syntax correctness rate 
- Semantic success rate
- Execution match rate (when Spider2 integration is used)

### Component Matching Metrics
For each SQL component (SELECT, FROM, WHERE, etc.):
- Precision
- Recall
- F1 Score
- Component match rate

### Overall Metrics
- Overall precision
- Overall recall
- Overall F1 score
- Table similarity

### Spider2 Integration Metrics
When database connection is available:
- Execution match rate (comparing query results)
- Result set similarity
- Execution error analysis

### Detailed Analysis
- Component-level analysis
- Error analysis
- Visualizations

## Usage

The enhanced evaluation functions can be used directly in the Snowflake NL2SQL notebook:

### Basic Component-Level Evaluation

```python
from sql_evaluation import advanced_spider2_evaluation, visualize_evaluation_results, compare_systems

# Run the advanced evaluation with component-level metrics
original_eval = advanced_spider2_evaluation(app, examples, num_samples=5)
enhanced_eval = advanced_spider2_evaluation(enhanced_app, examples, num_samples=5)

# Visualize the results
visualize_evaluation_results(original_eval, title="Original System Results")
visualize_evaluation_results(enhanced_eval, title="Enhanced System Results")

# Compare the systems
compare_systems(original_eval, enhanced_eval, title="System Comparison")
```

### Integrated Spider2 Evaluation (with Database Connection)

```python
from sql_evaluation import advanced_spider2_evaluation, visualize_evaluation_results, compare_systems

# Run the advanced evaluation with both component-level and execution-based metrics
original_eval = advanced_spider2_evaluation(
    app, 
    examples, 
    num_samples=5,
    conn=conn,                 # Database connection
    database_name="MYDATABASE"  # Database name
)

enhanced_eval = advanced_spider2_evaluation(
    enhanced_app, 
    examples, 
    num_samples=5,
    conn=conn,
    database_name="MYDATABASE"
)

# Visualize and compare as above
```

## Benefits

This enhanced evaluation framework provides several advantages:

1. **More comprehensive metrics** - Goes beyond basic execution success to evaluate SQL quality
2. **Component-level analysis** - Helps identify specific strengths and weaknesses in SQL generation
3. **Visualization support** - Makes it easier to interpret results and present findings
4. **Error analysis** - Facilitates understanding of failure cases and improvement opportunities

## Future Enhancements

Potential future improvements to the evaluation framework:

1. Deeper integration with Spider2's evaluation scripts for more standardized benchmarking
2. Support for evaluating more complex SQL features like subqueries and window functions
3. Automated suggestions for improving SQL generation based on evaluation results
4. Enhanced visualization of query execution plans and result differences
5. Integration with more database systems beyond Snowflake
