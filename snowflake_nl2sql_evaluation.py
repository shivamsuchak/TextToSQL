"""
Consolidated SQL Evaluation Framework for Snowflake NL2SQL

This module provides a comprehensive evaluation framework for SQL queries,
combining component-level analysis with execution-based evaluation.
It includes utilities for:
1. SQL component parsing and matching
2. Component-level metrics (precision, recall, F1)
3. Visualization of evaluation results
4. Integration with execution-based evaluation
"""

import re
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, List, Any, Tuple, Optional, Set
from collections import defaultdict

# -----------------------------------------------------------------------------
# SQL Component Matcher - Core SQL Parsing and Comparison Functions 
# -----------------------------------------------------------------------------

# SQL component patterns for parsing
SQL_COMPONENT_PATTERNS = {
    'select': r'SELECT\s+(DISTINCT\s+)?(.+?)(?=\s+FROM|\s*$)',
    'from': r'FROM\s+(.+?)(?=\s+WHERE|\s+GROUP BY|\s+ORDER BY|\s+LIMIT|\s+HAVING|\s*$)',
    'where': r'WHERE\s+(.+?)(?=\s+GROUP BY|\s+ORDER BY|\s+LIMIT|\s+HAVING|\s*$)',
    'group_by': r'GROUP BY\s+(.+?)(?=\s+ORDER BY|\s+LIMIT|\s+HAVING|\s*$)',
    'having': r'HAVING\s+(.+?)(?=\s+ORDER BY|\s+LIMIT|\s*$)',
    'order_by': r'ORDER BY\s+(.+?)(?=\s+LIMIT|\s*$)',
    'limit': r'LIMIT\s+(\d+)',
    'join': r'(LEFT |RIGHT |INNER |OUTER |CROSS |FULL |)JOIN\s+(.+?)(?=\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+LIMIT|\s*$)',
}

def extract_sql_components(sql: str) -> Dict[str, str]:
    """
    Extract different components from an SQL query.
    
    Args:
        sql: The SQL query to parse
        
    Returns:
        A dictionary with components as keys and their values
    """
    if not sql:
        return {}
    
    # Normalize the SQL query
    sql = ' '.join(sql.strip().replace('\n', ' ').split()).upper()
    
    components = {}
    for component, pattern in SQL_COMPONENT_PATTERNS.items():
        matches = re.search(pattern, sql, re.IGNORECASE)
        if matches:
            # Get the last group which contains what we need
            components[component] = matches.group(matches.lastindex).strip()
            
    # Special handling for WITH queries (CTEs)
    if sql.startswith('WITH '):
        with_match = re.match(r'WITH\s+(.+?)(?=\s+SELECT)', sql, re.IGNORECASE | re.DOTALL)
        if with_match:
            components['with'] = with_match.group(1).strip()
            
    return components

def normalize_sql_component(component_text: str) -> Set[str]:
    """
    Normalize a SQL component text for comparison.
    
    Args:
        component_text: The component text to normalize
        
    Returns:
        A set of normalized tokens
    """
    # Remove extra whitespace, lowercase, and split by common delimiters
    if not component_text:
        return set()
    
    # Remove aliases (AS keyword)
    component_text = re.sub(r'\s+AS\s+\w+', '', component_text, flags=re.IGNORECASE)
    
    # Handle special case for SELECT *
    if component_text.strip() == '*':
        return {'*'}
    
    # Split by commas for things like SELECT col1, col2, col3
    items = re.split(r',\s*', component_text)
    
    # For each item, clean it up
    normalized = set()
    for item in items:
        # Remove table prefixes (e.g., table_name.column_name -> column_name)
        item = re.sub(r'^\w+\.', '', item.strip())
        # Remove functions but keep their arguments
        item = re.sub(r'\w+\((.*)\)', r'\1', item)
        # Add to set if not empty
        if item:
            normalized.add(item.strip())
            
    return normalized

def calculate_component_metrics(pred_set: Set[str], gold_set: Set[str]) -> Dict[str, float]:
    """
    Calculate precision, recall, and F1 score for a component.
    
    Args:
        pred_set: Normalized predicted component items
        gold_set: Normalized gold component items
        
    Returns:
        Dict with precision, recall, F1
    """
    if not pred_set and not gold_set:
        return {'precision': 1.0, 'recall': 1.0, 'f1': 1.0}
    
    if not pred_set:
        return {'precision': 0.0, 'recall': 0.0, 'f1': 0.0}
    
    if not gold_set:
        return {'precision': 0.0, 'recall': 0.0, 'f1': 0.0}
    
    # Find intersection
    intersection = pred_set.intersection(gold_set)
    
    precision = len(intersection) / len(pred_set) if pred_set else 0.0
    recall = len(intersection) / len(gold_set) if gold_set else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return {'precision': precision, 'recall': recall, 'f1': f1}

def compare_sql_components(pred_components: Dict[str, str], 
                         gold_components: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """
    Compare SQL components between predicted and gold queries.
    
    Args:
        pred_components: Components from predicted SQL
        gold_components: Components from gold standard SQL
        
    Returns:
        Dict with precision, recall, F1, and component items for each SQL component
    """
    results = {}
    
    for component in set(pred_components.keys()).union(gold_components.keys()):
        pred_set = normalize_sql_component(pred_components.get(component, ''))
        gold_set = normalize_sql_component(gold_components.get(component, ''))
        
        # Calculate metrics
        metrics = calculate_component_metrics(pred_set, gold_set)
        
        results[component] = {
            **metrics,
            'pred': list(pred_set),
            'gold': list(gold_set)
        }
    
    return results

def extract_table_names(sql: str) -> List[str]:
    """
    Extract all table names referenced in an SQL query.
    
    Args:
        sql: The SQL query
        
    Returns:
        List of table names
    """
    if not sql:
        return []
    
    tables = []
    
    # Look for tables in FROM clause
    from_match = re.search(r'FROM\s+(.+?)(?=\s+WHERE|\s+GROUP BY|\s+ORDER BY|\s+LIMIT|\s+HAVING|\s*$)', 
                         sql, re.IGNORECASE)
    if from_match:
        # Split by commas and handle joins
        from_text = from_match.group(1)
        # Remove JOIN statements first
        from_text = re.sub(r'(LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+JOIN.*?ON.*?(?=,|\s+(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+JOIN|\s*$)',
                         '', from_text, flags=re.IGNORECASE)
        # Split by commas
        table_parts = re.split(r',\s*', from_text)
        for part in table_parts:
            # Extract table name (ignoring aliases)
            table_name = part.split(' ')[0].strip()
            if table_name:
                tables.append(table_name)
    
    # Look for tables in JOIN clauses
    join_matches = re.finditer(r'JOIN\s+(\w+)', sql, re.IGNORECASE)
    for match in join_matches:
        tables.append(match.group(1))
    
    return tables

def calculate_sql_similarity(pred_sql: str, gold_sql: str) -> Dict[str, Any]:
    """
    Calculate overall similarity between predicted and gold SQL queries.
    
    Args:
        pred_sql: The predicted SQL query
        gold_sql: The gold standard SQL query
        
    Returns:
        Dictionary with component-wise and overall metrics
    """
    # Parse components
    pred_components = extract_sql_components(pred_sql)
    gold_components = extract_sql_components(gold_sql)
    
    # Compare components
    component_results = compare_sql_components(pred_components, gold_components)
    
    # Extract tables
    pred_tables = extract_table_names(pred_sql)
    gold_tables = extract_table_names(gold_sql)
    
    # Calculate table overlap (simple Jaccard similarity)
    table_intersection = len(set(pred_tables).intersection(set(gold_tables)))
    table_union = len(set(pred_tables).union(set(gold_tables)))
    table_similarity = table_intersection / table_union if table_union > 0 else 0.0
    
    # Calculate overall metrics by averaging component F1 scores
    f1_scores = [metrics['f1'] for metrics in component_results.values()]
    precision_scores = [metrics['precision'] for metrics in component_results.values()]
    recall_scores = [metrics['recall'] for metrics in component_results.values()]
    
    overall = {
        'f1': sum(f1_scores) / len(f1_scores) if f1_scores else 0.0,
        'precision': sum(precision_scores) / len(precision_scores) if precision_scores else 0.0,
        'recall': sum(recall_scores) / len(recall_scores) if recall_scores else 0.0,
        'table_similarity': table_similarity
    }
    
    return {
        'component_metrics': component_results,
        'overall_metrics': overall,
        'pred_tables': pred_tables,
        'gold_tables': gold_tables
    }

# -----------------------------------------------------------------------------
# SQL Evaluation - Main Evaluation Framework
# -----------------------------------------------------------------------------

# Import Spider2 integration if available
try:
    from spider2_integration import integrate_spider2_execution_eval, enhanced_spider2_evaluation
    SPIDER2_INTEGRATION_AVAILABLE = True
except ImportError:
    SPIDER2_INTEGRATION_AVAILABLE = False

def advanced_spider2_evaluation(app, examples, num_samples=5, conn=None, database_name=None):
    """
    Comprehensive evaluation function for SQL generation systems.
    
    Args:
        app: The application (workflow) to evaluate
        examples: List of examples with questions and gold SQL queries
        num_samples: Number of samples to evaluate
        conn: Optional database connection for execution-based evaluation
        database_name: Optional database name to use with connection
        
    Returns:
        Dictionary with detailed evaluation metrics
    """
    # If Spider2 integration is available and connection is provided, use enhanced evaluation
    if SPIDER2_INTEGRATION_AVAILABLE and conn:
        from spider2_integration import enhanced_spider2_evaluation
        return enhanced_spider2_evaluation(app, examples, conn, database_name, num_samples)
    
    # Otherwise, fall back to our component-based evaluation
    results = {
        "total": min(num_samples, len(examples)),
        "execution_success": 0,
        "syntax_correctness": 0,
        "semantic_success": 0,
        "component_matches": {
            "select": {"correct": 0, "total": 0},
            "from": {"correct": 0, "total": 0},
            "where": {"correct": 0, "total": 0},
            "group_by": {"correct": 0, "total": 0},
            "having": {"correct": 0, "total": 0},
            "order_by": {"correct": 0, "total": 0},
            "limit": {"correct": 0, "total": 0},
        },
        "overall_metrics": {
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0
        },
        "details": []
    }
    
    # Limit examples for testing
    eval_examples = examples[:num_samples]
    
    # Component-level aggregates for final metrics
    all_precisions = []
    all_recalls = []
    all_f1s = []
    
    for i, example in enumerate(eval_examples):
        print(f"Evaluating example {i+1}/{len(eval_examples)}")
        
        question = example.get("instruction", "")
        gold_sql = example.get("sql", "")
        
        if not question:
            print(f"Skipping example {i+1} - no question found")
            continue
        
        # Initialize state
        state = {
            "question": question,
            "sql_query": "",
            "query_result": "",
            "query_rows": [],
            "attempts": 0,
            "relevance": "",
            "final_answer": "",
            "sql_error": False,
        }
        
        # Run our workflow
        try:
            result = app.invoke(state)
            generated_sql = result.get("sql_query", "")
            execution_success = not result.get("sql_error", True)
            
            # Check SQL syntax (simplified)
            syntax_correct = True
            if not generated_sql or generated_sql.strip() == "":
                syntax_correct = False
            elif not (generated_sql.lower().startswith("select") or 
                    generated_sql.lower().startswith("with")):
                syntax_correct = False
                
            # Simplified semantic check
            semantic_correct = execution_success and result.get("query_result", "") != "No data found for the specified query."
            
            # Extract and compare SQL components
            pred_components = extract_sql_components(generated_sql)
            gold_components = extract_sql_components(gold_sql)
            component_comparison = compare_sql_components(pred_components, gold_components)
            
            # Update component matching metrics
            for component, metrics in component_comparison.items():
                if component in results["component_matches"]:
                    results["component_matches"][component]["total"] += 1
                    # A component is considered correct if F1 >= 0.8
                    if metrics["f1"] >= 0.8:
                        results["component_matches"][component]["correct"] += 1
                
                # Collect metrics for overall calculation
                all_precisions.append(metrics["precision"])
                all_recalls.append(metrics["recall"])
                all_f1s.append(metrics["f1"])
            
            # Record basic metrics
            if execution_success:
                results["execution_success"] += 1
                
            if syntax_correct:
                results["syntax_correctness"] += 1
                
            if semantic_correct:
                results["semantic_success"] += 1
                
            # Store detailed results
            results["details"].append({
                "id": i,
                "question": question,
                "gold_sql": gold_sql,
                "generated_sql": generated_sql,
                "execution_success": execution_success,
                "syntax_correct": syntax_correct,
                "semantic_correct": semantic_correct,
                "query_result": result.get("query_result", "")[:500],  # Truncate long results
                "component_comparison": component_comparison
            })
                
        except Exception as e:
            print(f"Error processing example {i}: {e}")
            results["details"].append({
                "id": i,
                "question": question,
                "gold_sql": gold_sql,
                "error": str(e)
            })
    
    # Calculate overall metrics
    total = results["total"]
    if total > 0:
        results["execution_success_rate"] = results["execution_success"] / total
        results["syntax_correctness_rate"] = results["syntax_correctness"] / total
        results["semantic_success_rate"] = results["semantic_success"] / total
        
        # Add component matching rates
        for component in results["component_matches"]:
            comp_total = results["component_matches"][component]["total"]
            if comp_total > 0:
                results["component_matches"][component]["rate"] = results["component_matches"][component]["correct"] / comp_total
            else:
                results["component_matches"][component]["rate"] = 0.0
    
    # Calculate mean precision, recall, and F1 scores
    if all_precisions:
        results["overall_metrics"]["precision"] = np.mean(all_precisions)
    if all_recalls:  
        results["overall_metrics"]["recall"] = np.mean(all_recalls)
    if all_f1s:
        results["overall_metrics"]["f1"] = np.mean(all_f1s)
    
    return results

def visualize_evaluation_results(eval_results, title="SQL Evaluation Results", save_path=None):
    """
    Create visualizations for the evaluation results.
    
    Args:
        eval_results: The evaluation results dictionary
        title: Title for the visualization
        save_path: Path to save the visualization, if None, it will be displayed interactively
        
    Returns:
        None
    """
    plt.figure(figsize=(14, 10))
    
    # Plot 1: Basic Metrics
    plt.subplot(2, 2, 1)
    basic_metrics = [
        eval_results.get("execution_success_rate", 0), 
        eval_results.get("syntax_correctness_rate", 0), 
        eval_results.get("semantic_success_rate", 0)
    ]
    labels = ["Execution Success", "Syntax Correctness", "Semantic Success"]
    
    # Add execution match rate if available
    if "execution_match_rate" in eval_results:
        basic_metrics.append(eval_results.get("execution_match_rate", 0))
        labels.append("Execution Match")
    
    plt.bar(labels, basic_metrics, color='skyblue')
    plt.title("Basic Evaluation Metrics")
    plt.ylabel("Success Rate")
    plt.ylim(0, 1)
    for i, v in enumerate(basic_metrics):
        plt.text(i, v + 0.02, f"{v:.2f}", ha='center')
    
    # Plot 2: Component Matching Rates
    plt.subplot(2, 2, 2)
    components = []
    rates = []
    
    for component, metrics in eval_results.get("component_matches", {}).items():
        if "rate" in metrics:
            components.append(component.upper())
            rates.append(metrics["rate"])
    
    plt.bar(components, rates, color='lightgreen')
    plt.title("SQL Component Matching")
    plt.ylabel("Match Rate")
    plt.ylim(0, 1)
    for i, v in enumerate(rates):
        plt.text(i, v + 0.02, f"{v:.2f}", ha='center')
    
    # Plot 3: Precision, Recall, F1 Score
    plt.subplot(2, 2, 3)
    overall_metrics = eval_results.get("overall_metrics", {})
    prf_metrics = [
        overall_metrics.get("precision", 0),
        overall_metrics.get("recall", 0),
        overall_metrics.get("f1", 0)
    ]
    prf_labels = ["Precision", "Recall", "F1 Score"]
    plt.bar(prf_labels, prf_metrics, color='salmon')
    plt.title("Overall Component Metrics")
    plt.ylabel("Score")
    plt.ylim(0, 1)
    for i, v in enumerate(prf_metrics):
        plt.text(i, v + 0.02, f"{v:.2f}", ha='center')
    
    # Plot 4: Component-wise F1 Scores
    plt.subplot(2, 2, 4)
    component_f1s = {}
    
    # Collect F1 scores for each component type across all examples
    for detail in eval_results.get("details", []):
        comparison = detail.get("component_comparison", {})
        for comp_type, metrics in comparison.items():
            if comp_type not in component_f1s:
                component_f1s[comp_type] = []
            component_f1s[comp_type].append(metrics.get("f1", 0))
    
    # Calculate mean F1 for each component
    comp_names = []
    mean_f1s = []
    for comp, f1_scores in component_f1s.items():
        if f1_scores:
            comp_names.append(comp.upper())
            mean_f1s.append(np.mean(f1_scores))
    
    plt.bar(comp_names, mean_f1s, color='plum')
    plt.title("Component-wise F1 Scores")
    plt.ylabel("Mean F1 Score")
    plt.ylim(0, 1)
    for i, v in enumerate(mean_f1s):
        plt.text(i, v + 0.02, f"{v:.2f}", ha='center')
    
    plt.suptitle(title, fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()

def compare_systems(original_eval, enhanced_eval, title="System Comparison", save_path=None):
    """
    Create visualizations comparing two systems.
    
    Args:
        original_eval: Evaluation results for the original system
        enhanced_eval: Evaluation results for the enhanced system
        title: Title for the visualization
        save_path: Path to save the visualization, if None, it will be displayed interactively
        
    Returns:
        None
    """
    plt.figure(figsize=(14, 10))
    
    # Plot 1: Basic Metrics Comparison
    plt.subplot(2, 2, 1)
    labels = ["Execution Success", "Syntax Correctness", "Semantic Success"]
    original_metrics = [
        original_eval.get("execution_success_rate", 0),
        original_eval.get("syntax_correctness_rate", 0),
        original_eval.get("semantic_success_rate", 0)
    ]
    enhanced_metrics = [
        enhanced_eval.get("execution_success_rate", 0),
        enhanced_eval.get("syntax_correctness_rate", 0),
        enhanced_eval.get("semantic_success_rate", 0)
    ]
    
    # Add execution match rate if available
    if "execution_match_rate" in original_eval and "execution_match_rate" in enhanced_eval:
        labels.append("Execution Match")
        original_metrics.append(original_eval.get("execution_match_rate", 0))
        enhanced_metrics.append(enhanced_eval.get("execution_match_rate", 0))
    
    x = np.arange(len(labels))
    width = 0.35
    
    plt.bar(x - width/2, original_metrics, width, label='Original')
    plt.bar(x + width/2, enhanced_metrics, width, label='Enhanced')
    plt.title("Basic Metrics Comparison")
    plt.ylabel("Score")
    plt.ylim(0, 1)
    plt.xticks(x, labels)
    plt.legend()
    
    # Plot 2: F1, Precision, Recall Comparison
    plt.subplot(2, 2, 2)
    labels = ["Precision", "Recall", "F1 Score"]
    original_prf = [
        original_eval.get("overall_metrics", {}).get("precision", 0),
        original_eval.get("overall_metrics", {}).get("recall", 0),
        original_eval.get("overall_metrics", {}).get("f1", 0)
    ]
    enhanced_prf = [
        enhanced_eval.get("overall_metrics", {}).get("precision", 0),
        enhanced_eval.get("overall_metrics", {}).get("recall", 0),
        enhanced_eval.get("overall_metrics", {}).get("f1", 0)
    ]
    
    x = np.arange(len(labels))
    
    plt.bar(x - width/2, original_prf, width, label='Original')
    plt.bar(x + width/2, enhanced_prf, width, label='Enhanced')
    plt.title("PRF Metrics Comparison")
    plt.ylabel("Score")
    plt.ylim(0, 1)
    plt.xticks(x, labels)
    plt.legend()
    
    # Plot 3: Component Match Rate Comparison
    plt.subplot(2, 2, 3)
    
    # Collect component match rates
    components = set()
    for system_eval in [original_eval, enhanced_eval]:
        for comp in system_eval.get("component_matches", {}).keys():
            components.add(comp)
    components = sorted(list(components))
    
    original_rates = []
    enhanced_rates = []
    
    for comp in components:
        original_rates.append(original_eval.get("component_matches", {}).get(comp, {}).get("rate", 0))
        enhanced_rates.append(enhanced_eval.get("component_matches", {}).get(comp, {}).get("rate", 0))
    
    x = np.arange(len(components))
    
    plt.bar(x - width/2, original_rates, width, label='Original')
    plt.bar(x + width/2, enhanced_rates, width, label='Enhanced')
    plt.title("Component Match Rate Comparison")
    plt.ylabel("Match Rate")
    plt.ylim(0, 1)
    plt.xticks(x, [c.upper() for c in components])
    plt.legend()
    
    # Plot 4: Total Success Count
    plt.subplot(2, 2, 4)
    
    orig_success = original_eval.get("execution_success", 0)
    enhanced_success = enhanced_eval.get("execution_success", 0)
    orig_total = original_eval.get("total", 0)
    enhanced_total = enhanced_eval.get("total", 0)
    
    success_data = [orig_success, enhanced_success]
    total_data = [orig_total, enhanced_total]
    
    x = np.arange(2)
    
    plt.bar(x, total_data, width, label='Total')
    plt.bar(x, success_data, width, label='Success', color='lightgreen')
    plt.title("Execution Success Count")
    plt.ylabel("Count")
    plt.xticks(x, ['Original', 'Enhanced'])
    plt.legend()
    
    plt.suptitle(title, fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()

if __name__ == "__main__":
    # Example usage
    pred_sql = """
    SELECT c.customer_name, COUNT(o.order_id) as order_count
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= '2022-01-01'
    GROUP BY c.customer_name
    ORDER BY order_count DESC
    LIMIT 10
    """
    
    gold_sql = """
    SELECT customers.customer_name, COUNT(orders.order_id) as total_orders
    FROM customers
    LEFT JOIN orders ON customers.customer_id = orders.customer_id
    WHERE orders.order_date > '2022-01-01'
    GROUP BY customers.customer_name
    ORDER BY total_orders DESC
    LIMIT 10
    """
    
    # Test component matching
    similarity = calculate_sql_similarity(pred_sql, gold_sql)
    
    # Print results
    print("Component Metrics:")
    for component, metrics in similarity['component_metrics'].items():
        print(f"  {component}: F1={metrics['f1']:.2f}, Precision={metrics['precision']:.2f}, Recall={metrics['recall']:.2f}")
        print(f"    Gold: {metrics['gold']}")
        print(f"    Pred: {metrics['pred']}")
    
    print("\nOverall Metrics:")
    for metric, value in similarity['overall_metrics'].items():
        print(f"  {metric}: {value:.2f}")
