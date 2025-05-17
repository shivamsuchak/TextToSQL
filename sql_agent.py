"""
SQL Agent Module for Enhanced Snowflake NL2SQL

This module implements an agent-based architecture for SQL generation,
inspired by Spider2's approach to complex query planning and execution.
"""

import json
import logging
import re
import time
from typing import Dict, List, Optional, Any, Tuple
from pydantic import BaseModel, Field

# Set up logging
logger = logging.getLogger("sql_agent")
logger.setLevel(logging.INFO)

class SQLAgentState(BaseModel):
    """State representation for the SQL Agent"""
    question: str = Field(description="The original natural language question")
    schema_info: Dict[str, Any] = Field(description="Database schema information")
    plan: List[str] = Field(default_factory=list, description="Plan steps for query generation")
    current_sql: str = Field(default="", description="Current SQL query being constructed")
    thoughts: List[str] = Field(default_factory=list, description="Agent's thoughts during query generation")
    attempts: int = Field(default=0, description="Number of query generation attempts")
    error_messages: List[str] = Field(default_factory=list, description="Error messages from failed attempts")
    final_sql: str = Field(default="", description="Final SQL query")
    is_complete: bool = Field(default=False, description="Whether the SQL generation is complete")
    is_successful: bool = Field(default=False, description="Whether the query generation is successful")
    
class SQLGenerationAgent:
    """
    SQL Generation Agent inspired by Spider2 architecture.
    
    This agent follows a multi-step process:
    1. Analyze the question and schema
    2. Plan the SQL query components
    3. Generate the SQL query
    4. Validate and refine the query
    5. Execute and verify results
    """
    
    def __init__(self, llm_client, schema_processor):
        """
        Initialize the SQL Generation Agent.
        
        Args:
            llm_client: LLM client for generating queries
            schema_processor: Schema processor for schema understanding
        """
        self.llm = llm_client
        self.schema_processor = schema_processor
        self.state = None
        
    def initialize_state(self, question: str) -> SQLAgentState:
        """
        Initialize the agent state with the question and schema information.
        
        Args:
            question: Natural language question
            
        Returns:
            Initialized agent state
        """
        schema_info = self.schema_processor.db_schema
        self.state = SQLAgentState(
            question=question,
            schema_info=schema_info
        )
        return self.state
        
    def analyze_question(self) -> Dict[str, Any]:
        """
        Analyze the natural language question to understand intent and identify
        relevant schema elements.
        
        Returns:
            Dictionary with analysis results
        """
        logger.info(f"Analyzing question: {self.state.question}")
        
        # Start with schema processor's basic analysis
        analysis = self.schema_processor.analyze_query_complexity(self.state.question)
        
        # Enhance with LLM-based analysis
        prompt = f"""
        Analyze the following question in the context of SQL query generation:
        
        Question: {self.state.question}
        
        Please identify:
        1. What tables are likely needed to answer this question?
        2. What columns are being asked about?
        3. What filters or conditions need to be applied?
        4. Are any aggregations (COUNT, SUM, AVG, etc.) needed?
        5. Is data ordering or sorting required?
        6. Are there any temporal conditions (dates, times)?
        7. What is the expected result (a single value, a list, etc.)?
        8. Columns name should be in double quotes and lower case letters.
        
        Format your response as JSON with the following structure:
        {{{{
            "tables": ["table1", "table2"],
            "columns": ["column1", "column2"],
            "filters": [{{"column": "column_name", "condition": "equals", "value": "some_value"}}],
            "aggregations": ["count", "sum"],
            "ordering": {{"column": "column_name", "direction": "ASC"}},
            "temporal_conditions": [{{"column": "date_column", "condition": "after", "value": "2020-01-01"}}],
            "expected_result": "single_value"
        }}}}
        """
        
        try:
            response = self.llm.invoke(prompt)
            # Extract JSON from the response
            json_match = re.search(r'```json\n(.*?)\n```', response.content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_str = response.content.strip()
                
            # Clean up the JSON string to handle potential issues
            json_str = re.sub(r'```.*?```', '', json_str, flags=re.DOTALL)
            json_str = json_str.strip()
            
            llm_analysis = json.loads(json_str)
            
            # Combine the analyses
            analysis.update(llm_analysis)
            
            # Add the analysis to the state
            self.state.thoughts.append(f"Question Analysis: {json.dumps(analysis, indent=2)}")
            
            return analysis
        except Exception as e:
            logger.error(f"Error during question analysis: {e}")
            self.state.thoughts.append(f"Error during question analysis: {e}")
            return analysis
    
    def create_query_plan(self, analysis: Dict[str, Any]) -> List[str]:
        """
        Create a step-by-step plan for SQL query generation based on the analysis.
        
        Args:
            analysis: Question analysis results
            
        Returns:
            List of plan steps
        """
        logger.info("Creating query plan")
        
        # Default basic plan
        basic_plan = [
            "Identify the main table",
            "Determine SELECT columns",
            "Add WHERE clause filters",
            "Complete the SQL query"
        ]
        
        # Use LLM to generate a more detailed plan
        prompt = f"""
        Based on the following question and analysis, create a step-by-step plan for generating an SQL query.
        
        Question: {self.state.question}
        
        Analysis: {json.dumps(analysis, indent=2)}
        
        Please provide a detailed plan with specific steps for constructing the SQL query.
        Format each step as a bullet point, focusing on:
        1. Table selection and potential joins
        2. Column selection for the SELECT clause
        3. Filter conditions for the WHERE clause
        4. Grouping and aggregation if needed
        5. Ordering/sorting if needed
        6. Any other SQL components required
        
        Format your response as a list of steps like:
        1. Identify main tables X and Y
        2. Join tables using the Z relationship
        3. etc.
        """
        
        try:
            response = self.llm.invoke(prompt)
            content = response.content
            
            # Extract steps (numbered or bullet points)
            steps = re.findall(r'(?:^\d+\.|\*)\s*(.*?)(?=^\d+\.|\*|$)', content, re.MULTILINE | re.DOTALL)
            
            if not steps:
                # Try another pattern if the first one didn't work
                steps = re.findall(r'(?:^\d+\.|\-)\s*(.*?)$', content, re.MULTILINE)
            
            if not steps:
                # If we still can't find steps, just split by newlines and filter
                lines = content.split('\n')
                steps = [line.strip() for line in lines if line.strip() and not line.strip().startswith('#')]
            
            # Clean up the steps
            plan = [step.strip() for step in steps if step.strip()]
            
            if not plan:
                plan = basic_plan
                
            # Add the plan to the state
            self.state.plan = plan
            self.state.thoughts.append(f"Query Plan: {json.dumps(plan, indent=2)}")
            
            return plan
        except Exception as e:
            logger.error(f"Error creating query plan: {e}")
            self.state.thoughts.append(f"Error creating query plan: {e}")
            self.state.plan = basic_plan
            return basic_plan
    
    def generate_sql(self) -> str:
        """
        Generate the SQL query based on the question, schema, and plan.
        
        Returns:
            Generated SQL query
        """
        logger.info("Generating SQL query")
        
        # Format the schema information
        schema_info = self.schema_processor.format_schema_for_prompt(include_sample_data=False)
        
        # Use the plan in the prompt
        plan_text = "\n".join([f"{i+1}. {step}" for i, step in enumerate(self.state.plan)])
        
        prompt = f"""
        Generate a valid SQL query for Snowflake based on the following:
        
        Question: {self.state.question}
        
        Database Schema:
        {schema_info}
        
        Query Plan:
        {plan_text}
        
        Important Guidelines:
        1. Use only tables and columns that exist in the provided schema.
        2. CRITICAL: ALWAYS quote identifiers (column) with double quotes in Snowflake (e.g., "PRODUCTID", "PRODUCT").
        3. Snowflake identifiers are case-sensitive, and the default is UPPERCASE, so use "productid" not "PRODUCTID" or "ProductID".
        4. Every identifier in the query MUST be enclosed in double quotes to avoid case sensitivity issues, like: SELECT "productid" FROM "PRODUCT".
        5. Ensure proper JOIN conditions when relating multiple tables.
        6. Use appropriate aggregation functions when needed.
        7. Format the query clearly with proper indentation.
        8. Write only the SQL query in your response, without explanations or comments.
        
        SQL Query:
        """
        
        try:
            response = self.llm.invoke(prompt)
            content = response.content
            
            # Extract just the SQL query
            sql_match = re.search(r'```sql\n(.*?)\n```', content, re.DOTALL)
            if sql_match:
                sql_query = sql_match.group(1)
            else:
                # If no SQL code block found, clean up the content to get just the SQL
                sql_query = content.strip()
                sql_query = re.sub(r'^SQL Query:\s*', '', sql_query, flags=re.MULTILINE | re.IGNORECASE)
                
            # Clean up the query
            sql_query = sql_query.strip()
            
            # Add the query to the state
            self.state.current_sql = sql_query
            self.state.thoughts.append(f"Generated SQL: {sql_query}")
            
            return sql_query
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            self.state.thoughts.append(f"Error generating SQL: {e}")
            return ""
    
    def validate_sql(self, sql_query: str) -> Tuple[bool, str]:
        """
        Validate the generated SQL query for syntax and schema correctness.
        
        Args:
            sql_query: Generated SQL query
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        logger.info("Validating SQL query")
        
        if not sql_query:
            return False, "Empty SQL query"
            
        # Use LLM to perform validation
        schema_info = self.schema_processor.format_schema_for_prompt(include_sample_data=False)
        
        prompt = f"""
        Validate the following SQL query against the provided schema for Snowflake:
        
        SQL Query:
        ```sql
        {sql_query}
        ```
        
        Database Schema:
        {schema_info}
        
        Please check for:
        1. Syntax errors
        2. References to non-existent tables or columns
        3. Incorrect join conditions
        4. CRITICAL: Case sensitivity issues - All identifiers (table and column names) MUST be enclosed in double quotes
        5. Other potential problems
        
        Format your response as JSON:
        {{{{
            "is_valid": true/false,
            "errors": ["error1", "error2", ...],
            "suggestions": ["suggestion1", "suggestion2", ...]
        }}}}
        """
        
        try:
            response = self.llm.invoke(prompt)
            content = response.content
            
            # Extract JSON from the response
            json_match = re.search(r'```json\n(.*?)\n```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_str = content.strip()
                
            # Clean up the JSON string
            json_str = re.sub(r'```.*?```', '', json_str, flags=re.DOTALL)
            json_str = json_str.strip()
            
            validation_result = json.loads(json_str)
            
            is_valid = validation_result.get("is_valid", False)
            errors = validation_result.get("errors", [])
            suggestions = validation_result.get("suggestions", [])
            
            error_message = ""
            if not is_valid and errors:
                error_message = "Errors: " + "; ".join(errors)
                if suggestions:
                    error_message += " Suggestions: " + "; ".join(suggestions)
            
            # Add validation results to the state
            self.state.thoughts.append(f"Validation: {error_message if error_message else 'Valid'}")
            if not is_valid:
                self.state.error_messages.append(error_message)
            
            return is_valid, error_message
        except Exception as e:
            logger.error(f"Error during SQL validation: {e}")
            self.state.thoughts.append(f"Error during SQL validation: {e}")
            self.state.error_messages.append(str(e))
            return False, str(e)
    
    def refine_sql(self, sql_query: str, error_message: str) -> str:
        """
        Refine the SQL query based on validation errors.
        
        Args:
            sql_query: Original SQL query
            error_message: Error message from validation
            
        Returns:
            Refined SQL query
        """
        logger.info("Refining SQL query")
        
        schema_info = self.schema_processor.format_schema_for_prompt(include_sample_data=False)
        
        prompt = f"""
        Refine the following SQL query based on the validation errors:
        
        Original SQL Query:
        ```sql
        {sql_query}
        ```
        
        Validation Errors:
        {error_message}
        
        Database Schema:
        {schema_info}
        
        Please correct the errors and provide a revised SQL query. 
        
        CRITICAL: ALWAYS put double quotes around ALL table and column names to handle Snowflake's case sensitivity.
        Example: SELECT "PRODUCTID" FROM "PRODUCT" WHERE "PRODUCTID" > 300
        
        Write only the corrected SQL query without explanations or comments.
        """
        
        try:
            response = self.llm.invoke(prompt)
            content = response.content
            
            # Extract just the SQL query
            sql_match = re.search(r'```sql\n(.*?)\n```', content, re.DOTALL)
            if sql_match:
                refined_sql = sql_match.group(1)
            else:
                refined_sql = content.strip()
                
            # Clean up the query
            refined_sql = refined_sql.strip()
            
            # Add the refined query to the state
            self.state.current_sql = refined_sql
            self.state.thoughts.append(f"Refined SQL: {refined_sql}")
            
            return refined_sql
        except Exception as e:
            logger.error(f"Error refining SQL: {e}")
            self.state.thoughts.append(f"Error refining SQL: {e}")
            return sql_query
    
    def run(self, question: str, max_attempts: int = 3) -> SQLAgentState:
        """
        Run the full SQL generation pipeline.
        
        Args:
            question: Natural language question
            max_attempts: Maximum number of refinement attempts
            
        Returns:
            Agent state with final SQL query
        """
        # Initialize state
        self.initialize_state(question)
        
        # Analyze the question
        analysis = self.analyze_question()
        
        # Create query plan
        plan = self.create_query_plan(analysis)
        
        # Initial SQL generation
        sql_query = self.generate_sql()
        self.state.attempts = 1
        
        # Validate and refine
        is_valid, error_message = self.validate_sql(sql_query)
        
        # Refinement loop
        while not is_valid and self.state.attempts < max_attempts:
            self.state.attempts += 1
            sql_query = self.refine_sql(sql_query, error_message)
            is_valid, error_message = self.validate_sql(sql_query)
        
        # Update final state
        self.state.final_sql = sql_query
        self.state.is_complete = True
        self.state.is_successful = is_valid
        
        if not is_valid:
            logger.warning(f"Failed to generate valid SQL after {max_attempts} attempts")
            
        return self.state
    
    def format_agent_trace(self) -> str:
        """
        Format the agent's thought process for debugging or transparency.
        
        Returns:
            Formatted trace of the agent's actions
        """
        if not self.state:
            return "No agent trace available"
            
        trace = [
            f"Question: {self.state.question}",
            "Thoughts:",
        ]
        
        for i, thought in enumerate(self.state.thoughts):
            trace.append(f"  {i+1}. {thought}")
            
        trace.append(f"Attempts: {self.state.attempts}")
        
        if self.state.error_messages:
            trace.append("Errors:")
            for i, error in enumerate(self.state.error_messages):
                trace.append(f"  {i+1}. {error}")
                
        trace.append(f"Final SQL: {self.state.final_sql}")
        trace.append(f"Success: {self.state.is_successful}")
        
        return "\n".join(trace)

# Example usage
if __name__ == "__main__":
    # This would be integrated with your actual LLM client and schema processor
    from schema_processor import SchemaProcessor
    
    class MockLLM:
        def invoke(self, prompt):
            return type('obj', (object,), {'content': 'SELECT * FROM table'})
    
    schema_processor = SchemaProcessor()
    llm_client = MockLLM()
    
    agent = SQLGenerationAgent(llm_client, schema_processor)
    result = agent.run("List all customers from New York")
    print(agent.format_agent_trace())
