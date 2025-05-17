"""
Schema Processing Module for Enhanced Snowflake NL2SQL

This module provides advanced schema processing capabilities inspired by the Spider2 agent architecture.
It enhances schema understanding, relationship extraction, and query planning for complex Snowflake schemas.
"""

import json
import pandas as pd
import re
from typing import Dict, List, Optional, Tuple, Any
import logging
import os

# Set up logging
logger = logging.getLogger("snowflake_nl2sql")
logger.setLevel(logging.INFO)

class SchemaProcessor:
    """
    Enhanced schema processor for Snowflake databases.
    Inspired by Spider2's approach to schema understanding and relationship extraction.
    """
    
    def __init__(self, connection=None):
        """
        Initialize the schema processor.
        
        Args:
            connection: Snowflake connection object (optional, can be added later)
        """
        self.connection = connection
        self.tables = {}
        self.columns = {}
        self.relationships = {}
        self.primary_keys = {}
        self.foreign_keys = {}
        self.db_schema = {}
        self.sample_data = {}
        
    def connect(self, connection):
        """Set or update the Snowflake connection"""
        self.connection = connection
        
    def extract_schema_from_connection(self, database_name: str, schema_name: str) -> Dict[str, Any]:
        """
        Extract schema information directly from Snowflake using the connection.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema within the database
            
        Returns:
            Dict containing structured schema information
        """
        if not self.connection:
            raise ValueError("No active connection to Snowflake.")
            
        cursor = self.connection.cursor()
        
        # Extract tables
        cursor.execute(f"SHOW TABLES IN {database_name}.{schema_name}")
        tables_result = cursor.fetchall()
        tables_df = pd.DataFrame(tables_result, columns=[desc[0] for desc in cursor.description])
        
        schema_info = {
            "database_name": database_name,
            "schema_name": schema_name,
            "tables": {}
        }
        
        # For each table, extract columns and other metadata
        for _, table_row in tables_df.iterrows():
            table_name = table_row['name']
            
            # Get column information
            cursor.execute(f"DESCRIBE TABLE {database_name}.{schema_name}.{table_name}")
            columns_result = cursor.fetchall()
            columns_df = pd.DataFrame(columns_result, columns=[desc[0] for desc in cursor.description])
            
            # Get primary keys
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_CATALOG = '{database_name}' 
                AND TABLE_SCHEMA = '{schema_name}' 
                AND TABLE_NAME = '{table_name}' 
                AND CONSTRAINT_NAME LIKE 'PK%'
            """)
            pk_result = cursor.fetchall()
            primary_keys = [row[0] for row in pk_result] if pk_result else []
            
            # Get foreign keys
            cursor.execute(f"""
                SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME, ccu.TABLE_SCHEMA, ccu.TABLE_NAME, ccu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                AND tc.TABLE_CATALOG = '{database_name}'
                AND tc.TABLE_SCHEMA = '{schema_name}'
                AND tc.TABLE_NAME = '{table_name}'
            """)
            fk_result = cursor.fetchall()
            foreign_keys = []
            for fk in fk_result:
                foreign_keys.append({
                    "column_name": fk[1],
                    "references_schema": fk[2],
                    "references_table": fk[3],
                    "references_column": fk[4]
                })
            
            # Sample data (limit to 5 rows)
            try:
                cursor.execute(f"SELECT * FROM {database_name}.{schema_name}.{table_name} LIMIT 5")
                sample_result = cursor.fetchall()
                sample_df = pd.DataFrame(sample_result, columns=[desc[0] for desc in cursor.description])
                sample_data = sample_df.to_dict(orient='records') if not sample_df.empty else []
            except Exception as e:
                logger.warning(f"Could not fetch sample data for {table_name}: {e}")
                sample_data = []
                
            # Build table info
            table_info = {
                "name": table_name,
                "columns": columns_df.to_dict(orient='records'),
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "sample_data": sample_data
            }
            
            schema_info["tables"][table_name] = table_info
            
        self.db_schema = schema_info
        return schema_info
        
    def extract_schema_from_description(self, schema_description: str) -> Dict[str, Any]:
        """
        Extract schema information from a text description.
        This is useful when we don't have direct access to the database.
        
        Args:
            schema_description: Text description of the schema
            
        Returns:
            Dict containing structured schema information
        """
        schema_info = {
            "tables": {}
        }
        
        current_table = None
        
        # Regular expressions for parsing
        table_pattern = re.compile(r'Table:\s+(\w+)')
        column_pattern = re.compile(r'(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)')
        
        for line in schema_description.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            # Check if this is a table definition
            table_match = table_pattern.match(line)
            if table_match:
                current_table = table_match.group(1)
                schema_info["tables"][current_table] = {
                    "name": current_table,
                    "columns": [],
                    "primary_keys": [],
                    "foreign_keys": []
                }
                continue
                
            # If we have a current table, try to parse column info
            if current_table:
                column_match = column_pattern.match(line)
                if column_match:
                    column_name, column_type = column_match.groups()
                    schema_info["tables"][current_table]["columns"].append({
                        "name": column_name,
                        "type": column_type
                    })
        
        self.db_schema = schema_info
        return schema_info
    
    def extract_schema_from_json(self, schema_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract schema information from a JSON structure.
        
        Args:
            schema_json: JSON structure containing schema information
            
        Returns:
            Dict containing structured schema information
        """
        self.db_schema = schema_json
        return schema_json
    
    def infer_relationships(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Infer relationships between tables in the schema.
        Uses foreign keys and naming conventions to identify relationships.
        
        Returns:
            Dict mapping table names to lists of related tables with relationship types
        """
        relationships = {}
        
        # Process direct foreign key relationships
        for table_name, table_info in self.db_schema.get("tables", {}).items():
            relationships[table_name] = []
            
            # Add foreign key relationships
            for fk in table_info.get("foreign_keys", []):
                ref_table = fk.get("references_table")
                if ref_table:
                    relationships[table_name].append({
                        "related_table": ref_table,
                        "type": "many_to_one",
                        "join_column": fk.get("column_name"),
                        "referenced_column": fk.get("references_column")
                    })
            
            # Infer additional relationships based on naming conventions
            for col in table_info.get("columns", []):
                col_name = col.get("name", "").lower()
                # Look for columns ending with _id that might be foreign keys
                if col_name.endswith('_id') and not any(fk.get("column_name") == col_name for fk in table_info.get("foreign_keys", [])):
                    potential_table = col_name[:-3]  # Remove _id suffix
                    if potential_table in self.db_schema.get("tables", {}):
                        relationships[table_name].append({
                            "related_table": potential_table,
                            "type": "potential_foreign_key",
                            "join_column": col_name,
                            "referenced_column": "id"  # Assuming the referenced column is 'id'
                        })
        
        self.relationships = relationships
        return relationships
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get all columns for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column information dictionaries
        """
        if table_name in self.db_schema.get("tables", {}):
            return self.db_schema["tables"][table_name].get("columns", [])
        return []
    
    def format_schema_for_prompt(self, include_sample_data: bool = False) -> str:
        """
        Format the schema information into a text representation suitable for prompts.
        
        Args:
            include_sample_data: Whether to include sample data in the output
            
        Returns:
            Formatted schema string for inclusion in prompts
        """
        schema_text = "# DATABASE SCHEMA INFORMATION\n\n"
        
        # Add database and schema names if available
        if "database_name" in self.db_schema and "schema_name" in self.db_schema:
            schema_text += f"Database: {self.db_schema['database_name']}\n"
            schema_text += f"Schema: {self.db_schema['schema_name']}\n\n"
        
        # Add tables and their columns
        for table_name, table_info in self.db_schema.get("tables", {}).items():
            schema_text += f"## Table: {table_name}\n\n"
            
            # Add columns
            schema_text += "Columns:\n"
            for col in table_info.get("columns", []):
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                is_pk = col_name in table_info.get("primary_keys", [])
                pk_indicator = " (Primary Key)" if is_pk else ""
                schema_text += f"- {col_name} ({col_type}){pk_indicator}\n"
            
            # Add foreign keys
            if table_info.get("foreign_keys"):
                schema_text += "\nForeign Keys:\n"
                for fk in table_info.get("foreign_keys", []):
                    schema_text += f"- {fk.get('column_name')} references {fk.get('references_table')}.{fk.get('references_column')}\n"
            
            # Add sample data if requested
            if include_sample_data and table_info.get("sample_data"):
                schema_text += "\nSample Data:\n"
                # Format sample data in a tabular structure
                sample_df = pd.DataFrame(table_info.get("sample_data", []))
                if not sample_df.empty:
                    schema_text += sample_df.head(3).to_string(index=False) + "\n"
            
            schema_text += "\n"
        
        # Add relationships
        if self.relationships:
            schema_text += "## Table Relationships\n\n"
            for table, related_tables in self.relationships.items():
                if related_tables:
                    schema_text += f"Table '{table}' is related to:\n"
                    for rel in related_tables:
                        schema_text += f"- Table '{rel['related_table']}' via {rel['join_column']} -> {rel['referenced_column']} ({rel['type']})\n"
                    schema_text += "\n"
        
        return schema_text
        
    def suggest_join_paths(self, source_table: str, target_table: str) -> List[Dict[str, Any]]:
        """
        Suggest possible join paths between two tables.
        
        Args:
            source_table: Starting table name
            target_table: Target table name
            
        Returns:
            List of possible join paths with detailed information
        """
        # BFS to find all paths
        visited = set()
        queue = [(source_table, [])]
        paths = []
        
        while queue:
            current_table, path = queue.pop(0)
            
            # Skip if already visited
            if current_table in visited:
                continue
                
            visited.add(current_table)
            
            # Check if we reached the target
            if current_table == target_table:
                paths.append(path)
                continue
            
            # Add neighbors to queue
            if current_table in self.relationships:
                for rel in self.relationships[current_table]:
                    related_table = rel["related_table"]
                    new_path = path + [rel]
                    queue.append((related_table, new_path))
        
        # Format the results
        formatted_paths = []
        for path in paths:
            joins = []
            prev_table = source_table
            
            for rel in path:
                joins.append({
                    "left_table": prev_table,
                    "right_table": rel["related_table"],
                    "left_column": rel["join_column"],
                    "right_column": rel["referenced_column"],
                    "join_type": "INNER JOIN"  # Default, could be enhanced
                })
                prev_table = rel["related_table"]
            
            formatted_paths.append({
                "path_length": len(path),
                "joins": joins
            })
        
        # Sort by path length
        formatted_paths.sort(key=lambda x: x["path_length"])
        return formatted_paths
    
    def format_join_path_for_query(self, join_path: Dict[str, Any]) -> str:
        """
        Format a join path into SQL query fragments.
        
        Args:
            join_path: Join path information
            
        Returns:
            SQL query fragment for JOINs
        """
        sql = ""
        for join in join_path.get("joins", []):
            sql += f"{join['join_type']} {join['right_table']} ON {join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}\n"
        return sql
    
    def analyze_query_complexity(self, question: str) -> Dict[str, Any]:
        """
        Analyze the complexity of a natural language question and provide query planning insights.
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with query complexity analysis
        """
        analysis = {
            "complexity": "simple",  # simple, medium, complex
            "likely_tables": [],
            "likely_joins_needed": False,
            "likely_aggregations": False,
            "likely_filters": [],
            "suggested_approach": ""
        }
        
        # Check for indicators of complex queries
        table_names = [table for table in self.db_schema.get("tables", {})]
        
        # Look for table names in the question
        for table in table_names:
            table_lower = table.lower()
            if table_lower in question.lower():
                analysis["likely_tables"].append(table)
        
        # Check for join indicators
        join_indicators = ["between", "related", "connection", "join", "across"]
        if any(indicator in question.lower() for indicator in join_indicators):
            analysis["likely_joins_needed"] = True
            analysis["complexity"] = "medium" if analysis["complexity"] == "simple" else analysis["complexity"]
        
        # Check for aggregation indicators
        agg_indicators = ["average", "total", "sum", "count", "maximum", "minimum", "most", "least", "highest", "lowest"]
        if any(indicator in question.lower() for indicator in agg_indicators):
            analysis["likely_aggregations"] = True
            analysis["complexity"] = "medium" if analysis["complexity"] == "simple" else analysis["complexity"]
        
        # Check for complex query indicators
        complex_indicators = ["for each", "group by", "having", "more than", "less than", "before", "after", "between"]
        if any(indicator in question.lower() for indicator in complex_indicators):
            analysis["complexity"] = "complex"
        
        # Generate a suggested approach based on the analysis
        if analysis["complexity"] == "simple":
            analysis["suggested_approach"] = "Simple SELECT query with filters."
        elif analysis["complexity"] == "medium":
            if analysis["likely_aggregations"]:
                analysis["suggested_approach"] = "Use aggregation functions with possible GROUP BY."
            elif analysis["likely_joins_needed"]:
                analysis["suggested_approach"] = "Join tables with appropriate relationships."
        else:
            analysis["suggested_approach"] = "Complex query with joins, aggregations, and filters."
        
        return analysis

# Example usage
if __name__ == "__main__":
    processor = SchemaProcessor()
    schema_info = processor.extract_schema_from_description("Schema description here...")
    processor.infer_relationships()
    formatted_schema = processor.format_schema_for_prompt()
    print(formatted_schema)
