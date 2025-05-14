# Natural Language to SQL for Snowflake

This project demonstrates how to build a conversational AI agent that converts natural language questions into SQL queries for Snowflake databases using LangChain, GROQ (LLaMA models), and Snowflake connector.

## Project Overview

The system allows users to:
- Ask questions in natural language about data stored in Snowflake
- Get SQL queries generated automatically from their questions
- Receive natural language answers based on query results
- Handle cases where questions are not relevant to the database

The agent uses a directed graph workflow to process user questions through multiple specialized components:
1. Relevance checking
2. Natural language to SQL conversion
3. SQL execution
4. Answer generation
5. Question reformulation (if needed)

## Files in this Project

- `Snowflake_Natural_Language_To_SQL.ipynb` - Main notebook with the NL-to-SQL agent implementation
- `Snowflake_EDA.ipynb` - Exploratory data analysis for Snowflake databases
- `.env` - Environment variables including API keys (not tracked in git)
- `snowflake_credential.json` - Snowflake authentication credentials (not tracked in git)

## Prerequisites

### Required Accounts

1. **Snowflake Account**
   - You need a Snowflake account with access to a database
   - If you don't have one, sign up at [Snowflake](https://signup.snowflake.com/)
   - Basic/trial tier is sufficient for testing

2. **GROQ API Key**
   - Register at [GROQ's website](https://console.groq.com/) to get an API key
   - The project uses LLaMA models via GROQ's API

### Python Environment Setup

1. Create a virtual environment (recommended):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On macOS/Linux
   # or
   .venv\Scripts\activate  # On Windows
   ```

2. Install required packages:
   ```bash
   pip install -U langchain-community langchain-core langgraph langchain-groq snowflake-connector-python python-dotenv pandas
   ```

### Snowflake Configuration

1. Create a `snowflake_credential.json` file with the following format:
   ```json
   {
     "user": "your_username",
     "password": "your_password",
     "account": "your_account_identifier",
     "warehouse": "your_warehouse",
     "database": "your_database",
     "schema": "your_schema"
   }
   ```

2. Place this file in the project root directory or update the path in the notebook

### GROQ API Configuration

1. Create a `.env` file in the project root with your GROQ API key:
   ```
   GROQ_API_KEY=your_groq_api_key_here
   ```

## Usage

1. Start Jupyter Notebook or VS Code with Jupyter extension:
   ```bash
   jupyter notebook
   # or with VS Code
   code .
   ```

2. Open and run the `Snowflake_Natural_Language_To_SQL.ipynb` notebook

3. The notebook will:
   - Connect to your Snowflake account
   - Load database schema information
   - Initialize the LLM-based agent
   - Display a conversational interface for asking questions

4. Ask questions in natural language about your Snowflake data, for example:
   - "How many customers do we have in each country?"
   - "What are the top 5 selling products last month?"
   - "Show me department names with more than 10 employees"

5. Type "exit" or "quit" to end the conversation

## Customizing for Your Database

The notebook is set up to work with the AdventureWorks sample database, but can be adapted to any Snowflake database:

1. Update the database selection code to point to your database
2. The system will automatically extract schema information
3. You may need to update the prompts based on your specific database domain

## Troubleshooting

Common issues and solutions:

- **Connection Issues**: Verify your Snowflake credentials and network connectivity
- **Case Sensitivity Errors**: Snowflake is case-sensitive; check table and column names
- **API Rate Limits**: If using GROQ's free tier, be aware of API call limitations
- **Memory Issues**: Large tables may require limiting the data returned

## Spider2 Integration (Optional)

This project can optionally integrate with the Spider2 database benchmarking suite:

1. Clone the Spider2 repository:
   ```bash
   git clone https://github.com/Yale-LILY/spider2
   ```

2. Set up Spider2 according to its documentation for additional evaluation capabilities

## Resources

- [Snowflake Documentation](https://docs.snowflake.com)
- [LangChain Documentation](https://python.langchain.com/docs/get_started/introduction)
- [GROQ API Documentation](https://console.groq.com/docs/quickstart)
- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)

## License

This project is available under the MIT License.
