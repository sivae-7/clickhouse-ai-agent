# agent.py
from agno.agent import Agent
from agno.models.ollama import Ollama
from schema import get_schema_description
from config import MODEL_NAME
from vector_store import add_question, find_similar

llm = Ollama(MODEL_NAME)

async def generate_sql(user_question: str) -> str:
    schema_description = get_schema_description()

    # Step 1: Check for similar past queries
    similar = find_similar(user_question)
    similar_section = ""
    if similar:
        similar_section = "Here are similar past questions and their SQL:\n"
        for q, s in similar:
            similar_section += f"- Q: {q}\n  SQL: {s}\n"

    # Step 2: Prompt with context
    prompt = f"""
You are an expert SQL generator for ClickHouse.

Here is the database schema:
{schema_description}

{similar_section}

Generate a valid ClickHouse SQL query that answers this question:
{user_question}

Only return the SQL query without any explanation.
"""

    agent = Agent(model=llm, markdown=False)
    sql_res = await agent.arun(prompt)

    # Step 3: Clean SQL and store
    sql = sql_res.content.strip().strip("```").strip()
    if sql.lower().startswith("sql"):
        sql = sql[3:].strip()

    # Step 4: Save this question + sql pair
    add_question(user_question, sql)

    return sql

async def explain_result(user_question: str, query_result) -> str:
    prompt = f"""
You executed this SQL query to answer the question:
"{user_question}"

SQL result:
{query_result}

Explain the result in a clear and concise way for a user.
"""
    agent = Agent(model=llm, markdown=True)
    explanation_response = await agent.arun(prompt)
    explanation = explanation_response.content.strip().strip("```").strip()
    return explanation
