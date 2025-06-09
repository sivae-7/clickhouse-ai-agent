import asyncio
from agno.agent import Agent
from agno.models.ollama import Ollama
from clickhouse_driver import Client

# 1. Fetch dynamic DB schema
def get_schema_description():
    client = Client(
        host="localhost",
        port=9000,
        user="default",
        password="password",
        database="testAI"
    )
    tables = client.execute("SHOW TABLES")
    schema = ""

    for (table_name,) in tables:
        schema += f"\nTable: {table_name}\n"
        desc = client.execute(f"DESCRIBE TABLE {table_name}")
        for name, type_, *_ in desc:
            schema += f"- {name} ({type_})\n"
    return schema

# 2. Run SQL query
def run_query(sql: str):
    client = Client(
        host="localhost",
        port=9000,
        user="default",
        password="password",
        database="testAI"
    )
    return client.execute(sql)

# 3. Generate SQL with schema context
async def generate_sql(user_question: str) -> str:
    schema_description = get_schema_description()

    prompt = f"""
You are an expert SQL generator for ClickHouse.

Here is the database schema:
{schema_description}

Generate a valid ClickHouse SQL query that answers this question:
{user_question}

Only return the SQL query without any explanation.
"""
    agent = Agent(model=Ollama("llama3"), markdown=False)
    sql_res = await agent.arun(prompt)
    sql = sql_res.content.strip().strip("```").strip()
    if sql.lower().startswith("sql"):
        sql = sql[3:].strip()
    return sql

# 4. Explain result
async def explain_result(user_question: str, query_result) -> str:
    prompt = f"""
You executed this SQL query to answer the question:
"{user_question}"

SQL result:
{query_result}

Explain the result in a clear and concise way for a user.
"""
    agent = Agent(model=Ollama("llama3"), markdown=True)
    explanation_response = await agent.arun(prompt)
    explanation = explanation_response.content.strip().strip("```").strip()
    return explanation

# 5. Orchestrator
async def main():
    user_question = input("Ask me an event analytics question: ")

    print("Generating SQL query...")
    sql = await generate_sql(user_question)
    
    print(f"Generated SQL:\n{sql}\n")

    print("Running query on ClickHouse...")
    rows = await asyncio.to_thread(run_query, sql)

    if not rows:
        print("No data found for your question.")
        return

    print("Generating explanation...")
    explanation = await explain_result(user_question, rows)
    print("\nAnswer:\n", explanation)

if __name__ == "__main__":
    asyncio.run(main())
