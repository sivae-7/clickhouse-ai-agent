import asyncio
from agno.agent import Agent
from agno.models.ollama import Ollama
from clickhouse_driver import Client

# Database client (sync)
def run_query(sql: str):
    client = Client(
        host="localhost",
        port=9000,
        user="default",
        password="password",
        database="testAI"
    )
    return client.execute(sql)

# Use LLM to generate SQL from user question
async def generate_sql(user_question: str) -> str:
    prompt = f"""
You are an expert SQL generator for the ClickHouse database with the following table schema:

Table: repo_events_per_day
The table `repo_events_per_day` has the following columns:
- repo_name (String): name of the repository/project
- event_type (String): type of event, e.g. 'WatchEvent'
- created_at (Date,): date of the event
- count (UInt32): the count 

Generate a valid ClickHouse SQL query that answers this question:
{user_question}

Return ONLY the SQL query without explanation.
"""
    agent = Agent(model=Ollama("llama3"), markdown=False)
    # get the SQL query text from the LLM's response
    sql_res = await agent.arun(prompt)
    sql = sql_res.content.strip().strip("```").strip()
    print("=====")
    return sql

# Use LLM to explain query results to the user
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
