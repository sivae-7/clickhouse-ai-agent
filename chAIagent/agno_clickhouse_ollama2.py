import asyncio
from agno.agent import Agent
from agno.models.ollama import Ollama
from clickhouse_driver import Client

# Memory to store conversation context
conversation_history = []

# 1. Get live ClickHouse schema
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
    return schema.strip()

# 2. Run a SQL query
def run_query(sql: str):
    client = Client(
        host="localhost",
        port=9000,
        user="default",
        password="password",
        database="testAI"
    )
    return client.execute(sql)

# 3. Generate SQL from question using LLM and schema
async def generate_sql(user_question: str, chat_context: str) -> str:
    schema_description = get_schema_description()

    prompt = f"""
You are an expert ClickHouse SQL generator.

Here is the current schema:
{schema_description}

Conversation so far:
{chat_context}

Now generate an appropriate SQL query for the new question:
{user_question}

Only return a valid ClickHouse SQL query, no explanation.
""".strip()

    agent = Agent(model=Ollama("llama3"), markdown=False)
    response = await agent.arun(prompt)
    sql = response.content.strip().strip("```").strip()
    if sql.lower().startswith("sql"):
        sql = sql[3:].strip()
    return sql

# 4. Explain the query results clearly
async def explain_result(user_question: str, query_result, chat_context: str) -> str:
    prompt = f"""
This was the conversation context:
{chat_context}

Then the user asked:
"{user_question}"

Here are the SQL query results:
{query_result}

Now explain the result in simple terms.
""".strip()

    agent = Agent(model=Ollama("llama3"), markdown=True)
    response = await agent.arun(prompt)
    return response.content.strip().strip("```").strip()

# 5. Continuous conversation loop
async def main():
    print("🔍 Ask your ClickHouse analytics questions. Type 'exit' to quit.\n")

    while True:
        user_question = input("🧠 You: ")
        if user_question.strip().lower() in {"exit", "quit"}:
            print("👋 Goodbye!")
            break

        # Update conversation context
        conversation_history.append(f"User: {user_question}")
        chat_context = "\n".join(conversation_history[-10:])  # keep last 10 turns

        try:
            print("🛠️ Generating SQL query...")
            sql = await generate_sql(user_question, chat_context)
            print(f"\n📝 Generated SQL:\n{sql}\n")

            print("📊 Running query...")
            rows = await asyncio.to_thread(run_query, sql)

            if not rows:
                print("⚠️ No data found.")
                continue

            print("💡 Explaining result...")
            explanation = await explain_result(user_question, rows, chat_context)
            print(f"\n🧾 Answer:\n{explanation}\n")

            # Append system output to memory
            conversation_history.append(f"System: {explanation}")

        except Exception as e:
            print("❌ Error:", e)
            conversation_history.append(f"System: Error - {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
