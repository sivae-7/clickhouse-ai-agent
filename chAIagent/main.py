# main.py
import asyncio
from agent import generate_sql, explain_result
from schema import run_query
from db import init_db, save_message

async def main():
    await init_db()
    
    while True:
        user_question = input("\nAsk me an event analytics question (or type 'exit'): ")
        if user_question.lower() in {"exit", "quit"}:
            break

        await save_message("User", user_question)

        print("\nGenerating SQL query...")
        sql = await generate_sql(user_question)
        print(f"\nGenerated SQL:\n{sql}")

        try:
            print("\nRunning query on ClickHouse...")
            rows = await asyncio.to_thread(run_query, sql)
        except Exception as e:
            print(f"Error running query: {e}")
            continue

        if not rows:
            print("No data found.")
            continue

        print("\nExplaining result...")
        explanation = await explain_result(user_question, rows)
        print("\n📊 Answer:\n", explanation)

        await save_message("System", explanation)

if __name__ == "__main__":
    asyncio.run(main())
