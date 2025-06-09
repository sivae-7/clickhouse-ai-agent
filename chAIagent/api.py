from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from agent import generate_sql, explain_result
from schema import run_query
from db import init_db, save_message
import csv
from fastapi.responses import FileResponse


app = FastAPI(title="Analytics Assistant")

class QuestionRequest(BaseModel):
    question: str

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.post("/ask")
async def ask_question(data: QuestionRequest):
    user_question = data.question

    try:
        await save_message("User", user_question)
        sql = await generate_sql(user_question)
        rows = await asyncio.to_thread(run_query, sql)

        if not rows:
            return {"sql": sql, "result": [], "explanation": "No data found."}

        explanation = await explain_result(user_question, rows)
        await save_message("System", explanation)

        return {"sql": sql, "result": rows, "explanation": explanation}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export")
async def export_to_csv(data: QuestionRequest):
    user_question = data.question
    sql = await generate_sql(user_question)
    rows = await asyncio.to_thread(run_query, sql)

    if not rows:
        raise HTTPException(status_code=404, detail="No data")

    file_path = "result.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)

    return FileResponse(file_path, media_type="text/csv", filename="result.csv")
