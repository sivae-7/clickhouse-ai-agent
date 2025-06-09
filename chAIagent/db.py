# db.py
import aiosqlite
from config import MEMORY_DB

async def init_db():
    async with aiosqlite.connect(MEMORY_DB) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS conversation (
                role TEXT,
                message TEXT
            )
        """)
        await db.commit()

async def save_message(role: str, message: str):
    async with aiosqlite.connect(MEMORY_DB) as db:
        await db.execute("INSERT INTO conversation (role, message) VALUES (?, ?)", (role, message))
        await db.commit()

async def get_conversation():
    async with aiosqlite.connect(MEMORY_DB) as db:
        async with db.execute("SELECT role, message FROM conversation") as cursor:
            return await cursor.fetchall()
