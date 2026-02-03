import os
import asyncio
import asyncpg

# --- CONFIGURATION ---
DATABASE_URL = os.environ.get("DATABASE_URL")

async def main():
    print("🔌 Connecting to Database...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 1. Get List of ALL Tables
    print("🔍 Scanning for old tables...")
    tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    
    if not tables:
        print("✅ Database is already empty.")
    else:
        # 2. Drop Every Table
        for t in tables:
            name = t['tablename']
            print(f"🗑 Deleting table: {name}...")
            # CASCADE ensures linked data is also deleted
            await conn.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')
    
    print("✨ DONE! Database is completely clean.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
