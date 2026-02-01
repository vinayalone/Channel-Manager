import os
import logging
import asyncio
import datetime
import pytz
import asyncpg
from pyrogram import Client, filters, idle, errors
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

# --- CONFIGURATION (From Railway Variables) ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

# Admin IDs: Comma separated list of users who can own the bot
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ManagerBot")

# --- INIT CLIENTS ---
app = Client("manager_interface", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = AsyncIOScheduler(timezone=IST)

# Global Cache (For temporary state during conversation)
login_state = {}
user_state = {}
db_pool = None

# --- DATABASE HELPERS (AsyncPG) ---

async def get_db():
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
    return db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        # Sessions table
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_sessions (user_id BIGINT PRIMARY KEY, session_string TEXT)''')
        # Channels table
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_channels (user_id BIGINT, channel_id TEXT, title TEXT, PRIMARY KEY(user_id, channel_id))''')
        # Tasks table
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_tasks 
                          (task_id TEXT PRIMARY KEY, owner_id BIGINT, chat_id TEXT, msg_id BIGINT, 
                           source_chat BIGINT, pin BOOLEAN, delete_old BOOLEAN, 
                           repeat_interval TEXT, start_time TEXT, last_msg_id BIGINT)''')

# --- DB CRUD OPERATIONS ---

async def get_session(user_id):
    pool = await get_db()
    row = await pool.fetchrow("SELECT session_string FROM userbot_sessions WHERE user_id = $1", user_id)
    return row['session_string'] if row else None

async def save_session(user_id, session_string):
    pool = await get_db()
    await pool.execute("INSERT INTO userbot_sessions (user_id, session_string) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET session_string = $2", user_id, session_string)

async def del_session(user_id):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_sessions WHERE user_id = $1", user_id)

async def get_channels(user_id):
    pool = await get_db()
    return await pool.fetch("SELECT channel_id, title FROM userbot_channels WHERE user_id = $1", user_id)

async def add_channel_db(user_id, c_id, title):
    pool = await get_db()
    await pool.execute("INSERT INTO userbot_channels (user_id, channel_id, title) VALUES ($1, $2, $3) ON CONFLICT (user_id, channel_id) DO UPDATE SET title = $3", user_id, c_id, title)

async def del_channel_db(user_id, c_id):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1 AND channel_id = $2", user_id, c_id)

async def save_task(t):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_tasks (task_id, owner_id, chat_id, msg_id, source_chat, pin, delete_old, repeat_interval, start_time, last_msg_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (task_id) DO UPDATE SET last_msg_id = $10
    """, t['task_id'], t['owner_id'], t['chat_id'], t['msg_id'], t['source_chat'], t['pin'], t['delete_old'], t['repeat_interval'], t['start_time'], t['last_msg_id'])

async def get_all_tasks():
    pool = await get_db()
    rows = await pool.fetch("SELECT * FROM userbot_tasks")
    return [dict(row) for row in rows]

async def get_tasks_by_channel(user_id, chat_id):
    pool = await get_db()
    rows = await pool.fetch("SELECT * FROM userbot_tasks WHERE owner_id = $1 AND chat_id = $2", user_id, chat_id)
    return [dict(row) for row in rows]

async def delete_task(task_id):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_tasks WHERE task_id = $1", task_id)

async def update_last_msg(task_id, msg_id):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks SET last_msg_id = $1 WHERE task_id = $2", msg_id, task_id)

# --- COMMANDS ---

def is_auth(uid):
    return uid in ADMIN_IDS

@app.on_message(filters.command("manage"))
async def cmd_manage(c, m):
    if not is_auth(m.from_user.id): return
    uid = m.from_user.id
    
    if await get_session(uid):
        await show_main_menu(m)
    else:
        await m.reply_text(
            "👋 **Channel Manager Bot**\n\nI can schedule posts as YOU.\nPlease login first.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Login Account", callback_data="login_start")]])
        )

# --- CALLBACKS ---

@app.on_callback_query()
async def callback_handler(client, query):
    uid = query.from_user.id
    if not is_auth(uid): return
    d = query.data

    if d == "menu_home":
        user_state[uid] = None
        await show_main_menu(query.message)

    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await query.message.edit_text("📱 **Send Phone Number**\nFormat: `+1234567890`")

    elif d == "logout_confirm":
        await del_session(uid)
        await query.message.edit_text("✅ Logged out.")

    elif d == "add_channel":
        user_state[uid] = {"step": "waiting_forward"}
        await query.message.edit_text("📝 **Forward a Message** from the channel to me now.")

    elif d == "list_channels":
        await show_channels_list(uid, query.message)

    elif d.startswith("manage_ch_"):
        c_id = d.split("manage_ch_")[1]
        await show_channel_dashboard(uid, query.message, c_id)

    elif d.startswith("rem_ch_"):
        c_id = d.split("rem_ch_")[1]
        await del_channel_db(uid, c_id)
        await query.answer("Removed.")
        await show_channels_list(uid, query.message)

    elif d.startswith("new_post_"):
        c_id = d.split("new_post_")[1]
        user_state[uid] = {"step": "waiting_content", "target_channel": c_id}
        await query.message.edit_text("1️⃣ **Send Content**\n(Text, Photo, Video, etc.)")

    elif d == "date_now":
        user_state[uid]["start_time"] = datetime.datetime.now(IST) + datetime.timedelta(seconds=10) # Buffer
        await ask_repetition(query.message, uid)

    elif d == "repeat_none":
        user_state[uid]["interval"] = None
        await send_confirm_panel(query.message, uid)

    elif d in ["toggle_pin", "toggle_del"]:
        key = "pin" if "pin" in d else "del"
        user_state[uid][key] = not user_state[uid].get(key, False)
        await send_confirm_panel(query.message, uid)

    elif d == "confirm_schedule":
        st = user_state[uid]
        t_id = f"task_{int(datetime.datetime.now().timestamp())}"
        
        task_data = {
            "task_id": t_id,
            "owner_id": uid,
            "chat_id": st["target_channel"],
            "msg_id": st["msg_id"],
            "source_chat": query.message.chat.id,
            "pin": st.get("pin", False),
            "delete_old": st.get("del", False),
            "repeat_interval": st.get("interval"),
            "start_time": st["start_time"].isoformat(),
            "last_msg_id": None
        }
        
        await save_task(task_data)
        add_job(t_id, task_data)
        user_state[uid] = None
        await query.message.edit_text("✅ **Task Scheduled!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))

    elif d.startswith("view_tasks_"):
        c_id = d.split("view_tasks_")[1]
        await show_active_tasks(uid, query.message, c_id)

    elif d.startswith("del_task_"):
        t_id = d.split("del_task_")[1]
        try: scheduler.remove_job(t_id)
        except: pass
        await delete_task(t_id)
        await query.answer("Deleted")
        await show_main_menu(query.message)

# --- MESSAGES ---

@app.on_message(filters.private & ~filters.command("manage"))
async def message_handler(client, message):
    uid = message.from_user.id
    if not is_auth(uid): return
    text = message.text

    # Login Logic
    if uid in login_state:
        await handle_login(client, message, uid)
        return

    st = user_state.get(uid, {})
    step = st.get("step")

    if step == "waiting_forward":
        if message.forward_from_chat:
            chat = message.forward_from_chat
            await add_channel_db(uid, str(chat.id), chat.title)
            user_state[uid] = None
            await message.reply(f"✅ Added: {chat.title}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Menu", callback_data="menu_home")]]))
        else:
            await message.reply("⚠️ Please forward a message FROM the channel.")

    elif step == "waiting_content":
        st["msg_id"] = message.id
        st["step"] = "waiting_date"
        user_state[uid] = st
        await message.reply(
            "2️⃣ **When to post?** (IST)\nFormat: `2026-02-05 14:30`\nOr click button:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ Post Now", callback_data="date_now")]])
        )

    elif step == "waiting_date":
        try:
            dt = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M")
            st["start_time"] = IST.localize(dt)
            await ask_repetition(message, uid)
        except:
            await message.reply("❌ Invalid format.")

    elif step == "waiting_repeat":
        # Simple parser
        val = None
        if text:
            if "min" in text: val = f"minutes={text.split()[0]}"
            elif "hour" in text: val = f"hours={text.split()[0]}"
            elif "day" in text: val = f"days={text.split()[0]}"
        
        if val:
            st["interval"] = val
            await send_confirm_panel(message, uid)
        else:
            await message.reply("❌ Use: `10 mins`, `2 hours`, `1 day`")

# --- LOGIN HANDLER ---
async def handle_login(client, message, uid):
    st = login_state[uid]
    text = message.text

    if st["step"] == "waiting_phone":
        try:
            temp_client = Client(f":memory:", api_id=API_ID, api_hash=API_HASH)
            await temp_client.connect()
            sent = await temp_client.send_code(text)
            st.update({"client": temp_client, "phone": text, "hash": sent.phone_code_hash, "step": "waiting_code"})
            await message.reply("📩 **Code Sent!**\nSend as `12345` (No spaces)")
        except Exception as e:
            await message.reply(f"❌ Error: {e}")

    elif st["step"] == "waiting_code":
        try:
            await st["client"].sign_in(st["phone"], st["hash"], text)
            sess = await st["client"].export_session_string()
            await save_session(uid, sess)
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Login Success!**")
        except errors.SessionPasswordNeeded:
            st["step"] = "waiting_pass"
            await message.reply("🔐 **Enter 2FA Password:**")
        except Exception as e:
            await message.reply(f"❌ Error: {e}")

    elif st["step"] == "waiting_pass":
        try:
            await st["client"].check_password(text)
            sess = await st["client"].export_session_string()
            await save_session(uid, sess)
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Login Success!**")
        except Exception as e:
            await message.reply(f"❌ Error: {e}")

# --- UI HELPERS ---
async def show_main_menu(m):
    kb = [[InlineKeyboardButton("📢 Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add", callback_data="add_channel")],
          [InlineKeyboardButton("🚪 Logout", callback_data="logout_confirm")]]
    txt = "🤖 **Manager Dashboard**"
    if isinstance(m, Message): await m.reply(txt, reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def show_channels_list(uid, m):
    chs = await get_channels(uid)
    if not chs:
        await m.edit_text("❌ No channels.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add", callback_data="add_channel")]]))
        return
    kb = []
    for c in chs: kb.append([InlineKeyboardButton(c['title'], callback_data=f"manage_ch_{c['channel_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await m.edit_text("Select Channel:", reply_markup=InlineKeyboardMarkup(kb))

async def show_channel_dashboard(uid, m, c_id):
    tasks = await get_tasks_by_channel(uid, c_id)
    count = len(tasks)
    kb = [
        [InlineKeyboardButton("✨ New Post", callback_data=f"new_post_{c_id}"), InlineKeyboardButton(f"📋 Scheduled ({count})", callback_data=f"view_tasks_{c_id}")],
        [InlineKeyboardButton("🗑 Remove", callback_data=f"rem_ch_{c_id}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ]
    await m.edit_text(f"⚙️ **Channel Manager**", reply_markup=InlineKeyboardMarkup(kb))

async def ask_repetition(m, uid):
    user_state[uid]["step"] = "waiting_repeat"
    await m.edit_text("3️⃣ **Repetition?**\nType `1 day`, `2 hours` or click:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚫 One Time", callback_data="repeat_none")]]))

async def send_confirm_panel(m, uid):
    st = user_state[uid]
    st.setdefault("pin", False)
    st.setdefault("del", False)
    kb = [[InlineKeyboardButton(f"📌 Pin: {st['pin']}", callback_data="toggle_pin"), InlineKeyboardButton(f"🗑 Del Old: {st['del']}", callback_data="toggle_del")],
          [InlineKeyboardButton("✅ START", callback_data="confirm_schedule")]]
    msg_txt = f"⚙️ **Confirm**\nTime: `{st['start_time']}`\nRepeat: `{st.get('interval', 'No')}`"
    if isinstance(m, Message): await m.reply(msg_txt, reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text(msg_txt, reply_markup=InlineKeyboardMarkup(kb))

async def show_active_tasks(uid, m, c_id):
    tasks = await get_tasks_by_channel(uid, c_id)
    txt = "**Tasks:**\n"
    kb = []
    for t in tasks:
        txt += f"• {t['start_time']} (Rep: {t['repeat_interval']})\n"
        kb.append([InlineKeyboardButton("🛑 Stop", callback_data=f"del_task_{t['task_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"manage_ch_{c_id}")])
    await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

# --- WORKER ---
def add_job(t_id, t):
    async def job():
        try:
            # SAFETY: Connect only when needed, but using Async Context Manager
            sess = await get_session(t["owner_id"])
            if not sess: return
            
            async with Client(f":memory:", api_id=API_ID, api_hash=API_HASH, session_string=sess) as user:
                # 1. Delete Old if needed
                if t["delete_old"] and t["last_msg_id"]:
                    try: await user.delete_messages(int(t["chat_id"]), int(t["last_msg_id"]))
                    except: pass
                
                # 2. Copy Message (From Bot's chat to Channel)
                me_bot = await app.get_me()
                sent = await user.copy_message(
                    chat_id=int(t["chat_id"]),
                    from_chat_id=me_bot.username, 
                    message_id=int(t["msg_id"])
                )
                
                # 3. Pin
                if t["pin"]:
                    try: await sent.pin()
                    except: pass
                
                await update_last_msg(t["task_id"], sent.id)
                
        except Exception as e:
            logger.error(f"Job failed: {e}")

    # Trigger Logic
    dt = datetime.datetime.fromisoformat(t["start_time"])
    if t["repeat_interval"]:
        # Parse "minutes=10" -> kwarg
        k, v = t["repeat_interval"].split("=")
        kwargs = {k: int(v)}
        trigger = IntervalTrigger(start_date=dt, timezone=IST, **kwargs)
    else:
        trigger = DateTrigger(run_date=dt, timezone=IST)
        
    scheduler.add_job(job, trigger, id=t_id, replace_existing=True)

# --- STARTUP ---
async def main():
    await init_db()
    # Load tasks on boot
    all_tasks = await get_all_tasks()
    for t in all_tasks:
        add_job(t["task_id"], t)
    
    scheduler.start()
    await app.start()
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
