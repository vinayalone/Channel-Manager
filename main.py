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

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ManagerBot")

# --- INIT ---
app = Client("manager_v2", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = AsyncIOScheduler(timezone=IST)
db_pool = None

# Global Cache
login_state = {}
user_state = {}

# --- DATABASE (AsyncPG) ---
async def get_db():
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
    return db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_sessions (user_id BIGINT PRIMARY KEY, session_string TEXT)''')
        # channels: stores channel_id and title
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_channels (user_id BIGINT, channel_id TEXT, title TEXT, PRIMARY KEY(user_id, channel_id))''')
        # tasks: stores content type and file_id instead of just msg_id
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_tasks 
                          (task_id TEXT PRIMARY KEY, owner_id BIGINT, chat_id TEXT, 
                           content_type TEXT, content_text TEXT, file_id TEXT, 
                           pin BOOLEAN, delete_old BOOLEAN, 
                           repeat_interval TEXT, start_time TEXT, last_msg_id BIGINT)''')

# --- DB HELPERS ---
async def get_session(user_id):
    pool = await get_db()
    row = await pool.fetchrow("SELECT session_string FROM userbot_sessions WHERE user_id = $1", user_id)
    return row['session_string'] if row else None

async def save_session(user_id, session):
    pool = await get_db()
    await pool.execute("INSERT INTO userbot_sessions (user_id, session_string) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET session_string = $2", user_id, session)

async def del_session(user_id):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_sessions WHERE user_id = $1", user_id)

async def add_channel(user_id, cid, title):
    pool = await get_db()
    await pool.execute("INSERT INTO userbot_channels (user_id, channel_id, title) VALUES ($1, $2, $3) ON CONFLICT (user_id, channel_id) DO UPDATE SET title = $3", user_id, cid, title)

async def get_channels(user_id):
    pool = await get_db()
    return await pool.fetch("SELECT * FROM userbot_channels WHERE user_id = $1", user_id)

async def del_channel(user_id, cid):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1 AND channel_id = $2", user_id, cid)

async def save_task(t):
    pool = await get_db()
    # Safely insert None values
    await pool.execute("""
        INSERT INTO userbot_tasks VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (task_id) DO UPDATE SET last_msg_id = $11
    """, t['task_id'], t['owner_id'], t['chat_id'], t['content_type'], t['content_text'], t['file_id'], 
       t['pin'], t['delete_old'], t['repeat_interval'], t['start_time'], t['last_msg_id'])

async def get_all_tasks():
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks")]

async def get_user_tasks(user_id, chat_id):
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks WHERE owner_id = $1 AND chat_id = $2", user_id, chat_id)]

async def delete_task(task_id):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_tasks WHERE task_id = $1", task_id)

async def update_last_msg(task_id, msg_id):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks SET last_msg_id = $1 WHERE task_id = $2", msg_id, task_id)

# --- BOT INTERFACE ---

@app.on_message(filters.command("manage") | filters.command("start"))
async def start_cmd(c, m):
    uid = m.from_user.id
    if await get_session(uid):
        await show_main_menu(m)
    else:
        await m.reply_text(
            "👋 **Manager Bot v2**\n\nI schedule posts for you.\nFirst, I need to log in to your account.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Login", callback_data="login_start")]])
        )

@app.on_callback_query()
async def callback_router(c, q):
    uid = q.from_user.id
    d = q.data

    # --- NAV ---
    if d == "menu_home":
        user_state[uid] = None
        await show_main_menu(q.message)
    
    # --- LOGIN ---
    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await q.message.edit_text("📱 **Send Phone Number**\nFormat: `+1234567890`")
    
    elif d == "logout":
        await del_session(uid)
        await q.message.edit_text("👋 Logged out.")

    # --- CHANNELS ---
    elif d == "list_channels":
        await show_channels(uid, q.message)
    
    elif d == "add_channel":
        user_state[uid] = {"step": "waiting_forward"}
        await q.message.edit_text("📝 **Forward a message** from your channel to me now.")
    
    elif d.startswith("ch_"):
        cid = d.split("ch_")[1]
        await show_channel_options(uid, q.message, cid)
    
    elif d.startswith("rem_"):
        cid = d.split("rem_")[1]
        await del_channel(uid, cid)
        await q.answer("Removed!")
        await show_channels(uid, q.message)

    # --- POSTING ---
    elif d.startswith("new_"):
        cid = d.split("new_")[1]
        user_state[uid] = {"step": "waiting_content", "target": cid}
        await q.message.edit_text("1️⃣ **Send the Post**\n(Text, Photo, Video, or Sticker)")

    # --- EASY SCHEDULER ---
    elif d.startswith("time_"):
        # Calculate time based on button
        offset = d.split("time_")[1] # "0", "10", "60", "1440"
        
        now = datetime.datetime.now(IST)
        if offset == "0":
            run_time = now + datetime.timedelta(seconds=5) # 5s buffer
        else:
            run_time = now + datetime.timedelta(minutes=int(offset))
        
        user_state[uid]["start_time"] = run_time
        await ask_repetition(q.message, uid)

    elif d.startswith("rep_"):
        val = d.split("rep_")[1] # "0", "60", "1440"
        interval = None
        if val != "0":
            interval = f"minutes={val}"
        
        user_state[uid]["interval"] = interval
        await confirm_task(q.message, uid)

    # --- CONFIRM & SAVE ---
    elif d == "save_task":
        await create_task_logic(uid, q)

    # --- TASK MANAGE ---
    elif d.startswith("tasks_"):
        cid = d.split("tasks_")[1]
        await list_active_tasks(uid, q.message, cid)
    
    elif d.startswith("del_task_"):
        tid = d.split("del_task_")[1]
        try: scheduler.remove_job(tid)
        except: pass
        await delete_task(tid)
        await q.answer("Task deleted")
        await show_main_menu(q.message)

# --- MESSAGES & INPUTS ---

@app.on_message(filters.private & ~filters.command("manage") & ~filters.command("start"))
async def handle_inputs(c, m):
    uid = m.from_user.id
    text = m.text or ""

    # Login Logic
    if uid in login_state:
        await process_login(c, m, uid)
        return

    st = user_state.get(uid, {})
    step = st.get("step")

    if step == "waiting_forward":
        if m.forward_from_chat:
            chat = m.forward_from_chat
            await add_channel(uid, str(chat.id), chat.title)
            user_state[uid] = None
            await m.reply(f"✅ Added **{chat.title}**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))
        else:
            await m.reply("❌ That's not a channel forward. Try again.")

    elif step == "waiting_content":
        # Capture Content cleanly
        content_type = "text"
        file_id = None
        content_text = m.text or m.caption or ""

        if m.photo:
            content_type = "photo"
            file_id = m.photo.file_id
        elif m.video:
            content_type = "video"
            file_id = m.video.file_id
        elif m.document:
            content_type = "document"
            file_id = m.document.file_id
        
        st.update({
            "content_type": content_type,
            "content_text": content_text,
            "file_id": file_id,
            "step": "waiting_time"
        })
        user_state[uid] = st
        
        # Easy Time Buttons
        kb = [
            [InlineKeyboardButton("🚀 Post Now", callback_data="time_0")],
            [InlineKeyboardButton("⏱️ +10 Mins", callback_data="time_10"), InlineKeyboardButton("🕐 +1 Hour", callback_data="time_60")],
            [InlineKeyboardButton("🌙 +6 Hours", callback_data="time_360"), InlineKeyboardButton("📅 +24 Hours", callback_data="time_1440")]
        ]
        await m.reply("2️⃣ **When should I post this?**", reply_markup=InlineKeyboardMarkup(kb))

# --- PROCESS FUNCTIONS ---

async def ask_repetition(m, uid):
    # Easy Repeat Buttons
    kb = [
        [InlineKeyboardButton("🚫 No Repeat", callback_data="rep_0")],
        [InlineKeyboardButton("🔁 Every 30 Mins", callback_data="rep_30"), InlineKeyboardButton("🔁 Hourly", callback_data="rep_60")],
        [InlineKeyboardButton("🔁 Every 6 Hours", callback_data="rep_360"), InlineKeyboardButton("🔁 Daily", callback_data="rep_1440")]
    ]
    if isinstance(m, Message): await m.reply("3️⃣ **Should this repeat?**", reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text("3️⃣ **Should this repeat?**", reply_markup=InlineKeyboardMarkup(kb))

async def confirm_task(m, uid):
    st = user_state[uid]
    t_str = st["start_time"].strftime("%I:%M %p")
    r_str = st["interval"] if st["interval"] else "Once"
    
    txt = (f"✅ **Ready?**\n\n"
           f"📢 Type: `{st['content_type']}`\n"
           f"🕒 Time: `{t_str}`\n"
           f"🔄 Repeat: `{r_str}`")
    
    kb = [[InlineKeyboardButton("✅ Confirm & Schedule", callback_data="save_task")]]
    await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def create_task_logic(uid, q):
    st = user_state[uid]
    tid = f"task_{int(datetime.datetime.now().timestamp())}"
    
    task_data = {
        "task_id": tid,
        "owner_id": uid,
        "chat_id": st["target"],
        "content_type": st["content_type"],
        "content_text": st["content_text"],
        "file_id": st["file_id"],
        "pin": True,       # Default ON
        "delete_old": True, # Default ON
        "repeat_interval": st["interval"],
        "start_time": st["start_time"].isoformat(),
        "last_msg_id": None
    }
    
    await save_task(task_data)
    add_scheduler_job(tid, task_data)
    user_state[uid] = None
    await q.message.edit_text("🎉 **Done!** Post scheduled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))

# --- MENUS ---
async def show_main_menu(m):
    kb = [[InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Connect Channel", callback_data="add_channel")],
          [InlineKeyboardButton("🚪 Logout", callback_data="logout")]]
    txt = "👋 **Manager Dashboard**\nManage your channels easily."
    if isinstance(m, Message): await m.reply(txt, reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def show_channels(uid, m):
    chs = await get_channels(uid)
    if not chs:
        await m.edit_text("❌ No channels connected.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Connect One", callback_data="add_channel")]]))
        return
    kb = []
    for c in chs: kb.append([InlineKeyboardButton(c['title'], callback_data=f"ch_{c['channel_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await m.edit_text("👇 **Select a Channel:**", reply_markup=InlineKeyboardMarkup(kb))

async def show_channel_options(uid, m, cid):
    tasks = await get_user_tasks(uid, cid)
    kb = [
        [InlineKeyboardButton("✍️ Schedule Post", callback_data=f"new_{cid}")],
        [InlineKeyboardButton(f"📅 View Active Tasks ({len(tasks)})", callback_data=f"tasks_{cid}")],
        [InlineKeyboardButton("🗑 Unlink Channel", callback_data=f"rem_{cid}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ]
    await m.edit_text(f"⚙️ **Managing Channel**", reply_markup=InlineKeyboardMarkup(kb))

async def list_active_tasks(uid, m, cid):
    tasks = await get_user_tasks(uid, cid)
    if not tasks:
        await m.edit_text("✅ No active tasks.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")]]))
        return
    txt = "**Active Tasks:**\n"
    kb = []
    for t in tasks:
        # Easy readable time
        dt = datetime.datetime.fromisoformat(t["start_time"])
        txt += f"• {t['content_type'].upper()} at {dt.strftime('%H:%M')} (Rep: {t['repeat_interval'] or 'No'})\n"
        kb.append([InlineKeyboardButton("🗑 Delete Task", callback_data=f"del_task_{t['task_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")])
    await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

# --- WORKER (THE FIX) ---
def add_scheduler_job(tid, t):
    async def job_func():
        try:
            session = await get_session(t["owner_id"])
            if not session: return # User logged out
            
            # Start User Client
            async with Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session) as user:
                target = int(t["chat_id"])
                
                # 1. Delete Old Message
                if t["delete_old"] and t["last_msg_id"]:
                    try: await user.delete_messages(target, int(t["last_msg_id"]))
                    except: pass
                
                # 2. Send New Message (Direct Send, No Copy)
                sent = None
                caption = t["content_text"]
                
                if t["content_type"] == "text":
                    sent = await user.send_message(target, t["content_text"])
                elif t["content_type"] == "photo":
                    sent = await user.send_photo(target, t["file_id"], caption=caption)
                elif t["content_type"] == "video":
                    sent = await user.send_video(target, t["file_id"], caption=caption)
                elif t["content_type"] == "document":
                    sent = await user.send_document(target, t["file_id"], caption=caption)
                
                if sent:
                    # 3. Pin
                    if t["pin"]:
                        try: await sent.pin()
                        except: pass
                    # Update DB
                    await update_last_msg(tid, sent.id)
                    
        except Exception as e:
            logger.error(f"Worker Error {tid}: {e}")

    # Scheduling Logic
    dt = datetime.datetime.fromisoformat(t["start_time"])
    trigger = None
    
    if t["repeat_interval"]:
        # Parse "minutes=30"
        k, v = t["repeat_interval"].split("=")
        kw = {k: int(v)}
        trigger = IntervalTrigger(start_date=dt, timezone=IST, **kw)
    else:
        trigger = DateTrigger(run_date=dt, timezone=IST)
        
    scheduler.add_job(job_func, trigger, id=tid, replace_existing=True)

# --- LOGIN HELPERS ---
async def process_login(c, m, uid):
    st = login_state[uid]
    text = m.text.strip()
    
    if st["step"] == "waiting_phone":
        try:
            temp = Client(":memory:", api_id=API_ID, api_hash=API_HASH)
            await temp.connect()
            sent = await temp.send_code(text)
            st.update({"client": temp, "phone": text, "hash": sent.phone_code_hash, "step": "waiting_code"})
            await m.reply("📩 **Code sent!**\nSend it like: `12345`")
        except Exception as e:
            await m.reply(f"❌ Error: {e}")

    elif st["step"] == "waiting_code":
        try:
            await st["client"].sign_in(st["phone"], st["hash"], text)
            sess = await st["client"].export_session_string()
            await save_session(uid, sess)
            await st["client"].disconnect()
            del login_state[uid]
            await m.reply("✅ **Success!** You are logged in.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Start Managing", callback_data="menu_home")]]))
        except errors.SessionPasswordNeeded:
            st["step"] = "waiting_pass"
            await m.reply("🔐 **2FA Password required:**")
        except Exception as e:
            await m.reply(f"❌ Error: {e}")

    elif st["step"] == "waiting_pass":
        try:
            await st["client"].check_password(text)
            sess = await st["client"].export_session_string()
            await save_session(uid, sess)
            await st["client"].disconnect()
            del login_state[uid]
            await m.reply("✅ **Success!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Start Managing", callback_data="menu_home")]]))
        except Exception as e:
            await m.reply(f"❌ Error: {e}")

# --- STARTUP ---
async def main():
    await init_db()
    # Resume tasks
    tasks = await get_all_tasks()
    for t in tasks:
        add_scheduler_job(t['task_id'], t)
        
    scheduler.start()
    await app.start()
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
