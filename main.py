import os
import logging
import asyncio
import datetime
import pytz
import asyncpg
import json  # Needed to serialize entities
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
app = Client("manager_premium_v11", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = None 
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
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_channels (user_id BIGINT, channel_id TEXT, title TEXT, PRIMARY KEY(user_id, channel_id))''')
        
        # V7 TABLE: Added 'entities' column to store Premium formatting
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_tasks_v7
                          (task_id TEXT PRIMARY KEY, owner_id BIGINT, chat_id TEXT, 
                           content_type TEXT, content_text TEXT, file_id TEXT, 
                           entities TEXT,  -- New Column for Premium Data
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
    # Save entities as JSON string
    entities_json = t.get('entities') # Already serialized in logic
    await pool.execute("""
        INSERT INTO userbot_tasks_v7 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (task_id) DO UPDATE SET last_msg_id = $12, start_time = $11
    """, t['task_id'], t['owner_id'], t['chat_id'], t['content_type'], t['content_text'], t['file_id'], 
       entities_json, t['pin'], t['delete_old'], t['repeat_interval'], t['start_time'], t['last_msg_id'])

async def get_all_tasks():
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks_v7")]

async def get_user_tasks(user_id, chat_id):
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks_v7 WHERE owner_id = $1 AND chat_id = $2", user_id, chat_id)]

async def delete_task(task_id):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_tasks_v7 WHERE task_id = $1", task_id)

async def update_last_msg(task_id, msg_id):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks_v7 SET last_msg_id = $1 WHERE task_id = $2", msg_id, task_id)

async def update_next_run(task_id, next_time_str):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks_v7 SET start_time = $1 WHERE task_id = $2", next_time_str, task_id)

# --- BOT INTERFACE ---

@app.on_message(filters.command("manage") | filters.command("start"))
async def start_cmd(c, m):
    uid = m.from_user.id
    if await get_session(uid):
        await show_main_menu(m)
    else:
        await m.reply_text(
            "👋 **Manager Bot V11 (Premium)**\n\nPlease login to start.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Login", callback_data="login_start")]])
        )

@app.on_callback_query()
async def callback_router(c, q):
    uid = q.from_user.id
    d = q.data

    if d == "menu_home":
        user_state[uid] = None
        await show_main_menu(q.message)
    
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
        await q.message.edit_text("📝 **Forward a message** from your channel to me now.", 
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]]))
    
    elif d.startswith("ch_"):
        cid = d.split("ch_")[1]
        await show_channel_options(uid, q.message, cid)
    
    elif d.startswith("rem_"):
        cid = d.split("rem_")[1]
        await del_channel(uid, cid)
        await q.answer("Removed!")
        await show_channels(uid, q.message)

    # --- POST CREATION FLOW ---
    elif d.startswith("new_"):
        cid = d.split("new_")[1]
        user_state[uid] = {"step": "waiting_content", "target": cid}
        await q.message.edit_text("1️⃣ **Send the Post**\n(Text, Photo, Video, or Sticker)", 
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data=f"ch_{cid}")]]))

    # --- TIME SELECTION ---
    elif d == "step_time":
        await show_time_menu(q.message, uid)

    elif d.startswith("time_"):
        offset = d.split("time_")[1] 
        
        if offset == "custom":
            user_state[uid]["step"] = "waiting_custom_date"
            await q.message.edit_text(
                "📅 **Enter Custom Date**\n\n"
                "Format: `02-Feb 11:56 PM`\n"
                "Example: `05-Feb 02:30 PM`\n\n"
                "Type it below 👇",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="step_time")]])
            )
            return

        now = datetime.datetime.now(IST)
        if offset == "0":
            run_time = now + datetime.timedelta(seconds=5)
        else:
            run_time = now + datetime.timedelta(minutes=int(offset))
            run_time = run_time.replace(second=0, microsecond=0)
        
        user_state[uid]["start_time"] = run_time
        await ask_repetition(q.message, uid)

    # --- REPETITION ---
    elif d.startswith("rep_"):
        val = d.split("rep_")[1]
        interval = None
        if val != "0":
            interval = f"minutes={val}"
        
        user_state[uid]["interval"] = interval
        await ask_settings(q.message, uid)

    # --- SETTINGS & CONFIRM ---
    elif d in ["toggle_pin", "toggle_del"]:
        st = user_state[uid]
        st.setdefault("pin", True)
        st.setdefault("del", True)
        if d == "toggle_pin": st["pin"] = not st["pin"]
        if d == "toggle_del": st["del"] = not st["del"]
        await ask_settings(q.message, uid)

    elif d == "goto_confirm":
        await confirm_task(q.message, uid)

    elif d == "save_task":
        await create_task_logic(uid, q)

    # --- TASK MANAGEMENT ---
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

    if uid in login_state:
        await process_login(c, m, uid)
        return

    st = user_state.get(uid, {})
    step = st.get("step")

    # 1. Add Channel
    if step == "waiting_forward":
        if m.forward_from_chat:
            chat = m.forward_from_chat
            await add_channel(uid, str(chat.id), chat.title)
            user_state[uid] = None
            await m.reply(f"✅ Added **{chat.title}**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))
        else:
            await m.reply("❌ Invalid. Forward from a channel.")

    # 2. Receive Content (PREMIUM FIX APPLIED HERE)
    elif step == "waiting_content":
        content_type = "text"
        file_id = None
        content_text = m.text or m.caption or ""
        
        # ✅ CAPTURE ENTITIES (This holds the Premium Custom Emoji data)
        raw_entities = m.entities or m.caption_entities
        entities_str = str(raw_entities) if raw_entities else None

        if m.photo:
            content_type = "photo"
            file_id = m.photo.file_id
        elif m.video:
            content_type = "video"
            file_id = m.video.file_id
        elif m.document:
            content_type = "document"
            file_id = m.document.file_id
        elif m.sticker:
            content_type = "sticker"
            file_id = m.sticker.file_id
        
        st.update({
            "content_type": content_type,
            "content_text": content_text,
            "file_id": file_id,
            "entities": entities_str, # Store raw string of entities
            "step": "waiting_time"
        })
        user_state[uid] = st
        
        await show_time_menu(m, uid)

    # 3. Custom Date Input
    elif step == "waiting_custom_date":
        try:
            current_year = datetime.datetime.now(IST).year
            full_str = f"{current_year}-{text}"
            dt = datetime.datetime.strptime(full_str, "%Y-%d-%b %I:%M %p")
            dt = IST.localize(dt)
            user_state[uid]["start_time"] = dt
            await ask_repetition(m, uid)
        except ValueError:
            await m.reply("❌ **Invalid Format!**\nUse: `02-Feb 11:56 PM`")

# --- UI MENUS ---

async def show_main_menu(m):
    kb = [[InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")],
          [InlineKeyboardButton("🚪 Logout", callback_data="logout")]]
    txt = "👋 **Manager Dashboard**"
    if isinstance(m, Message): await m.reply(txt, reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def show_channels(uid, m):
    chs = await get_channels(uid)
    if not chs:
        await m.edit_text("❌ No channels.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add One", callback_data="add_channel")]]))
        return
    kb = []
    for c in chs: kb.append([InlineKeyboardButton(c['title'], callback_data=f"ch_{c['channel_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await m.edit_text("👇 **Select a Channel:**", reply_markup=InlineKeyboardMarkup(kb))

async def show_channel_options(uid, m, cid):
    tasks = await get_user_tasks(uid, cid)
    kb = [
        [InlineKeyboardButton("✍️ Schedule Post", callback_data=f"new_{cid}")],
        [InlineKeyboardButton(f"📅 Scheduled ({len(tasks)})", callback_data=f"tasks_{cid}")],
        [InlineKeyboardButton("🗑 Unlink", callback_data=f"rem_{cid}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ]
    await m.edit_text(f"⚙️ **Managing Channel**", reply_markup=InlineKeyboardMarkup(kb))

async def show_time_menu(m, uid):
    kb = [
        [InlineKeyboardButton("🚀 Post Now", callback_data="time_0")],
        [InlineKeyboardButton("⏱️ +15 Mins", callback_data="time_15"), InlineKeyboardButton("🕐 +1 Hour", callback_data="time_60")],
        [InlineKeyboardButton("📅 Custom Date", callback_data="time_custom")]
    ]
    txt = "2️⃣ **When to post?**"
    if isinstance(m, Message): await m.reply(txt, reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def ask_repetition(m, uid):
    kb = [
        [InlineKeyboardButton("🚫 No Repeat", callback_data="rep_0")],
        [InlineKeyboardButton("🔁 30 Mins", callback_data="rep_30"), InlineKeyboardButton("🔁 Hourly", callback_data="rep_60")],
        [InlineKeyboardButton("🔁 6 Hours", callback_data="rep_360"), InlineKeyboardButton("🔁 Daily", callback_data="rep_1440")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_time")]
    ]
    st = user_state[uid]
    time_str = st["start_time"].strftime("%d-%b %I:%M %p")
    txt = f"3️⃣ **Repeat?**\nSelected Time: `{time_str}`"
    if isinstance(m, Message): await m.reply(txt, reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def ask_settings(m, uid):
    st = user_state[uid]
    st.setdefault("pin", True)
    st.setdefault("del", True)
    
    pin_icon = "✅" if st["pin"] else "❌"
    del_icon = "✅" if st["del"] else "❌"
    
    kb = [
        [InlineKeyboardButton(f"📌 Pin Msg: {pin_icon}", callback_data="toggle_pin")],
        [InlineKeyboardButton(f"🗑 Del Old: {del_icon}", callback_data="toggle_del")],
        [InlineKeyboardButton("➡️ Confirm", callback_data="goto_confirm")],
        [InlineKeyboardButton("🔙 Back", callback_data="time_0")]
    ]
    txt = "4️⃣ **Settings**"
    if isinstance(m, Message): await m.reply(txt, reply_markup=InlineKeyboardMarkup(kb))
    else: await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def confirm_task(m, uid):
    st = user_state[uid]
    t_str = st["start_time"].strftime("%d-%b %I:%M %p")
    r_str = st["interval"] if st["interval"] else "Once"
    
    txt = (f"✅ **Summary**\n"
           f"📅 `{t_str}`\n"
           f"🔁 `{r_str}`\n"
           f"📌 Pin: {st['pin']} | 🗑 Del: {st['del']}")
    
    kb = [[InlineKeyboardButton("✅ Schedule It", callback_data="save_task")],
          [InlineKeyboardButton("🔙 Edit", callback_data="time_0")]]
    
    await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def list_active_tasks(uid, m, cid):
    tasks = await get_user_tasks(uid, cid)
    if not tasks:
        await m.edit_text("✅ No active tasks.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")]]))
        return
    txt = "**Active Tasks:**\n"
    kb = []
    for t in tasks:
        try:
            dt = datetime.datetime.fromisoformat(t["start_time"])
            time_str = dt.strftime('%d-%b %I:%M %p')
        except: time_str = "?"
        txt += f"• `{time_str}` ({t['repeat_interval'] or 'Once'})\n"
        kb.append([InlineKeyboardButton(f"🗑 Delete {time_str}", callback_data=f"del_task_{t['task_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")])
    await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

# --- WORKER (With Premium Entity Support) ---
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
        "entities": st.get("entities"), # Pass the entities
        "pin": st["pin"],
        "delete_old": st["del"],
        "repeat_interval": st["interval"],
        "start_time": st["start_time"].isoformat(),
        "last_msg_id": None
    }
    
    try:
        await save_task(task_data)
        add_scheduler_job(tid, task_data)
        await q.message.edit_text("🎉 **Scheduled!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="menu_home")]]))
    except Exception as e:
        logger.error(f"Save Error: {e}")
        await q.message.edit_text(f"❌ Error: {e}")

def add_scheduler_job(tid, t):
    if scheduler is None: return

    async def job_func():
        logger.info(f"🚀 JOB {tid} TRIGGERED")
        next_run_iso = None
        if t["repeat_interval"]:
            try:
                now = datetime.datetime.now(IST)
                mins = int(t["repeat_interval"].split("=")[1])
                next_run = now + datetime.timedelta(minutes=mins)
                next_run_iso = next_run.isoformat()
            except: pass

        try:
            session = await get_session(t["owner_id"])
            if not session: return 
            
            async with Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session) as user:
                target = int(t["chat_id"])
                
                # Peer Resolution
                try: await user.get_chat(target)
                except:
                    async for dialog in user.get_dialogs(limit=200):
                        if dialog.chat.id == target: break
                
                # 1. Del Old
                if t["delete_old"] and t["last_msg_id"]:
                    try: await user.delete_messages(target, int(t["last_msg_id"]))
                    except: pass
                
                # 2. Send (WITH ENTITIES)
                sent = None
                caption = t["content_text"]
                # Parse entities back from string if they exist
                entities = eval(t["entities"]) if t["entities"] and t["entities"] != "None" else None

                try:
                    if t["content_type"] == "text":
                        sent = await user.send_message(target, t["content_text"], entities=entities)
                    elif t["content_type"] == "photo":
                        sent = await user.send_photo(target, t["file_id"], caption=caption, caption_entities=entities)
                    elif t["content_type"] == "video":
                        sent = await user.send_video(target, t["file_id"], caption=caption, caption_entities=entities)
                    elif t["content_type"] == "document":
                        sent = await user.send_document(target, t["file_id"], caption=caption, caption_entities=entities)
                    elif t["content_type"] == "sticker":
                        sent = await user.send_sticker(target, t["file_id"])
                    
                    logger.info(f"✅ Job {tid}: Message Sent! ID: {sent.id}")
                except Exception as e:
                    logger.error(f"❌ Job {tid} Fail: {e}")

                if sent:
                    if t["pin"]:
                        try: await sent.pin()
                        except: pass
                    await update_last_msg(tid, sent.id)

        except Exception as e:
            logger.error(f"🔥 Job {tid} Critical: {e}")
        
        finally:
            if next_run_iso:
                try: await update_next_run(tid, next_run_iso)
                except: pass

    dt = datetime.datetime.fromisoformat(t["start_time"])
    trigger = None
    if t["repeat_interval"]:
        mins = int(t["repeat_interval"].split("=")[1])
        trigger = IntervalTrigger(start_date=dt, timezone=IST, minutes=mins)
    else:
        trigger = DateTrigger(run_date=dt, timezone=IST)
    
    scheduler.add_job(job_func, trigger, id=tid, replace_existing=True)

# --- LOGIN ---
async def process_login(c, m, uid):
    st = login_state[uid]
    text = m.text.strip()
    if st["step"] == "waiting_phone":
        try:
            temp = Client(":memory:", api_id=API_ID, api_hash=API_HASH)
            await temp.connect()
            sent = await temp.send_code(text)
            st.update({"client": temp, "phone": text, "hash": sent.phone_code_hash, "step": "waiting_code"})
            await m.reply("📩 **Code?**")
        except Exception as e: await m.reply(f"❌ {e}")
    elif st["step"] == "waiting_code":
        try:
            await st["client"].sign_in(st["phone"], st["hash"], text)
            sess = await st["client"].export_session_string()
            await save_session(uid, sess)
            await st["client"].disconnect()
            del login_state[uid]
            await m.reply("✅ Logged In!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Go", callback_data="menu_home")]]))
        except errors.SessionPasswordNeeded:
            st["step"] = "waiting_pass"
            await m.reply("🔐 **2FA Password?**")
        except Exception as e: await m.reply(f"❌ {e}")
    elif st["step"] == "waiting_pass":
        try:
            await st["client"].check_password(text)
            sess = await st["client"].export_session_string()
            await save_session(uid, sess)
            await st["client"].disconnect()
            del login_state[uid]
            await m.reply("✅ Logged In!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Go", callback_data="menu_home")]]))
        except Exception as e: await m.reply(f"❌ {e}")

# --- STARTUP ---
async def main():
    await init_db()
    global scheduler
    scheduler = AsyncIOScheduler(timezone=IST, event_loop=asyncio.get_running_loop())
    scheduler.start()
    try:
        tasks = await get_all_tasks()
        logger.info(f"📂 Loaded {len(tasks)} tasks")
        for t in tasks: add_scheduler_job(t['task_id'], t)
    except: pass
    await app.start()
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
