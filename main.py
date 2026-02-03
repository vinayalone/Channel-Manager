import os
import logging
import asyncio
import datetime
import pytz
import asyncpg
import json
from io import BytesIO
from pyrogram import Client, filters, idle, errors, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, MessageEntity
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ManagerBot")

# --- INIT ---
app = Client("manager_v31_stable", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = None 
db_pool = None
queue_lock = None # Initialized in main

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
        await conn.execute('''CREATE TABLE IF NOT EXISTS userbot_tasks_v11
                          (task_id TEXT PRIMARY KEY, owner_id BIGINT, chat_id TEXT, 
                           content_type TEXT, content_text TEXT, file_id TEXT, 
                           entities TEXT, 
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
    tasks = await pool.fetch("SELECT task_id FROM userbot_tasks_v11 WHERE chat_id = $1", cid)
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t['task_id'])
            except: pass
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE chat_id = $1", cid)
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1 AND channel_id = $2", user_id, cid)

async def save_task(t):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_tasks_v11 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (task_id) DO UPDATE SET last_msg_id = $12, start_time = $11
    """, t['task_id'], t['owner_id'], t['chat_id'], t['content_type'], t['content_text'], t['file_id'], 
       t['entities'], t['pin'], t['delete_old'], t['repeat_interval'], t['start_time'], t['last_msg_id'])

async def get_all_tasks():
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks_v11")]

async def get_user_tasks(user_id, chat_id):
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks_v11 WHERE owner_id = $1 AND chat_id = $2", user_id, chat_id)]

async def get_single_task(task_id):
    pool = await get_db()
    row = await pool.fetchrow("SELECT * FROM userbot_tasks_v11 WHERE task_id = $1", task_id)
    return dict(row) if row else None

async def delete_task(task_id):
    pool = await get_db()
    row = await pool.fetchrow("SELECT chat_id FROM userbot_tasks_v11 WHERE task_id = $1", task_id)
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE task_id = $1", task_id)
    return row['chat_id'] if row else None

async def update_last_msg(task_id, msg_id):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks_v11 SET last_msg_id = $1 WHERE task_id = $2", msg_id, task_id)

async def get_task_last_msg_id(task_id):
    pool = await get_db()
    return await pool.fetchval("SELECT last_msg_id FROM userbot_tasks_v11 WHERE task_id = $1", task_id)

async def update_next_run(task_id, next_time_str):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks_v11 SET start_time = $1 WHERE task_id = $2", next_time_str, task_id)

# --- SERIALIZATION ---
def serialize_entities(entities_list):
    if not entities_list: return None
    data = []
    for e in entities_list:
        data.append({
            "type": str(e.type), "offset": e.offset, "length": e.length,
            "url": e.url, "language": e.language, "custom_emoji_id": e.custom_emoji_id
        })
    return json.dumps(data)

def deserialize_entities(json_str):
    if not json_str: return None
    try:
        data = json.loads(json_str)
        entities = []
        for item in data:
            type_str = item["type"].split(".")[-1] 
            e_type = getattr(enums.MessageEntityType, type_str)
            entity = MessageEntity(
                type=e_type, offset=item["offset"], length=item["length"],
                url=item["url"], language=item["language"], custom_emoji_id=item["custom_emoji_id"]
            )
            entities.append(entity)
        return entities
    except: return None

# --- UI HELPER: HYBRID FLOW ---
async def update_menu(m, text, kb, uid, force_new=False):
    markup = InlineKeyboardMarkup(kb) if kb else None
    
    if force_new:
        sent = await app.send_message(m.chat.id, text, reply_markup=markup)
        if uid in user_state:
            user_state[uid]["menu_msg_id"] = sent.id
        return

    st = user_state.get(uid, {})
    menu_id = st.get("menu_msg_id")
    
    if menu_id:
        try:
            await app.edit_message_text(m.chat.id, menu_id, text, reply_markup=markup)
            return
        except: pass 

    sent = await app.send_message(m.chat.id, text, reply_markup=markup)
    if uid in user_state:
        user_state[uid]["menu_msg_id"] = sent.id

# --- BOT INTERFACE ---

@app.on_message(filters.command("manage") | filters.command("start"))
async def start_cmd(c, m):
    uid = m.from_user.id
    if uid not in user_state: user_state[uid] = {}
    
    if await get_session(uid):
        kb = [[InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")],
              [InlineKeyboardButton("🚪 Logout", callback_data="logout")]]
        sent = await m.reply("👋 **Manager Dashboard**\n\nWelcome back, Admin.", reply_markup=InlineKeyboardMarkup(kb))
        user_state[uid]["menu_msg_id"] = sent.id
    else:
        await m.reply_text(
            "👋 **Welcome to Manager Bot!**\n\n"
            "I help you schedule and manage your channel posts.\n\n"
            "**How to use:**\n"
            "1️⃣ Login with your account.\n"
            "2️⃣ Add a Channel.\n"
            "3️⃣ Send content to schedule.\n\n"
            "👇 **Click Login to get started.**",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Login Account", callback_data="login_start")]])
        )

@app.on_callback_query()
async def callback_router(c, q):
    uid = q.from_user.id
    d = q.data

    if uid not in user_state: user_state[uid] = {}
    user_state[uid]["menu_msg_id"] = q.message.id

    if d == "menu_home":
        user_state[uid]["step"] = None
        await show_main_menu(q.message, uid)
    
    # --- PRO LOGIN FLOW ---
    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await update_menu(q.message, "📱 **Step 1: Phone Number**\n\nPlease enter your Telegram phone number with country code.\n\nExample: `+919876543210`", [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)
    
    elif d == "logout":
        await del_session(uid)
        await update_menu(q.message, "👋 **Logged out.**\n\nUse /start to login again.", None, uid)

    # --- TASK ACTIONS ---
    elif d.startswith("view_"):
        tid = d.split("view_")[1]
        await show_task_details(uid, q.message, tid)

    elif d.startswith("prev_"):
        tid = d.split("prev_")[1]
        task = await get_single_task(tid)
        if task and task['last_msg_id']:
            try:
                await app.copy_message(chat_id=uid, from_chat_id=int(task['chat_id']), message_id=task['last_msg_id'])
                await q.answer("✅ Preview sent!")
            except: await q.answer("❌ Cannot preview (Message not posted yet or deleted)")
        else:
            await q.answer("❌ Task hasn't run yet.")

    elif d.startswith("del_task_"):
        tid = d.split("del_task_")[1]
        try: scheduler.remove_job(tid)
        except: pass
        chat_id = await delete_task(tid)
        await q.answer("🗑 Task Deleted!")
        if chat_id: await list_active_tasks(uid, q.message, chat_id, force_new=False) 
        else: await show_main_menu(q.message, uid)

    elif d.startswith("back_list_"):
        cid = d.split("back_list_")[1]
        await list_active_tasks(uid, q.message, cid, force_new=False)

    # --- CHANNEL MANAGEMENT ---
    elif d == "list_channels":
        await show_channels(uid, q.message)
    
    elif d == "add_channel":
        user_state[uid]["step"] = "waiting_forward"
        await update_menu(q.message, "📝 **Step 2: Add Channel**\n\nForward a message from your channel to this chat now.\nI will detect the ID automatically.", 
                        [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)
    
    elif d.startswith("ch_"):
        cid = d.split("ch_")[1]
        await show_channel_options(uid, q.message, cid)
    
    elif d.startswith("rem_"):
        cid = d.split("rem_")[1]
        await del_channel(uid, cid)
        await q.answer("Channel Unlinked!")
        await show_channels(uid, q.message)

    elif d.startswith("new_"):
        cid = d.split("new_")[1]
        user_state[uid].update({"step": "waiting_content", "target": cid})
        await update_menu(q.message, "1️⃣ **Create Post**\n\nSend me the content you want to schedule:\n• Text / Photo / Video\n• Audio / Voice Note\n• Poll", 
                        [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)

    # --- WIZARD BACK LOGIC ---
    elif d == "step_time":
        await show_time_menu(q.message, uid)
    elif d == "step_rep":
        await ask_repetition(q.message, uid)
    elif d == "step_settings":
        await ask_settings(q.message, uid)

    # --- TIME ---
    elif d.startswith("time_"):
        offset = d.split("time_")[1] 
        if offset == "custom":
            user_state[uid]["step"] = "waiting_custom_date"
            cur_time = datetime.datetime.now(IST).strftime("%d-%b %I:%M %p")
            msg_txt = (f"📅 **Select Custom Date**\n\n"
                       f"Current Time: `{cur_time}`\n"
                       f"(Tap to copy)\n\n"
                       f"Please type the date and time in this format:\n"
                       f"`{cur_time}`")
            await update_menu(q.message, msg_txt, [[InlineKeyboardButton("🔙 Back", callback_data="step_time")]], uid)
            return

        now = datetime.datetime.now(IST)
        if offset == "0":
            run_time = now + datetime.timedelta(seconds=5)
        else:
            run_time = now + datetime.timedelta(minutes=int(offset))
            run_time = run_time.replace(second=0, microsecond=0)
        
        user_state[uid]["start_time"] = run_time
        await ask_repetition(q.message, uid)

    # --- REPEAT ---
    elif d.startswith("rep_"):
        val = d.split("rep_")[1]
        interval = None
        if val != "0": interval = f"minutes={val}"
        user_state[uid]["interval"] = interval
        await ask_settings(q.message, uid)

    # --- SETTINGS ---
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

    elif d.startswith("tasks_"):
        cid = d.split("tasks_")[1]
        await list_active_tasks(uid, q.message, cid)

# --- INPUTS ---

@app.on_message(filters.private & ~filters.command("manage") & ~filters.command("start"))
async def handle_inputs(c, m):
    uid = m.from_user.id
    text = m.text.strip() if m.text else ""

    # --- LOGIN LOGIC ---
    if uid in login_state:
        st = login_state[uid]
        if st["step"] == "waiting_phone":
            wait_msg = await m.reply("⏳ **Trying to connect!**\nThis can take up to 2 minutes.\n\nPlease wait...")
            try:
                temp = Client(":memory:", api_id=API_ID, api_hash=API_HASH)
                await temp.connect()
                sent = await temp.send_code(text)
                st.update({"client": temp, "phone": text, "hash": sent.phone_code_hash, "step": "waiting_code"})
                await wait_msg.delete()
                await update_menu(m, "📩 **Step 2: Verification Code**\n\n⚠️ **IMPORTANT:** To prevent code expiry, add `aa` before the code.\n\nIf code is `12345`, send: `aa12345`", None, uid, force_new=True)
            except Exception as e: 
                await wait_msg.delete()
                await m.reply(f"❌ Error: {e}\nTry /start again.")
        elif st["step"] == "waiting_code":
            try:
                real_code = text.lower().replace("aa", "").strip()
                await st["client"].sign_in(st["phone"], st["hash"], real_code)
                sess = await st["client"].export_session_string()
                await save_session(uid, sess)
                await st["client"].disconnect()
                del login_state[uid]
                if uid not in user_state: user_state[uid] = {}
                kb = [[InlineKeyboardButton("🚀 Start Managing", callback_data="menu_home")]]
                await update_menu(m, "✅ **Login Successful!**\n\nSetup complete.", kb, uid, force_new=True)
            except errors.SessionPasswordNeeded:
                st["step"] = "waiting_pass"
                await update_menu(m, "🔐 **Step 3: 2FA Password**\n\nEnter your cloud password.", None, uid, force_new=True)
            except Exception as e:
                await m.reply(f"❌ Error: {e}\nDid you add 'aa'?")
        elif st["step"] == "waiting_pass":
            try:
                await st["client"].check_password(text)
                sess = await st["client"].export_session_string()
                await save_session(uid, sess)
                await st["client"].disconnect()
                del login_state[uid]
                kb = [[InlineKeyboardButton("🚀 Start Managing", callback_data="menu_home")]]
                await update_menu(m, "✅ **Login Successful!**", kb, uid, force_new=True)
            except Exception as e:
                await m.reply(f"❌ Error: {e}")
        return

    # --- TASK LOGIC ---
    st = user_state.get(uid, {})
    step = st.get("step")

    if step == "waiting_forward":
        if m.forward_from_chat:
            chat = m.forward_from_chat
            await add_channel(uid, str(chat.id), chat.title)
            user_state[uid]["step"] = None
            await update_menu(m, f"✅ Added **{chat.title}**", [[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]], uid)
        else: 
            await m.reply("❌ Invalid Forward. Try again.")

    elif step == "waiting_content":
        content_type = "text"
        file_id = None
        content_text = m.text or m.caption or ""
        entities_json = None
        
        if m.poll:
            content_type = "poll"
            content_text = m.poll.question
            poll_data = {
                "options": [opt.text for opt in m.poll.options],
                "is_anonymous": m.poll.is_anonymous,
                "allows_multiple_answers": m.poll.allows_multiple_answers,
                "type": str(m.poll.type) 
            }
            entities_json = json.dumps(poll_data)
        else:
            raw_entities = m.entities or m.caption_entities
            entities_json = serialize_entities(raw_entities)
            
            # Media Detection
            media = m.media
            if media == enums.MessageMediaType.PHOTO:
                content_type = "photo"
                file_id = m.photo.file_id
            elif media == enums.MessageMediaType.VIDEO:
                content_type = "video"
                file_id = m.video.file_id
            elif media == enums.MessageMediaType.AUDIO:
                content_type = "audio"
                file_id = m.audio.file_id
            elif media == enums.MessageMediaType.VOICE:
                content_type = "voice"
                file_id = m.voice.file_id
            elif media == enums.MessageMediaType.DOCUMENT:
                content_type = "document"
                file_id = m.document.file_id
            elif media == enums.MessageMediaType.STICKER:
                content_type = "sticker"
                file_id = m.sticker.file_id
            elif media == enums.MessageMediaType.ANIMATION:
                content_type = "animation"
                file_id = m.animation.file_id
        
        if content_type != "text" and content_type != "poll" and not file_id:
            await m.reply("❌ **Error:** Media ID missing. Please forward the file.")
            return

        st.update({
            "content_type": content_type,
            "content_text": content_text,
            "file_id": file_id,
            "entities": entities_json, 
            "step": "waiting_time"
        })
        user_state[uid] = st
        await show_time_menu(m, uid, force_new=True)

    elif step == "waiting_custom_date":
        try:
            current_year = datetime.datetime.now(IST).year
            full_str = f"{current_year}-{text}"
            dt = datetime.datetime.strptime(full_str, "%Y-%d-%b %I:%M %p")
            dt = IST.localize(dt)
            user_state[uid]["start_time"] = dt
            await ask_repetition(m, uid, force_new=True) 
        except: 
            await m.reply("❌ Invalid Format. Use: `04-Feb 12:30 PM`")

# --- UI MENUS ---

async def show_main_menu(m, uid, force_new=False):
    kb = [[InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")],
          [InlineKeyboardButton("🚪 Logout", callback_data="logout")]]
    await update_menu(m, "👋 **Manager Dashboard**", kb, uid, force_new)

async def show_channels(uid, m, force_new=False):
    chs = await get_channels(uid)
    if not chs:
        await update_menu(m, "❌ No channels.", [[InlineKeyboardButton("➕ Add One", callback_data="add_channel")]], uid, force_new)
        return
    kb = []
    for c in chs: kb.append([InlineKeyboardButton(c['title'], callback_data=f"ch_{c['channel_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await update_menu(m, "👇 **Select a Channel:**", kb, uid, force_new)

async def show_channel_options(uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    kb = [
        [InlineKeyboardButton("✍️ Schedule Post", callback_data=f"new_{cid}")],
        [InlineKeyboardButton(f"📅 Scheduled ({len(tasks)})", callback_data=f"tasks_{cid}")],
        [InlineKeyboardButton("🗑 Unlink", callback_data=f"rem_{cid}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ]
    await update_menu(m, f"⚙️ **Managing Channel**", kb, uid, force_new)

async def show_time_menu(m, uid, force_new=False):
    cid = user_state[uid].get("target")
    kb = [
        [InlineKeyboardButton("🚀 Post Now", callback_data="time_0")],
        [InlineKeyboardButton("⏱️ +15 Mins", callback_data="time_15"), InlineKeyboardButton("🕐 +1 Hour", callback_data="time_60")],
        [InlineKeyboardButton("📅 Custom Date", callback_data="time_custom")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")] 
    ]
    await update_menu(m, "2️⃣ **When to post?**", kb, uid, force_new)

async def ask_repetition(m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("🚫 No Repeat", callback_data="rep_0")],
        [InlineKeyboardButton("🔁 5 Mins", callback_data="rep_5"), InlineKeyboardButton("🔁 30 Mins", callback_data="rep_30")],
        [InlineKeyboardButton("🔁 Hourly", callback_data="rep_60"), InlineKeyboardButton("🔁 6 Hours", callback_data="rep_360")],
        [InlineKeyboardButton("🔁 Daily", callback_data="rep_1440")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_time")] 
    ]
    st = user_state[uid]
    time_str = st["start_time"].strftime("%d-%b %I:%M %p")
    await update_menu(m, f"3️⃣ **Repeat?**\nSelected Time: `{time_str}`", kb, uid, force_new)

async def ask_settings(m, uid, force_new=False):
    st = user_state[uid]
    st.setdefault("pin", True)
    st.setdefault("del", True)
    
    pin_icon = "✅" if st["pin"] else "❌"
    del_icon = "✅" if st["del"] else "❌"
    
    kb = [
        [InlineKeyboardButton(f"📌 Pin Msg: {pin_icon}", callback_data="toggle_pin")],
        [InlineKeyboardButton(f"🗑 Del Old: {del_icon}", callback_data="toggle_del")],
        [InlineKeyboardButton("➡️ Confirm", callback_data="goto_confirm")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_rep")]
    ]
    await update_menu(m, "4️⃣ **Settings**", kb, uid, force_new)

async def confirm_task(m, uid, force_new=False):
    st = user_state[uid]
    t_str = st["start_time"].strftime("%d-%b %I:%M %p")
    r_str = st["interval"] if st["interval"] else "Once"
    
    type_map = {
        "text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
        "audio": "🎵 Audio", "voice": "🎙 Voice", "document": "📁 File",
        "poll": "📊 Poll", "animation": "🎞 GIF", "sticker": "✨ Sticker"
    }
    type_str = type_map.get(st['content_type'], st['content_type'].upper())
    
    txt = (f"✅ **Summary**\n\n"
           f"📢 Content: {type_str}\n"
           f"📅 Time: `{t_str}`\n"
           f"🔁 Repeat: `{r_str}`\n"
           f"📌 Pin: {'✅' if st['pin'] else '❌'} | 🗑 Del: {'✅' if st['del'] else '❌'}")
    
    kb = [[InlineKeyboardButton("✅ Schedule It", callback_data="save_task")],
          [InlineKeyboardButton("🔙 Back", callback_data="step_settings")]]
    
    await update_menu(m, txt, kb, uid, force_new)

async def list_active_tasks(uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    if not tasks:
        await update_menu(m, "✅ No active tasks.", [[InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")]], uid, force_new)
        return
    
    tasks.sort(key=lambda x: x['start_time'])
    txt = "**📅 Scheduled Tasks:**\nSelect one to manage:"
    kb = []
    
    type_icons = {"text": "📝", "photo": "📷", "video": "📹", "audio": "🎵", "poll": "📊"}
    
    for t in tasks:
        snippet = (t['content_text'] or "Media")[:15] + "..."
        icon = type_icons.get(t['content_type'], "📁")
        try:
            dt = datetime.datetime.fromisoformat(t["start_time"])
            time_str = dt.strftime('%I:%M %p') 
        except: time_str = "?"
        
        btn_text = f"{icon} {snippet} | ⏰ {time_str}"
        kb.append([InlineKeyboardButton(btn_text, callback_data=f"view_{t['task_id']}")])
        
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")])
    await update_menu(m, txt, kb, uid, force_new)

async def show_task_details(uid, m, tid):
    t = await get_single_task(tid)
    if not t:
        await update_menu(m, "❌ Task not found.", [[InlineKeyboardButton("🏠 Home", callback_data="menu_home")]], uid)
        return

    dt = datetime.datetime.fromisoformat(t["start_time"])
    time_str = dt.strftime('%d-%b %I:%M %p')
    type_map = {"text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video", "audio": "🎵 Audio", "poll": "📊 Poll"}
    type_str = type_map.get(t['content_type'], "📁 File")
    
    txt = (f"⚙️ **Managing Task**\n\n"
           f"📝 **Snippet:** `{t['content_text'][:50]}...`\n"
           f"📂 **Type:** {type_str}\n"
           f"📅 **Time:** `{time_str}`\n"
           f"🔁 **Repeat:** `{t['repeat_interval'] or 'No'}`\n\n"
           f"👇 **Select Action:**")

    kb = [
        # [InlineKeyboardButton("👁️ Preview Msg", callback_data=f"prev_{tid}")], # Optional
        [InlineKeyboardButton("🗑 Delete Task", callback_data=f"del_task_{tid}")],
        [InlineKeyboardButton("🔙 Back to List", callback_data=f"back_list_{t['chat_id']}")]
    ]
    await update_menu(m, txt, kb, uid)

# --- WORKER ---
async def create_task_logic(uid, q):
    st = user_state[uid]
    tid = f"task_{int(datetime.datetime.now().timestamp())}"
    
    t_str = st["start_time"].strftime("%d-%b %I:%M %p")
    chs = await get_channels(uid)
    ch_title = "Channel"
    for c in chs:
        if c['channel_id'] == st['target']:
            ch_title = c['title']
            break

    task_data = {
        "task_id": tid,
        "owner_id": uid,
        "chat_id": st["target"],
        "content_type": st["content_type"],
        "content_text": st["content_text"],
        "file_id": st["file_id"],
        "entities": st.get("entities"), 
        "pin": st["pin"],
        "delete_old": st["del"],
        "repeat_interval": st["interval"],
        "start_time": st["start_time"].isoformat(),
        "last_msg_id": None
    }
    
    try:
        await save_task(task_data)
        add_scheduler_job(tid, task_data)
        
        final_txt = (f"🎉 **Scheduled Successfully!**\n\n"
                     f"📢 **Channel:** `{ch_title}`\n"
                     f"📅 **Time:** `{t_str}`\n"
                     f"🔁 **Repeat:** `{st['interval'] or 'No'}`\n\n"
                     f"👉 Click /manage to schedule more.")
        
        await update_menu(q.message, final_txt, None, uid, force_new=True)
    except Exception as e:
        logger.error(f"Save Error: {e}")
        await q.message.edit_text(f"❌ Error: {e}")

def add_scheduler_job(tid, t):
    if scheduler is None: return

    async def job_func():
        async with queue_lock:
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
                    try: await user.get_chat(target)
                    except:
                        async for dialog in user.get_dialogs(limit=200):
                            if dialog.chat.id == target: break
                    
                    real_last_msg_id = await get_task_last_msg_id(t["task_id"])
                    
                    if t["delete_old"] and real_last_msg_id:
                        try: await user.delete_messages(target, int(real_last_msg_id))
                        except: pass
                    
                    sent = None
                    caption = t["content_text"]
                    
                    if t["content_type"] == "poll":
                        poll_cfg = json.loads(t["entities"])
                        try:
                            sent = await user.send_poll(
                                chat_id=target,
                                question=caption,
                                options=poll_cfg["options"],
                                is_anonymous=poll_cfg["is_anonymous"],
                                allows_multiple_answers=poll_cfg["allows_multiple_answers"]
                            )
                        except Exception as e: logger.error(f"Poll Error: {e}")
                    else:
                        entities_objs = deserialize_entities(t["entities"])
                        try:
                            if t["content_type"] == "text":
                                sent = await user.send_message(target, t["content_text"], entities=entities_objs)
                            elif t["content_type"] == "photo":
                                sent = await user.send_photo(target, t["file_id"], caption=caption, caption_entities=entities_objs)
                            elif t["content_type"] == "video":
                                sent = await user.send_video(target, t["file_id"], caption=caption, caption_entities=entities_objs)
                            # ✅ FIXED: Correct Logic for Audio/Voice
                            elif t["content_type"] in ["audio", "voice"]:
                                try:
                                    logger.info(f"📥 Downloading {t['content_type']}...")
                                    file_bytes = await app.download_media(t["file_id"], in_memory=True)
                                    media_file = BytesIO(file_bytes) # Wrap in BytesIO
                                    
                                    if t["content_type"] == "voice":
                                        media_file.name = "voice.ogg"
                                        sent = await user.send_voice(target, media_file, caption=caption)
                                    else:
                                        media_file.name = "audio.mp3"
                                        sent = await user.send_audio(target, media_file, caption=caption, caption_entities=entities_objs)
                                except Exception as e:
                                    logger.error(f"❌ Audio/Voice Upload Error: {e}")

                            elif t["content_type"] == "document":
                                sent = await user.send_document(target, t["file_id"], caption=caption, caption_entities=entities_objs)
                            elif t["content_type"] == "sticker":
                                sent = await user.send_sticker(target, t["file_id"])
                            elif t["content_type"] == "animation":
                                sent = await user.send_animation(target, t["file_id"], caption=caption, caption_entities=entities_objs)
                        except Exception as e: logger.error(f"Send Error: {e}")

                    if sent:
                        logger.info(f"✅ Job {tid}: Message Sent! ID: {sent.id}")
                        if t["pin"]:
                            try: 
                                pinned = await sent.pin()
                                if isinstance(pinned, Message): await pinned.delete()
                                await asyncio.sleep(0.5)
                                await user.delete_messages(target, sent.id + 1)
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

# --- STARTUP ---
async def main():
    global queue_lock
    queue_lock = asyncio.Lock() # Init lock
    await init_db()
    
    executors = { 'default': AsyncIOExecutor() }
    global scheduler
    scheduler = AsyncIOScheduler(timezone=IST, event_loop=asyncio.get_running_loop(), executors=executors)
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
