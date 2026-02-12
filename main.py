import os
import logging
import asyncio
import datetime
import pytz
import asyncpg
import json
from io import BytesIO
from pyrogram import Client, filters, idle, errors, enums
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    Message, MessageEntity,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL") 

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ManagerBot")

# --- INIT ---
app = Client("manager_v32_master", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
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
        # 1. Create the table with ALL columns included from the start
        # This is cleaner and faster than creating then altering.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS userbot_tasks_v11 (
                task_id TEXT PRIMARY KEY, 
                owner_id BIGINT, 
                chat_id TEXT, 
                content_type TEXT, 
                content_text TEXT, 
                file_id TEXT, 
                entities TEXT, 
                pin BOOLEAN DEFAULT FALSE, 
                delete_old BOOLEAN DEFAULT FALSE, 
                repeat_interval TEXT, 
                start_time TEXT, 
                last_msg_id BIGINT,
                auto_delete_offset INTEGER DEFAULT 0,
                reply_target TEXT
            );
        ''')
        
        # 2. SEAMLESS MIGRATION (The "Self-Healing" logic)
        # If you ever change the table name or add columns later, 
        # these 'IF NOT EXISTS' alterations ensure existing DBs don't break.
        migrations = [
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS auto_delete_offset INTEGER DEFAULT 0",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS reply_target TEXT"
        ]
        
        for query in migrations:
            try:
                await conn.execute(query)
            except Exception as e:
                logger.warning(f"⚠️ Migration note (likely already exists): {e}")

    logger.info("📡 Database initialized: userbot_tasks_v11 is ready.")

async def migrate_to_v11():
    pool = await get_db()
    async with pool.acquire() as conn:
        logger.info("🔄 [MIGRATION] Checking for legacy data in 'tasks' table...")
        
        # Check if the old table exists before trying to move data
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'tasks'
            );
        """)
        
        if table_exists:
            try:
                # This query copies all data and ignores duplicates if you've already run it
                result = await conn.execute('''
                    INSERT INTO userbot_tasks_v11 (
                        task_id, owner_id, chat_id, content_type, content_text, 
                        file_id, entities, pin, delete_old, repeat_interval, 
                        start_time, last_msg_id
                    )
                    SELECT 
                        task_id, owner_id, chat_id, content_type, content_text, 
                        file_id, entities, pin, delete_old, repeat_interval, 
                        start_time, last_msg_id
                    FROM tasks
                    ON CONFLICT (task_id) DO NOTHING;
                ''')
                logger.info(f"✅ [MIGRATION] Success: {result}")
                
                # OPTIONAL: Rename the old table so we don't try to migrate again
                # await conn.execute("ALTER TABLE tasks RENAME TO tasks_legacy_backup;")
                
            except Exception as e:
                logger.error(f"❌ [MIGRATION] Failed to move data: {e}")
        else:
            logger.info("ℹ️ [MIGRATION] No legacy 'tasks' table found. Skipping.")

async def delete_sent_message(owner_id, chat_id, message_id):
    """
    Independent worker to delete a message.
    Fetches the user's client to ensure it's still connected.
    """
    try:
        client = user_clients.get(owner_id)
        if client and client.is_connected:
            await client.delete_messages(chat_id, message_id)
            logger.info(f"🗑️ Auto-delete success: Msg {message_id} in {chat_id}")
    except Exception as e:
        logger.error(f"❌ Auto-delete failed: {e}")

async def save_task(t):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_tasks_v11 (task_id, owner_id, chat_id, content_type, content_text, file_id, entities, pin, delete_old, repeat_interval, start_time, last_msg_id, reply_target)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (task_id) DO UPDATE SET last_msg_id = $12, start_time = $11
    """, t['task_id'], t['owner_id'], t['chat_id'], t['content_type'], t['content_text'], t['file_id'], 
       t['entities'], t['pin'], t['delete_old'], t['repeat_interval'], t['start_time'], t['last_msg_id'], 
       t.get('reply_target'))

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

async def delete_all_user_data(user_id):
    pool = await get_db()
    
    # 1. Try to Terminate Telegram Session (Max 5 Seconds)
    session_str = await get_session(user_id)
    if session_str:
        try:
            async def fast_logout():
                # 👇 CHANGED: Added Custom Device & App Names here
                async with Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session_str,
                                  device_model="AutoCast Client", 
                                  system_version="PC",
                                  app_version="AutoCast Version") as temp_user:
                    await temp_user.log_out()
            
            # Force Timeout to prevent hanging
            await asyncio.wait_for(fast_logout(), timeout=5.0)
            logger.info(f"✅ User {user_id} session terminated.")
            
        except Exception as e:
            logger.warning(f"⚠️ Session kill skipped (Error/Timeout): {e}")

    # 2. Stop scheduler jobs
    tasks = await pool.fetch("SELECT task_id FROM userbot_tasks_v11 WHERE owner_id = $1", user_id)
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t['task_id'])
            except: pass
            
    # 3. Delete Everything from DB (Fixed Column Name)
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE owner_id = $1", user_id)
    
    # 👇 Changed 'owner_id' to 'user_id' because that is the column name in this table
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1", user_id) 
    
    await pool.execute("DELETE FROM userbot_sessions WHERE user_id = $1", user_id)

    # 4. Clear Memory Cache
    if user_id in user_state: del user_state[user_id]
    if user_id in login_state: del login_state[user_id]
        
async def get_channels(user_id):
    pool = await get_db()
    return await pool.fetch("SELECT * FROM userbot_channels WHERE user_id = $1", user_id)

async def del_channel(user_id, cid):
    pool = await get_db() 
    # 1. Find all tasks scheduled for this channel
    tasks = await pool.fetch("SELECT task_id FROM userbot_tasks_v11 WHERE chat_id = $1", cid)
    # 2. Stop them in the Scheduler (Stop sending messages)
    if scheduler:
        for t in tasks:
            try: 
                scheduler.remove_job(t['task_id'])
            except: pass 
    # 3. Delete the Tasks from the Database
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE chat_id = $1", cid)
    # 4. Finally, Delete the Channel itself
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1 AND channel_id = $2", user_id, cid)

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

async def get_delete_before_kb(task_id, repeat_mins):
    """
    Generates deletion options based on the repetition interval.
    """
    # Options in minutes: 1h, 6h, 12h, 1d, 2d, 7d
    options = [
        ("1 Hour", 60), ("6 Hours", 360), ("12 Hours", 720), 
        ("1 Day", 1440), ("2 Days", 2880), ("7 Days", 10080)
    ]
    
    buttons = []
    row = []
    
    for label, mins in options:
        # Only show options that are shorter than the repetition time
        if mins < repeat_mins:
            row.append(InlineKeyboardButton(f"⏳ {label} Before", callback_data=f"set_del_off_{task_id}_{mins}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row: buttons.append(row)
    
    buttons.append([InlineKeyboardButton("❌ Don't Delete", callback_data=f"set_del_off_{task_id}_0")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"edit_task_{task_id}")])
    
    return InlineKeyboardMarkup(buttons)

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
        kb = [
            [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
            [InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")],
            [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
        ]
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

async def show_broadcast_selection(uid, m):
    chs = await get_channels(uid)
    if not chs:
        await update_menu(m, "❌ No channels found.", [[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]], uid)
        return

    targets = user_state[uid].get("broadcast_targets", [])
    kb = []
    
    # Create Toggle Buttons
    for c in chs:
        is_selected = c['channel_id'] in targets
        icon = "✅" if is_selected else "⬜"
        kb.append([InlineKeyboardButton(f"{icon} {c['title']}", callback_data=f"toggle_bc_{c['channel_id']}")])
    
    # Done Button
    kb.append([InlineKeyboardButton(f"➡️ Done ({len(targets)} Selected)", callback_data="broadcast_confirm")])
    kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")])
    
    await update_menu(m, "📢 **Broadcast Mode**\n\nSelect channels to post to:", kb, uid)

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
    
    # --- 3-STEP SECURE LOGOUT ---

    # Step 1: First Warning
    elif d == "logout":
        # Check how many tasks they have for the warning
        tasks = await get_all_tasks() # Note: Better to filter by user in DB, but this works for simple count
        user_tasks = [t for t in tasks if t['owner_id'] == uid]
        
        txt = (f"⚠️ **Wait! Are you sure?**\n\n"
               f"You have **{len(user_tasks)} active tasks** scheduled.\n"
               f"If you logout, the bot will stop working.")
        
        kb = [[InlineKeyboardButton("⚠️ Yes, I want to Logout", callback_data="logout_step_2")],
              [InlineKeyboardButton("🔙 No, Cancel", callback_data="menu_home")]]
        
        await update_menu(q.message, txt, kb, uid)

    # Step 2: Final "Danger" Warning
    elif d == "logout_step_2":
        txt = ("🛑 **FINAL WARNING** 🛑\n\n"
               "This will **PERMANENTLY DELETE** all your scheduled posts and settings.\n"
               "This action cannot be undone.\n\n"
               "Are you absolutely sure?")
        
        kb = [[InlineKeyboardButton("🗑️ Delete Everything & Logout", callback_data="logout_final")],
              [InlineKeyboardButton("🔙 No! Go Back", callback_data="menu_home")]]
        
        await update_menu(q.message, txt, kb, uid)

    # Step 3: Execution
    elif d == "logout_final":
        # 1. Immediate Feedback
        try:
            await app.edit_message_text(uid, q.message.id, "⏳ **Logging out...**\nTerminating session and wiping data.")
        except: 
            await q.answer("⏳ Processing...", show_alert=False)

        # 2. Do the heavy lifting (Safe version)
        await delete_all_user_data(uid) 
        
        # 3. Final Success Message
        try:
            await app.edit_message_text(
                chat_id=uid, 
                message_id=q.message.id, 
                text="👋 **Logged out successfully.**\n\nAll data has been wiped and your active session has been terminated."
            )
        except:
            await app.send_message(uid, "👋 **Logged out successfully.**")

    # --- TASK ACTIONS (PRO UI) ---
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

    # --- BROADCAST MODE ---
    elif d == "broadcast_start":
        # Initialize empty target list
        user_state[uid]["broadcast_targets"] = []
        user_state[uid]["step"] = "broadcast_select"
        await show_broadcast_selection(uid, q.message)

    elif d.startswith("toggle_bc_"):
        cid = d.split("toggle_bc_")[1]
        targets = user_state[uid].get("broadcast_targets", [])
        
        if cid in targets: targets.remove(cid) # Deselect
        else: targets.append(cid) # Select
        
        user_state[uid]["broadcast_targets"] = targets
        await show_broadcast_selection(uid, q.message) # Refresh menu to show ✅

    elif d == "broadcast_confirm":
        targets = user_state[uid].get("broadcast_targets", [])
        if not targets:
            await q.answer("❌ Select at least one channel!", show_alert=True)
            return
        
        # Initialize the Queue
        user_state[uid]["broadcast_queue"] = [] 
        user_state[uid]["step"] = "waiting_broadcast_content"
        
        # Show Persistent "DONE" Button
        markup = ReplyKeyboardMarkup(
            [[KeyboardButton("✅ Done Adding Posts")], [KeyboardButton("❌ Cancel")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        
        # 👇 Detailed User Guide
        guide_text = (
            f"📢 **Multi-Post Mode Active**\n"
            f"Selected: **{len(targets)} Channels**\n\n"
            
            f"👇 **How to Use:**\n"
            f"1️⃣ **Send Posts:** Send text, photos, or videos one by one.\n"
            f"2️⃣ **Create Threads:** If you want Post B to reply to Post A, simply **reply to Post A** right here!\n"
            f"3️⃣ **Finish:** Click **✅ Done** when finished.\n\n"
            
            f"⚙️ *You can configure Pin/Delete settings for each post individually after adding them.*"
        )
        
        await app.send_message(q.message.chat.id, guide_text, reply_markup=markup)

    # --- CHANNEL MANAGEMENT ---
    elif d == "list_channels":
        await show_channels(uid, q.message)
    
    elif d == "add_channel":
        user_state[uid]["step"] = "waiting_forward"
        # ✅ FIX: Pass 'None' as the 3rd argument to show NO buttons
        await update_menu(q.message, 
                          "📝 **Step 2: Add Channel**\n\nForward a message from your channel to this chat now.\nI will detect the ID automatically.", 
                          None, 
                          uid)
    
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
    # --- WIZARD: HANDLE AUTO-DELETE OFFSET ---
    elif d.startswith("wizard_ask_offset"):
        # 1. Check if Repetition is enabled (Required for Auto-Delete)
        interval = user_state[uid].get("interval")
        if not interval:
            await q.answer("⚠️ You must set a Repeat Interval first!", show_alert=True)
            return

        repeat_mins = int(interval.split("=")[1])
        
        # 2. Check if this is for a specific post in a Batch (e.g., "wizard_ask_offset_2")
        parts = d.split("_")
        is_batch = len(parts) > 3 
        # ID logic: If batch, ID is "WIZARD_2" (index 2). If single, ID is "WIZARD"
        temp_task_id = f"WIZARD_{parts[3]}" if is_batch else "WIZARD"
        
        # 3. Generate the keyboard
        # We reuse the existing keyboard builder!
        markup = await get_delete_before_kb(temp_task_id, repeat_mins)
        
        await update_menu(
            q.message, 
            f"⏳ **Select Auto-Delete Time**\n\n"
            f"Repeat Interval: Every {repeat_mins} mins.\n"
            f"When should this post be deleted?", 
            markup.inline_keyboard, 
            uid
        )

    # --- WIZARD: SAVE THE OFFSET ---
    elif d.startswith("set_del_off_WIZARD"):
        # Format can be: 
        # Single: set_del_off_WIZARD_60
        # Batch:  set_del_off_WIZARD_2_60
        
        parts = d.split("_")
        
        if len(parts) == 5:
            # SINGLE MODE
            offset = int(parts[4])
            user_state[uid]["auto_delete_offset"] = offset
            await q.answer(f"✅ Auto-Delete set to {offset}m!")
            
        elif len(parts) == 6:
            # BATCH MODE
            idx = int(parts[4])
            offset = int(parts[5])
            # Save to the specific post in the queue
            if "broadcast_queue" in user_state[uid]:
                user_state[uid]["broadcast_queue"][idx]["auto_delete_offset"] = offset
            await q.answer(f"✅ Post #{idx+1} auto-delete set to {offset}m!")

        # Return to Settings Menu
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

    # --- PER-POST SETTINGS HANDLER ---
    elif d.startswith("cfg_q_"):
        idx = int(d.split("cfg_q_")[1])
        post = user_state[uid]["broadcast_queue"][idx]
        
        # Display current state
        p_stat = "Enabled ✅" if post["pin"] else "Disabled ❌"
        d_stat = "Enabled ✅" if post["delete_old"] else "Disabled ❌"
        
        txt = (f"⚙️ **Configuring Post #{idx+1}**\n\n"
               f"📂 Type: **{post['content_type']}**\n"
               f"📌 Pin this post? **{p_stat}**\n"
               f"🗑 Delete previous? **{d_stat}**")
               
        kb = [
            [InlineKeyboardButton(f"📌 Toggle Pin", callback_data=f"t_q_pin_{idx}")],
            [InlineKeyboardButton(f"🗑 Toggle Delete", callback_data=f"t_q_del_{idx}")],
            [InlineKeyboardButton("⏰ Set Delete Before", callback_data=f"wizard_ask_offset_{idx}")],
            [InlineKeyboardButton("🔙 Back to List", callback_data="step_settings")]
        ]
        await update_menu(q.message, txt, kb, uid)

    elif d.startswith("t_q_"):
        # Format: t_q_pin_0 (action_index)
        parts = d.split("_")
        action = parts[2] # "pin" or "del"
        idx = int(parts[3])
        
        post = user_state[uid]["broadcast_queue"][idx]
        
        # Toggle Logic
        if action == "pin": post["pin"] = not post["pin"]
        if action == "del": post["delete_old"] = not post["delete_old"]
        
        # Re-open the specific menu to show update
        # (We basically redirect to 'cfg_q_IDX')
        q.data = f"cfg_q_{idx}"
        await callback_router(c, q)

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
                # 👇 CHANGED: Added Custom Device & App Names here
                temp = Client(":memory:", api_id=API_ID, api_hash=API_HASH, 
                              device_model="AutoCast Client",
                              system_version="PC", 
                              app_version="AutoCast Version")
                
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
            await update_menu(m, f"✅ Added **{chat.title}**", [[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]], uid, force_new=True)
        else: 
            await m.reply("❌ Invalid Forward. Try again.")

    # --- MULTI-POST BROADCAST LOGIC ---
    elif step == "waiting_broadcast_content":
        # 1. Handle "Done" Button
        if text == "✅ Done Adding Posts":
            queue = st.get("broadcast_queue", [])
            if not queue:
                await m.reply("❌ You haven't added any posts yet!", quote=True)
                return
            
            # Move to Scheduling Step
            st["step"] = "waiting_time" 
            
            await m.reply(
                f"✅ **Batch Created:** {len(queue)} Posts captured.\nSelect time below:", 
                reply_markup=ReplyKeyboardRemove()
            )
            await show_time_menu(m, uid, force_new=True)
            return

        # 2. Handle "Cancel"
        if text == "❌ Cancel":
            user_state[uid]["step"] = None
            if "broadcast_queue" in user_state[uid]: del user_state[uid]["broadcast_queue"]
            await m.reply("🚫 Broadcast Cancelled.", reply_markup=ReplyKeyboardRemove())
            await show_main_menu(m, uid, force_new=True)
            return

        # 3. Capture Content
        content_type = "text"
        file_id = None
        content_text = m.text or m.caption or ""
        entities_json = None
        
        if m.poll:
            content_type = "poll"
            content_text = m.poll.question
            poll_data = { "options": [o.text for o in m.poll.options], "is_anonymous": m.poll.is_anonymous, "allows_multiple_answers": m.poll.allows_multiple_answers, "type": str(m.poll.type) }
            entities_json = json.dumps(poll_data)
        else:
            raw_entities = m.entities or m.caption_entities
            entities_json = serialize_entities(raw_entities)
            media = m.media
            if media:
                content_type = media.value 
                if media == enums.MessageMediaType.PHOTO: file_id = m.photo.file_id
                elif media == enums.MessageMediaType.VIDEO: file_id = m.video.file_id
                elif media == enums.MessageMediaType.AUDIO: file_id = m.audio.file_id
                elif media == enums.MessageMediaType.VOICE: file_id = m.voice.file_id
                elif media == enums.MessageMediaType.DOCUMENT: file_id = m.document.file_id
                elif media == enums.MessageMediaType.ANIMATION: file_id = m.animation.file_id
        
        if content_type != "text" and not file_id and content_type != "poll":
            await m.reply("❌ Unsupported media. Send Text, Photo, Video, Audio, or Document.")
            return

        # Check reply reference
        reply_ref_id = None
        if m.reply_to_message:
            reply_ref_id = m.reply_to_message.id

        # Add to Queue
        post_data = {
            "content_type": content_type,
            "content_text": content_text,
            "file_id": file_id,
            "entities": entities_json,
            "pin": True,
            "delete_old": True,
            "input_msg_id": m.id,      
            "reply_ref_id": reply_ref_id 
        }
        st.setdefault("broadcast_queue", []).append(post_data)
        
        await m.reply(f"✅ **Post #{len(st['broadcast_queue'])} Added!**\nSend next or click Done.", quote=True)
    
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
    kb = [
        [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
        [InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")],
        [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
    ]
    await update_menu(m, "👋 **Manager Dashboard**", kb, uid, force_new)

async def show_channels(uid, m, force_new=False):
    chs = await get_channels(uid)
    if not chs:
        kb = [
            [InlineKeyboardButton("➕ Add One", callback_data="add_channel")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_home")]
        ]
        await update_menu(m, "❌ No channels.", kb, uid, force_new)
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
        [InlineKeyboardButton("🔁 Daily", callback_data="rep_1440"), InlineKeyboardButton("🔁 2 Days", callback_data="rep_2880")],
        [InlineKeyboardButton("🔁 7 Days", callback_data="rep_10080")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_time")] 
    ]
    st = user_state[uid]
    time_str = st["start_time"].strftime("%d-%b %I:%M %p")
    await update_menu(m, f"3️⃣ **Repeat?**\nSelected Time: `{time_str}`", kb, uid, force_new)

async def ask_settings(m, uid, force_new=False):
    st = user_state[uid]
    queue = st.get("broadcast_queue")

    # --- CASE 1: BATCH MODE ---
    if queue:
        txt = ("4️⃣ **Batch Post Settings**\n\n"
               "**Legend:**\n"
               "📌 **Pin:** Pin message.\n"
               "🗑 **Del:** Delete previous post.\n"
               "⏰ **Off:** Minutes to delete *before* next post.\n\n"
               "👇 **Configure individual post settings:**")
        
        kb = []
        for i, post in enumerate(queue):
            p_stat = "ON" if post.get("pin") else "OFF"
            d_stat = "ON" if post.get("delete_old") else "OFF"
            # Show the offset in the button label
            offset = post.get("auto_delete_offset", 0)
            off_stat = f"{offset}m" if offset > 0 else "OFF"
            
            # Label format: ✅ Post #1 | P: ON | D: ON | Off: 60m
            btn_txt = f"✅ Post #{i+1} | P: {p_stat} | D: {d_stat} | ⏰ {off_stat}"
            
            kb.append([InlineKeyboardButton(btn_txt, callback_data=f"cfg_q_{i}")])
        
        kb.append([InlineKeyboardButton("➡️ Confirm All", callback_data="goto_confirm")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="step_rep")])
        
        await update_menu(m, txt, kb, uid, force_new)
        return

    # --- CASE 2: SINGLE POST MODE ---
    st.setdefault("pin", True)
    st.setdefault("del", True)
    # Default offset to 0 if not present
    offset = st.get("auto_delete_offset", 0)
    
    pin_icon = "✅" if st["pin"] else "❌"
    del_icon = "✅" if st["del"] else "❌"
    
    # Text for the deletion button
    off_text = f"⏰ Delete: {offset}m Before Next" if offset > 0 else "⏰ Auto-Delete: OFF"
    
    kb = [
        [InlineKeyboardButton(f"📌 Pin Msg: {pin_icon}", callback_data="toggle_pin")],
        [InlineKeyboardButton(f"🗑 Del Old: {del_icon}", callback_data="toggle_del")],
        # 👇 NEW BUTTON FOR SINGLE MODE 👇
        [InlineKeyboardButton(off_text, callback_data="wizard_ask_offset")],
        [InlineKeyboardButton("➡️ Confirm", callback_data="goto_confirm")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_rep")]
    ]
    
    msg_text = (f"4️⃣ **Settings**\n\n"
                f"Configure how your post behaves.\n"
                f"Auto-delete is currently: **{offset} minutes** before next run.")
                
    await update_menu(m, msg_text, kb, uid, force_new)
    
async def confirm_task(m, uid, force_new=False):
    st = user_state[uid]
    t_str = st["start_time"].strftime("%d-%b %I:%M %p")
    r_str = st["interval"] if st["interval"] else "Once"
    
    queue = st.get("broadcast_queue")
    
    if queue:
        # BATCH SUMMARY
        type_str = f"📦 Batch ({len(queue)} Posts)"
        # Calculate how many are pinned
        pin_count = sum(1 for p in queue if p['pin'])
        settings_str = f"📌 Pinning: {pin_count}/{len(queue)} Posts"
    else:
        # SINGLE SUMMARY
        type_map = {
            "text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
            "audio": "🎵 Audio", "voice": "🎙 Voice", "document": "📁 File",
            "poll": "📊 Poll", "animation": "🎞 GIF", "sticker": "✨ Sticker"
        }
        c_type = st.get('content_type', 'unknown')
        type_str = type_map.get(c_type, c_type.upper())
        
        settings_str = f"📌 Pin: {'✅' if st.get('pin',True) else '❌'} | 🗑 Del: {'✅' if st.get('del',True) else '❌'}"
    
    txt = (f"✅ **Summary**\n\n"
           f"📢 Content: {type_str}\n"
           f"📅 Time: `{t_str}`\n"
           f"🔁 Repeat: `{r_str}`\n"
           f"{settings_str}")
    
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
    targets = st.get("broadcast_targets", [st.get("target")])
    queue = st.get("broadcast_queue")

    # Single post fallback
    if not queue:
        queue = [{
            "content_type": st["content_type"],
            "content_text": st["content_text"],
            "file_id": st["file_id"],
            "entities": st.get("entities"),
            "input_msg_id": 0, "reply_ref_id": None,
            # Ensure single post inherits the offset from the wizard step
            "auto_delete_offset": st.get("auto_delete_offset", 0) 
        }]

    base_tid = int(datetime.datetime.now().timestamp())
    t_str = st["start_time"].strftime("%d-%b %I:%M %p")
    total_tasks = 0

    # Loop Channels
    for ch_idx, cid in enumerate(targets):
        
        # 1. First Pass: Map Input IDs to Task IDs
        batch_map = {} 
        for post_idx, post in enumerate(queue):
            tid = f"task_{base_tid}_{ch_idx}_{post_idx}"
            if "input_msg_id" in post:
                batch_map[post["input_msg_id"]] = tid

        # 2. Second Pass: Create Tasks
        for post_idx, post in enumerate(queue):
            tid = f"task_{base_tid}_{ch_idx}_{post_idx}"
            
            # 👇 FIX: Increased delay to 10 seconds to prevent Reply Race Conditions
            run_time = st["start_time"] + datetime.timedelta(seconds=post_idx * 10)
            
            # SMART LINKING
            target_tid = None
            if post.get("reply_ref_id") and post["reply_ref_id"] in batch_map:
                target_tid = batch_map[post["reply_ref_id"]]
            elif post.get("reply_to_old") and post_idx > 0:
                target_tid = f"task_{base_tid}_{ch_idx}_{post_idx-1}"

            task_data = {
                "task_id": tid,
                "owner_id": uid,
                "chat_id": cid,
                "content_type": post["content_type"],
                "content_text": post["content_text"],
                "file_id": post["file_id"],
                "entities": post["entities"],
                "pin": post.get("pin", st.get("pin", True)),
                "delete_old": post.get("delete_old", st.get("del", True)),
                
                # 👇 UPDATED: Save the Auto-Delete Offset
                # Priorities: 1. Specific Post Setting -> 2. Global Batch Setting -> 3. Default 0
                "auto_delete_offset": post.get("auto_delete_offset", st.get("auto_delete_offset", 0)),
                
                "repeat_interval": st["interval"],
                "start_time": run_time.isoformat(),
                "last_msg_id": None,
                "reply_target": target_tid
            }
            
            try:
                await save_task(task_data)
                # Ensure your add_scheduler_job accepts the task_data dict correctly
                add_scheduler_job(task_data) 
                total_tasks += 1
            except Exception as e:
                logger.error(f"Task Fail: {e}")

    # Cleanup
    if "broadcast_targets" in user_state[uid]: del user_state[uid]["broadcast_targets"]
    if "broadcast_queue" in user_state[uid]: del user_state[uid]["broadcast_queue"]
    # Cleanup the offset from state as well to prevent bleeding into next task
    if "auto_delete_offset" in user_state[uid]: del user_state[uid]["auto_delete_offset"]

    final_txt = (f"🎉 **Broadcast Scheduled!**\n\n"
                 f"📢 **Channels:** `{len(targets)}`\n"
                 f"📬 **Posts per Channel:** `{len(queue)}`\n"
                 f"⏱️ **Post Gap:** `10 seconds` (Safe Mode)\n"
                 f"📅 **Start Time:** `{t_str}`\n\n"
                 f"👉 Click /manage to schedule more.")

    await update_menu(q.message, final_txt, None, uid, force_new=False)

def add_scheduler_job(tid, t):
    if scheduler is None: return

    async def job_func():
        async with queue_lock:
            logger.info(f"🚀 JOB {tid} TRIGGERED")
            
            # 1. Calculate Next Run
            next_run_iso = None
            if t["repeat_interval"]:
                try:
                    now = datetime.datetime.now(IST)
                    mins = int(t["repeat_interval"].split("=")[1])
                    next_run = now + datetime.timedelta(minutes=mins)
                    next_run_iso = next_run.isoformat()
                except: pass

            try:
                # 2. Login as User
                session = await get_session(t["owner_id"])
                if not session: return 
                
                # 👇 CHANGED: Added Custom Device & App Names here
                async with Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session,
                                  device_model="AutoCast Client", 
                                  system_version="PC",
                                  app_version="AutoCast Version") as user:
                    target = int(t["chat_id"])
                    
                    # 3. Resolve Chat
                    try: await user.get_chat(target)
                    except:
                        async for dialog in user.get_dialogs(limit=200):
                            if dialog.chat.id == target: break
                    
                    # 4. Determine Reply Target (Smart Linking)
                    reply_id = None
                    if t.get("reply_target"):
                        # If this task is linked to another task (e.g., Post 5 -> Post 1)
                        # We fetch the Message ID that Post 1 sent.
                        target_msg_id = await get_task_last_msg_id(t["reply_target"])
                        if target_msg_id:
                            reply_id = int(target_msg_id)
                    
                    # 5. Delete Old Message (Cleanup Previous Run)
                    real_last_msg_id = await get_task_last_msg_id(t["task_id"])
                    
                    # Safe Delete: Only delete if we have an old ID.
                    if t["delete_old"] and real_last_msg_id:
                         # Safety check: Don't delete the message we are about to reply to (rare edge case)
                         if real_last_msg_id != reply_id:
                            try: await user.delete_messages(target, int(real_last_msg_id))
                            except: pass
                    
                    sent = None
                    caption = t["content_text"]
                    entities_objs = deserialize_entities(t["entities"])
                    
                    # 6. Send Content (The Heavy Logic)
                    
                    # --- A. POLL ---
                    if t["content_type"] == "poll":
                        poll_cfg = json.loads(t["entities"])
                        try:
                            sent = await user.send_poll(
                                chat_id=target, 
                                question=caption, 
                                options=poll_cfg["options"], 
                                is_anonymous=poll_cfg["is_anonymous"], 
                                allows_multiple_answers=poll_cfg["allows_multiple_answers"],
                                reply_to_message_id=reply_id
                            )
                        except Exception as e: logger.error(f"Poll Error: {e}")
                    
                    # --- B. TEXT ---
                    elif t["content_type"] == "text":
                        sent = await user.send_message(target, caption, entities=entities_objs, reply_to_message_id=reply_id, disable_web_page_preview=True)
                    
                    # --- C. MEDIA (Photo, Video, Animation, Document) ---
                    # Strategy: Try File ID first (Fast). If fails, Download & Upload (Reliable).
                    elif t["content_type"] in ["photo", "video", "animation", "document"]:
                        try:
                            # Attempt 1: Fast Send (File ID)
                            if t["content_type"] == "photo":
                                sent = await user.send_photo(target, t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif t["content_type"] == "video":
                                sent = await user.send_video(target, t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif t["content_type"] == "animation":
                                sent = await user.send_animation(target, t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif t["content_type"] == "document":
                                sent = await user.send_document(target, t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        
                        except Exception as e:
                            logger.warning(f"⚠️ ID failed for {t['content_type']} ({e}). Switching to Download Mode...")
                            # Attempt 2: Slow Send (Download -> Upload)
                            try:
                                f = await app.download_media(t["file_id"], in_memory=True)
                                if t["content_type"] == "photo":
                                    sent = await user.send_photo(target, f, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                                elif t["content_type"] == "video":
                                    sent = await user.send_video(target, f, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                                elif t["content_type"] == "animation":
                                    sent = await user.send_animation(target, f, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                                elif t["content_type"] == "document":
                                    sent = await user.send_document(target, f, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            except Exception as down_e:
                                logger.error(f"❌ Download Failed: {down_e}")

                    # --- D. AUDIO / VOICE (Always Download) ---
                    # File IDs for voice/audio are often tricky between bots/users, so we default to download.
                    elif t["content_type"] in ["audio", "voice"]:
                        try:
                            media_file = await app.download_media(t["file_id"], in_memory=True)
                            if t["content_type"] == "voice":
                                media_file.name = "voice.ogg"
                                sent = await user.send_voice(target, media_file, caption=caption, reply_to_message_id=reply_id)
                            else:
                                media_file.name = "audio.mp3"
                                sent = await user.send_audio(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                        except Exception as e: logger.error(f"❌ Audio/Voice Error: {e}")
                    
                    # --- E. STICKER ---
                    elif t["content_type"] == "sticker":
                        sent = await user.send_sticker(target, t["file_id"], reply_to_message_id=reply_id)

 # --- 7. Post-Send Actions ---
                    if sent:
                        logger.info(f"✅ Job {tid}: Message Sent! ID: {sent.id}")
                        
                        # Pinning (if enabled)
                        if t["pin"]:
                            try: 
                                pinned = await sent.pin()
                                if isinstance(pinned, Message): await pinned.delete()
                            except: pass
                        
                        # Update DB with new Message ID
                        await update_last_msg(tid, sent.id)

                        # 🚀 NEW: AUTO-DELETE BEFORE NEW POST LOGIC
                        offset_mins = t.get("auto_delete_offset", 0)
                        if offset_mins > 0 and t["repeat_interval"]:
                            try:
                                # Calculate when the NEXT post will happen
                                interval_mins = int(t["repeat_interval"].split("=")[1])
                                
                                # Deletion Time = (Current Time + Interval) - Offset
                                # This ensures it deletes exactly X mins before the next one
                                delay_until_deletion = interval_mins - offset_mins
                                
                                if delay_until_deletion > 0:
                                    run_at = datetime.datetime.now(IST) + datetime.timedelta(minutes=delay_until_deletion)
                                    
                                    # Schedule the deletion job
                                    scheduler.add_job(
                                        delete_sent_message,
                                        'date',
                                        run_date=run_at,
                                        args=[t['owner_id'], t['chat_id'], sent.id],
                                        id=f"del_{tid}_{sent.id}", # Unique ID prevents overwriting
                                        misfire_grace_time=60
                                    )
                                    logger.info(f"⏳ Scheduled delete for Job {tid} at {run_at} ({offset_mins}m before next)")
                                else:
                                    logger.warning(f"⚠️ Offset {offset_mins}m is >= Interval {interval_mins}m. Skipping auto-delete.")
                            except Exception as e:
                                logger.error(f"❌ Deletion Scheduling Error: {e}")

                        # Auto-Delete Task from DB if it is NOT repeating
                        if not t["repeat_interval"]:
                            await delete_task(tid)
                            logger.info(f"🗑️ One-time task {tid} deleted from DB.")

            except Exception as e:
                logger.error(f"🔥 Job {tid} Critical: {e}")
            
            finally:
                # Update Next Run Time (for repeating tasks)
                if next_run_iso and t["repeat_interval"]:
                    try: await update_next_run(tid, next_run_iso)
                    except: pass

    # 8. Setup Scheduler Trigger
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
    queue_lock = asyncio.Lock()
    
    # 1. Initialize the new schema
    await init_db()
    
    # 2. Run the migration to pull data from the old 'tasks' table
    await migrate_to_v11()
    
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
