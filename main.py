import os
import json
import logging
import asyncio
import datetime
import pytz
from pyrogram import Client, filters, errors
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

# --- Configuration ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# Admin IDs: Separated by commas (e.g., "123456, 987654")
# If empty, ANYONE can use the bot (careful!)
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip()]

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)

# --- Database ---
DB_FILE = "data.json"
# Structure:
# {
#   "sessions": { "user_id": "session_string" },
#   "tasks": { "task_id": { ... } }
# }
data = {"sessions": {}, "tasks": {}}

# --- State Management ---
# Stores temporary login states: { user_id: { "step": "phone", "client": ClientObject, "phone": "+91...", "phone_hash": "..." } }
login_state = {}
user_state = {} # For menu navigation

# --- Init Bot ---
app = Client("manager_interface", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = AsyncIOScheduler(timezone=IST)

# --- Persistence Helpers ---
def load_db():
    global data
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            data = json.load(f)

def save_db():
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4, default=str)

def is_authorized(user_id):
    if not ADMIN_IDS: return True # Public mode if no admins set
    return user_id in ADMIN_IDS

# --- Helper: Dynamic User Client ---
# Starts a temporary client for the specific user to perform actions
async def get_user_client(user_id):
    session = data["sessions"].get(str(user_id))
    if not session: return None
    
    # Start a temporary client in memory
    user_app = Client(f":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session, no_updates=True)
    await user_app.start()
    return user_app

# --- 1. LOGIN FLOW (The hard part) ---

@app.on_message(filters.command("start"))
async def start_cmd(c, m):
    if not is_authorized(m.from_user.id): return
    
    # Check if user is already logged in
    if str(m.from_user.id) in data["sessions"]:
        await m.reply_text("✅ You are logged in! Type /manage to start.", 
                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚪 Logout", callback_data="logout")]]))
    else:
        await m.reply_text("👋 **Welcome!**\n\nTo use this bot, you need to log in with your Telegram account so I can post on your behalf.\n\nClick below to login.",
                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Login Account", callback_data="login_start")]]))

@app.on_callback_query()
async def callback_handler(client, query):
    uid = query.from_user.id
    if not is_authorized(uid): return
    d = query.data

    # --- Login Handlers ---
    if d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await query.message.edit_text("📱 **Enter Phone Number**\n\nPlease send your phone number in international format.\nExample: `+919876543210`")
    
    elif d == "logout":
        if str(uid) in data["sessions"]:
            del data["sessions"][str(uid)]
            save_db()
        await query.message.edit_text("✅ Logged out.")

    # --- Menu Handlers ---
    elif d == "menu_home":
        await show_main_menu(query.message)
    
    elif d == "list_channels":
        await list_user_channels(uid, query.message)

    elif d.startswith("sel_ch_"):
        c_id = d.split("_")[2]
        await show_post_menu(uid, query.message, c_id)

    elif d.startswith("del_task_"):
        t_id = d.split("del_task_")[1]
        if t_id in data["tasks"]:
            try: scheduler.remove_job(t_id)
            except: pass
            del data["tasks"][t_id]
            save_db()
            await query.answer("Task deleted.")
            await show_main_menu(query.message)

    # --- Scheduler Confirmations ---
    elif d == "confirm_schedule":
        await finalize_schedule(uid, query)
    
    elif d in ["toggle_pin", "toggle_del", "repeat_none", "date_now"]:
        await handle_schedule_toggles(uid, query, d)

# --- Message Handler (Login & Content) ---
# --- Message Handler (Login & Content) ---
@app.on_message(filters.private)
async def message_handler(client, message):
    uid = message.from_user.id
    if not is_authorized(uid): return
    text = message.text
    
    # 1. LOGIN PROCESS
    if uid in login_state:
        st = login_state[uid]
        
        # Step A: Phone Received -> Send Code
        if st["step"] == "waiting_phone":
            phone = text.replace(" ", "")
            status_msg = await message.reply("🔄 Sending Login Code...")
            
            temp_client = Client(f"session_{uid}", api_id=API_ID, api_hash=API_HASH)
            await temp_client.connect()
            
            try:
                sent_code = await temp_client.send_code(phone)
                st["client"] = temp_client
                st["phone"] = phone
                st["phone_hash"] = sent_code.phone_code_hash
                st["step"] = "waiting_code"
                
                # IMPORTANT: Instructions for the "aa" trick
                await status_msg.edit_text(
                    "📩 **Code Sent!**\n\n"
                    "⚠️ **To prevent Telegram from expiring the code:**\n"
                    "Please add `aa` before the code.\n\n"
                    "Example: If code is `12345`, send: **`aa12345`**"
                )
            except Exception as e:
                await temp_client.disconnect()
                await status_msg.edit_text(f"❌ Error: {e}")
                del login_state[uid]

        # Step B: Code Received -> Sign In
        elif st["step"] == "waiting_code":
            # CLEANUP: Remove 'aa' or any non-digit characters
            # This turns "aa12345", "aa 12345", "code 12345" -> "12345"
            raw_text = text.lower().replace("aa", "").replace(" ", "")
            code = "".join([c for c in raw_text if c.isdigit()])

            if not code:
                await message.reply("⚠️ invalid format. Please send like **`aa12345`**.")
                return

            temp_client = st["client"]
            
            try:
                await message.reply("🔄 Verifying code...")
                await temp_client.sign_in(st["phone"], st["phone_hash"], code)
                
                # If we get here, Login is SUCCESS (No 2FA)
                string = await temp_client.export_session_string()
                data["sessions"][str(uid)] = string
                save_db()
                await temp_client.disconnect()
                del login_state[uid]
                
                await message.reply("✅ **Login Successful!**\nType /manage to start.", 
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Open Menu", callback_data="menu_home")]]))
            
            except errors.SessionPasswordNeeded:
                # 2FA DETECTED!
                st["step"] = "waiting_password"
                await message.reply("🔐 **Two-Step Verification Detected**\n\nPlease enter your Telegram Cloud Password.")
            
            except errors.PhoneCodeExpired:
                 await message.reply("❌ **Code Expired.**\nTelegram deleted the code. Please try logging in again.")
                 await temp_client.disconnect()
                 del login_state[uid]

            except Exception as e:
                await message.reply(f"❌ Error: {e}")

        # Step C: 2FA Password Received (If needed)
        elif st["step"] == "waiting_password":
            temp_client = st["client"]
            password = text # The user's password
            
            try:
                await message.reply("🔄 Checking Password...")
                await temp_client.check_password(password)
                
                # Login SUCCESS (After 2FA)
                string = await temp_client.export_session_string()
                data["sessions"][str(uid)] = string
                save_db()
                await temp_client.disconnect()
                del login_state[uid]
                
                await message.reply("✅ **Login Successful!**\nType /manage to start.", 
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Open Menu", callback_data="menu_home")]]))
            except errors.PasswordHashInvalid:
                 await message.reply("❌ **Wrong Password.** Please try again.")
            except Exception as e:
                await message.reply(f"❌ Error: {e}")

    # 2. MAIN MENU COMMAND
    elif text == "/manage":
        if str(uid) not in data["sessions"]:
            await message.reply("Please /start and Login first.")
            return
        await show_main_menu(message)

    # 3. POST CONTENT & DATE INPUTS
    elif uid in user_state:
        st = user_state[uid]
        
        # Content Received
        if st["step"] == "waiting_content":
            st["msg_id"] = message.id
            st["step"] = "waiting_date"
            user_state[uid] = st
            now_str = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
            await message.reply(
                f"2️⃣ **When to Post?** (IST)\nCurrent: `{now_str}`\n\nType date: `2026-02-05 14:30`\nOR click button for NOW.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ Now", callback_data="date_now")]])
            )

        # Date Received
        elif st["step"] == "waiting_date":
            try:
                dt = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M")
                st["start_time"] = IST.localize(dt)
                st["step"] = "waiting_repeat"
                user_state[uid] = st
                await ask_repetition(message)
            except:
                await message.reply("⚠️ Invalid format. Use `YYYY-MM-DD HH:MM`")

        # Repeat Interval Received
        elif st["step"] == "waiting_repeat":
            interval = parse_interval(text)
            if interval:
                st["interval"] = interval
                st["step"] = "confirm"
                st["pin"] = False
                st["del"] = False
                user_state[uid] = st
                await send_confirm_panel(message.chat.id, uid)
            else:
                await message.reply("⚠️ Invalid. Try `1 day` or `2 hours`.")

# --- Logic Functions ---

async def show_main_menu(message):
    uid = message.chat.id
    # Count user tasks
    user_tasks = [t for t in data["tasks"].values() if str(t["owner_id"]) == str(uid)]
    
    text = f"🤖 **Manager Dashboard**\nActive Tasks: {len(user_tasks)}"
    
    # List tasks button
    buttons = [[InlineKeyboardButton("📢 Select Channel & Post", callback_data="list_channels")]]
    
    if user_tasks:
        # Show delete buttons for active tasks
        text += "\n\n**Running Tasks:**"
        for t in user_tasks:
             # Find channel name (lazy way, usually we'd cache it)
             text += f"\n• Task `{t['task_id'][-4:]}` (Next: {t.get('next_run', 'Wait...')})"
             buttons.append([InlineKeyboardButton(f"🗑 Stop Task {t['task_id'][-4:]}", callback_data=f"del_task_{t['task_id']}")])

    buttons.append([InlineKeyboardButton("❌ Close", callback_data="logout")])
    if isinstance(message, Message): await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))
    else: await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

async def list_user_channels(uid, message):
    status = await message.edit_text("🔄 Fetching your channels... (This takes a second)")
    try:
        user = await get_user_client(uid)
        channels = []
        async for dialog in user.get_dialogs():
            if dialog.chat.type in [enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP]:
                # Check admin rights (basic check: if we can post)
                # Note: getting full admin status is slow, we assume if user picks it, they know
                channels.append(dialog.chat)
        await user.stop()
        
        buttons = []
        for c in channels[:15]: # Limit to 15 to avoid button limits
            buttons.append([InlineKeyboardButton(f"📢 {c.title}", callback_data=f"sel_ch_{c.id}")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
        
        await status.edit_text("**Select a Channel:**", reply_markup=InlineKeyboardMarkup(buttons))
        
    except Exception as e:
        await status.edit_text(f"❌ Error fetching channels: {e}")

async def show_post_menu(uid, message, c_id):
    user_state[uid] = {"step": "waiting_content", "target_channel": c_id}
    await message.edit_text(
        f"1️⃣ **Send Post Content**\n\nTarget Channel ID: `{c_id}`\nSend the text/photo/video now.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data="menu_home")]])
    )

async def ask_repetition(message):
    await message.reply(
        "3️⃣ **Repetition?**\nType: `1 day`, `24 hours`\nOR click No Repetition.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚫 One Time Only", callback_data="repeat_none")]])
    )

async def handle_schedule_toggles(uid, query, d):
    st = user_state[uid]
    if d == "date_now":
        st["start_time"] = datetime.datetime.now(IST)
        st["step"] = "waiting_repeat"
        user_state[uid] = st
        await ask_repetition(query.message)
    elif d == "repeat_none":
        st["interval"] = None
        st["step"] = "confirm"
        st["pin"] = False
        st["del"] = False
        user_state[uid] = st
        await send_confirm_panel(query.message.chat.id, uid)
    elif d == "toggle_pin":
        st["pin"] = not st.get("pin", False)
        await send_confirm_panel(query.message.chat.id, uid)
    elif d == "toggle_del":
        st["del"] = not st.get("del", False)
        await send_confirm_panel(query.message.chat.id, uid)

async def send_confirm_panel(chat_id, uid):
    st = user_state[uid]
    start_str = st["start_time"].strftime('%d-%b %H:%M')
    repeat_str = str(st["interval"]) if st["interval"] else "No Repetition"
    
    text = (
        f"⚙️ **Confirm**\n"
        f"📅 Start: {start_str}\n🔁 Repeat: {repeat_str}\n"
        f"📌 Pin: {st['pin']} | 🗑 Del Old: {st['del']}"
    )
    buttons = [
        [InlineKeyboardButton(f"Pin: {st['pin']}", callback_data="toggle_pin"),
         InlineKeyboardButton(f"Del: {st['del']}", callback_data="toggle_del")],
        [InlineKeyboardButton("✅ START TASK", callback_data="confirm_schedule")]
    ]
    # Handle edit vs new message
    try: await app.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(buttons))
    except: pass # If callback

async def finalize_schedule(uid, query):
    st = user_state[uid]
    t_id = f"task_{int(datetime.datetime.now().timestamp())}"
    
    task_data = {
        "task_id": t_id,
        "owner_id": uid,
        "chat_id": st["target_channel"],
        "source_chat": query.message.chat.id, # Bot chat
        "msg_id": st["msg_id"],
        "pin": st["pin"],
        "delete_old": st["del"],
        "repeat_interval": st["interval"],
        "start_time_iso": st["start_time"].isoformat(),
        "last_msg_id": None
    }
    
    data["tasks"][t_id] = task_data
    save_db()
    
    add_job(t_id, task_data)
    user_state[uid] = None
    await query.message.edit_text("✅ **Task Started!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))

# --- Scheduler Logic ---
def add_job(t_id, t):
    start_dt = datetime.datetime.fromisoformat(t["start_time_iso"])
    
    async def worker():
        try:
            # 1. Start User Client
            user = await get_user_client(t["owner_id"])
            if not user: return
            
            # 2. Delete Old
            if t["delete_old"] and t["last_msg_id"]:
                try: await user.delete_messages(int(t["chat_id"]), t["last_msg_id"])
                except: pass
            
            # 3. Send
            # We must COPY from the BOT CHAT to the CHANNEL
            # But the UserClient can't see the Bot Chat history unless they are in it.
            # Trick: The UserClient just copies the message content provided they have access.
            # Ideally, we use the bot to copy, but we want the USER to look like the sender.
            # Actually, copy_message from bot chat via UserClient works if User has history access.
            # If not, we might need to forward. 
            # Safer for UserBot: Send Text/File fresh. But for now, let's try copy.
            
            # Since the message is in Bot DM, the UserClient might not find it easily by ID.
            # Fallback: The BOT copies it to the Channel (as Bot), OR the UserBot posts it.
            # User request: "User Bot".
            # To fix "UserBot can't see Bot DM message": 
            # We will use the `app` (Bot) to get the message, download media, and UserBot uploads it.
            # COMPLEXITY REDUCTION: We will assume the UserBot can just forward or we use copy.
            
            # Simple approach: The UserClient copies from the Bot's chat (User must have started bot).
            # The UserClient sees the chat with the bot as just a user ID.
            
            bot_info = await app.get_me()
            sent = await user.copy_message(
                chat_id=int(t["chat_id"]),
                from_chat_id=bot_info.id, # Copy from the Bot
                message_id=t["msg_id"]
            )
            
            if t["pin"]:
                try: await sent.pin()
                except: pass
            
            data["tasks"][t_id]["last_msg_id"] = sent.id
            save_db()
            await user.stop()
            
        except Exception as e:
            logging.error(f"Task {t_id} error: {e}")

    if t["repeat_interval"]:
        scheduler.add_job(worker, IntervalTrigger(start_date=start_dt, timezone=IST, **t["repeat_interval"]), id=t_id, replace_existing=True)
    else:
        scheduler.add_job(worker, DateTrigger(run_date=start_dt, timezone=IST), id=t_id, replace_existing=True)

def parse_interval(text):
    try:
        parts = text.lower().split()
        val = int(parts[0])
        if "min" in parts[1]: return {"minutes": val}
        if "hour" in parts[1]: return {"hours": val}
        if "day" in parts[1]: return {"days": val}
    except: return None

# --- Main ---
from pyrogram import idle, enums  # Added 'idle' to keep the bot running

async def boot_services():
    # 1. Start the Bot (Connects to Telegram)
    await app.start()
    print("✅ Bot Started")

    # 2. Start the Scheduler (Now that the Loop is definitively running)
    scheduler.start()
    print("✅ Scheduler Started")

    # 3. Keep the bot running until you stop it
    await idle()
    
    # 4. Stop gracefully
    await app.stop()
    print("🛑 Bot Stopped")

if __name__ == "__main__":
    load_db()
    
    # Reload saved tasks into memory
    print("🔄 Loading tasks...")
    for k, v in data["tasks"].items():
        try:
            add_job(k, v)
        except Exception as e:
            print(f"Failed to load task {k}: {e}")

    # Start everything using app.run()
    # This automatically creates the Event Loop first, preventing the crash
    print("🚀 Launching...")
    app.run(boot_services())
