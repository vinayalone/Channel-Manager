import os
import json
import logging
import asyncio
import datetime
import pytz

from pyrogram import Client, filters, idle, errors, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

# --- Configuration ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# Admin IDs: List of user IDs allowed to use the bot
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip()]

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)

# --- Database ---
DB_FILE = "data.json"
data = {"sessions": {}, "tasks": {}, "channels": {}} # Added 'channels' cache

# --- State ---
login_state = {}
user_state = {}

# --- Init ---
app = Client("manager_interface", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = AsyncIOScheduler(timezone=IST)

# --- Persistence ---
def load_db():
    global data
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            data = json.load(f)

def save_db():
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4, default=str)

def is_authorized(user_id):
    if not ADMIN_IDS: return True
    return user_id in ADMIN_IDS

# --- Helper: Get User Client ---
async def get_user_client(user_id):
    session = data["sessions"].get(str(user_id))
    if not session: return None
    user_app = Client(f":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session, no_updates=True)
    await user_app.start()
    return user_app

# --- 1. COMMANDS & MENUS ---

@app.on_message(filters.command("manage"))
async def cmd_manage(c, m):
    if not is_authorized(m.from_user.id): return
    uid = str(m.from_user.id)
    
    if uid not in data["sessions"]:
        await m.reply_text(
            "👋 **Welcome!**\nI am your Channel Manager.\n\nPlease login first so I can post for you.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Login Now", callback_data="login_start")]])
        )
    else:
        await show_main_menu(m)

# --- 2. CALLBACK HANDLER (The Brains) ---

@app.on_callback_query()
async def callback_handler(client, query):
    uid = query.from_user.id
    if not is_authorized(uid): return
    d = query.data
    
    # --- Navigation ---
    if d == "menu_home":
        user_state[uid] = None # Reset state
        await show_main_menu(query.message)

    # --- Logout Flow ---
    elif d == "logout_confirm":
        await query.message.edit_text(
            "⚠️ **Are you sure?**\nThis will disconnect your account from the bot.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Logout", callback_data="logout_final")],
                [InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]
            ])
        )
    
    elif d == "logout_final":
        if str(uid) in data["sessions"]:
            del data["sessions"][str(uid)]
            save_db()
        await query.message.edit_text("✅ **Logged Out Successfully.**\nSee you next time!")

    # --- Channel Management ---
    elif d == "add_channel":
        user_state[uid] = {"step": "waiting_forward"}
        await query.message.edit_text(
            "📝 **Add a Channel**\n\n"
            "Simply **Forward a Message** from that channel to me right now.\n"
            "I will detect the ID automatically.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]])
        )

    elif d == "list_channels":
        await show_channels_list(uid, query.message)

    elif d.startswith("manage_ch_"):
        c_id = d.split("_")[2]
        await show_channel_dashboard(uid, query.message, c_id)

    elif d.startswith("rem_ch_"):
        c_id = d.split("_")[2]
        # Remove from local cache
        if str(uid) in data["channels"] and c_id in data["channels"][str(uid)]:
            del data["channels"][str(uid)][c_id]
            save_db()
        await query.answer("Channel Removed")
        await show_channels_list(uid, query.message)

    # --- Post Management ---
    elif d.startswith("new_post_"):
        c_id = d.split("_")[2]
        user_state[uid] = {"step": "waiting_content", "target_channel": c_id}
        await query.message.edit_text(
            "1️⃣ **Send Content**\n\n"
            "Send the Text, Photo, or Video you want to post.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data=f"manage_ch_{c_id}")]])
        )

    elif d.startswith("view_tasks_"):
        c_id = d.split("_")[2]
        await show_active_tasks(uid, query.message, c_id)

    elif d.startswith("del_task_"):
        t_id = d.split("del_task_")[1]
        c_id = data["tasks"][t_id]["chat_id"]
        
        # Stop job
        try: scheduler.remove_job(t_id)
        except: pass
        
        del data["tasks"][t_id]
        save_db()
        await query.answer("Task Stopped")
        await show_active_tasks(uid, query.message, c_id)

    # --- Scheduling Toggles ---
    elif d in ["date_now", "repeat_none", "toggle_pin", "toggle_del", "confirm_schedule"]:
        await handle_schedule_logic(uid, query, d)

    # --- Login ---
    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await query.message.edit_text("📱 **Enter Phone Number**\n\nExample: `+919876543210`")

# --- 3. MESSAGE HANDLER (Inputs) ---

@app.on_message(filters.private)
async def message_handler(client, message):
    uid = message.from_user.id
    if not is_authorized(uid): return
    text = message.text

    # --- Login Inputs (Same as before but with "aa" logic) ---
    if uid in login_state:
        await handle_login_input(client, message, uid)
        return

    # --- Add Channel (Forwarding) ---
    if user_state.get(uid, {}).get("step") == "waiting_forward":
        if message.forward_from_chat:
            chat = message.forward_from_chat
            c_id = str(chat.id)
            title = chat.title
            
            # Save to user's personal channel list
            if str(uid) not in data["channels"]: data["channels"][str(uid)] = {}
            data["channels"][str(uid)][c_id] = title
            save_db()
            
            user_state[uid] = None
            await message.reply(f"✅ **Added:** {title}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_home")]]))
        else:
            await message.reply("⚠️ That was not a channel forward. Please forward a message from the channel.")
        return

    # --- New Post Content ---
    if user_state.get(uid, {}).get("step") == "waiting_content":
        st = user_state[uid]
        st["msg_id"] = message.id
        st["step"] = "waiting_date"
        user_state[uid] = st
        
        now_str = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        await message.reply(
            f"2️⃣ **When to Post?** (IST)\nNow: `{now_str}`\n\n"
            "Type date: `2026-02-05 14:30`\nOR click **Now**.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ Post Now", callback_data="date_now")]])
        )
        return

    # --- Date Input ---
    if user_state.get(uid, {}).get("step") == "waiting_date":
        try:
            dt = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M")
            user_state[uid]["start_time"] = IST.localize(dt)
            await ask_repetition(message, uid)
        except:
            await message.reply("⚠️ Invalid format. Use `YYYY-MM-DD HH:MM`")
        return

    # --- Repetition Input ---
    if user_state.get(uid, {}).get("step") == "waiting_repeat":
        interval = parse_interval(text)
        if interval:
            user_state[uid]["interval"] = interval
            await send_confirm_panel(message, uid)
        else:
            await message.reply("⚠️ Invalid. Try `1 day` or `2 hours`.")

# --- 4. UI HELPERS ---

async def show_main_menu(message):
    buttons = [
        [InlineKeyboardButton("📢 My Channels", callback_data="list_channels"),
         InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")],
        [InlineKeyboardButton("🚪 Logout", callback_data="logout_confirm")]
    ]
    text = "🤖 **Manager Dashboard**\nSelect an option:"
    if isinstance(message, Message): await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))
    else: await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

async def show_channels_list(uid, message):
    user_chs = data["channels"].get(str(uid), {})
    if not user_chs:
        await message.edit_text(
            "❌ **No Channels Found**\nClick 'Add Channel' to add one.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add Channel", callback_data="add_channel"), InlineKeyboardButton("🔙 Back", callback_data="menu_home")]])
        )
        return

    buttons = []
    for cid, title in user_chs.items():
        buttons.append([InlineKeyboardButton(f"📢 {title}", callback_data=f"manage_ch_{cid}")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    
    await message.edit_text("**Select a Channel to Manage:**", reply_markup=InlineKeyboardMarkup(buttons))

async def show_channel_dashboard(uid, message, c_id):
    title = data["channels"][str(uid)].get(c_id, "Unknown")
    # Count tasks
    count = sum(1 for t in data["tasks"].values() if str(t["chat_id"]) == c_id)
    
    buttons = [
        [InlineKeyboardButton("✨ New Post", callback_data=f"new_post_{c_id}"),
         InlineKeyboardButton(f"📋 Scheduled ({count})", callback_data=f"view_tasks_{c_id}")],
        [InlineKeyboardButton("🗑 Remove Channel", callback_data=f"rem_ch_{c_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ]
    await message.edit_text(f"⚙️ **Managing:** {title}", reply_markup=InlineKeyboardMarkup(buttons))

async def show_active_tasks(uid, message, c_id):
    tasks = {k:v for k,v in data["tasks"].items() if str(v["chat_id"]) == str(c_id)}
    if not tasks:
        await message.edit_text("✅ No active schedules.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"manage_ch_{c_id}")]]))
        return

    text = "**Active Schedules:**\n"
    buttons = []
    for t_id, t in tasks.items():
        start_fmt = datetime.datetime.fromisoformat(t["start_time_iso"]).strftime('%d-%b %H:%M')
        text += f"• Next: {start_fmt} | Repeat: {t['repeat_interval']}\n"
        buttons.append([InlineKeyboardButton(f"🛑 Stop Task", callback_data=f"del_task_{t_id}")])
    
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"manage_ch_{c_id}")])
    await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# --- 5. LOGIC HELPERS ---

async def ask_repetition(message, uid):
    user_state[uid]["step"] = "waiting_repeat"
    await message.reply( # Or edit if callback
        "3️⃣ **Repetition?**\n"
        "Type: `1 day`, `24 hours`\nOR click button:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚫 One Time Only", callback_data="repeat_none")]])
    )

async def send_confirm_panel(message, uid):
    st = user_state[uid]
    st["step"] = "confirm"
    st.setdefault("pin", False)
    st.setdefault("del", False)
    
    # Format text
    start_str = st["start_time"].strftime('%d-%b %H:%M')
    rep = st.get("interval") if st.get("interval") else "None"
    
    text = (
        "⚙️ **Confirm Schedule**\n\n"
        f"📅 Start: `{start_str}`\n"
        f"🔁 Repeat: `{rep}`\n"
        f"📌 Pin Msg: {'✅' if st['pin'] else '❌'}\n"
        f"🗑 Del Old: {'✅' if st['del'] else '❌'}"
    )
    
    buttons = [
        [InlineKeyboardButton(f"Pin: {st['pin']}", callback_data="toggle_pin"),
         InlineKeyboardButton(f"Del: {st['del']}", callback_data="toggle_del")],
        [InlineKeyboardButton("✅ START", callback_data="confirm_schedule")]
    ]
    
    if isinstance(message, Message): await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))
    else: await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

async def handle_schedule_logic(uid, query, d):
    st = user_state[uid]
    
    if d == "date_now":
        st["start_time"] = datetime.datetime.now(IST)
        await ask_repetition(query.message, uid)
    
    elif d == "repeat_none":
        st["interval"] = None
        await send_confirm_panel(query.message, uid)
        
    elif d == "toggle_pin":
        st["pin"] = not st["pin"]
        await send_confirm_panel(query.message, uid)
        
    elif d == "toggle_del":
        st["del"] = not st["del"]
        await send_confirm_panel(query.message, uid)
        
    elif d == "confirm_schedule":
        # SAVE TASK
        t_id = f"task_{int(datetime.datetime.now().timestamp())}"
        task = {
            "task_id": t_id,
            "owner_id": uid,
            "chat_id": st["target_channel"],
            "msg_id": st["msg_id"],
            "source_chat": query.message.chat.id,
            "pin": st["pin"],
            "delete_old": st["del"],
            "repeat_interval": st.get("interval"),
            "start_time_iso": st["start_time"].isoformat(),
            "last_msg_id": None
        }
        data["tasks"][t_id] = task
        save_db()
        add_job(t_id, task)
        
        user_state[uid] = None
        await query.message.edit_text("✅ **Task Scheduled!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))

# --- 6. LOGIN LOGIC (Hidden to save space, but essential) ---
async def handle_login_input(client, message, uid):
    st = login_state[uid]
    text = message.text
    
    if st["step"] == "waiting_phone":
        phone = text.replace(" ", "")
        temp = Client(f"sess_{uid}", api_id=API_ID, api_hash=API_HASH)
        await temp.connect()
        try:
            sent = await temp.send_code(phone)
            st.update({"client": temp, "phone": phone, "hash": sent.phone_code_hash, "step": "waiting_code"})
            await message.reply("📩 **Code Sent!**\n\nTo avoid error, send code as: `aa12345`")
        except Exception as e:
            await message.reply(f"❌ {e}")
            await temp.disconnect()

    elif st["step"] == "waiting_code":
        code = text.lower().replace("aa", "").replace(" ", "")
        try:
            await st["client"].sign_in(st["phone"], st["hash"], code)
            data["sessions"][str(uid)] = await st["client"].export_session_string()
            save_db()
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Login Success!** Type /manage")
        except errors.SessionPasswordNeeded:
            st["step"] = "waiting_pass"
            await message.reply("🔐 **Enter 2FA Password:**")
        except Exception as e:
            await message.reply(f"❌ {e}")

    elif st["step"] == "waiting_pass":
        try:
            await st["client"].check_password(text)
            data["sessions"][str(uid)] = await st["client"].export_session_string()
            save_db()
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Login Success!** Type /manage")
        except Exception as e:
            await message.reply(f"❌ {e}")

# --- 7. SCHEDULER WORKER & STARTUP ---

def add_job(t_id, t):
    start_dt = datetime.datetime.fromisoformat(t["start_time_iso"])
    
    async def worker():
        try:
            user = await get_user_client(t["owner_id"])
            if not user: return
            
            # Del Old
            if t["delete_old"] and t["last_msg_id"]:
                try: await user.delete_messages(int(t["chat_id"]), t["last_msg_id"])
                except: pass
            
            # Send
            # Note: Copying from Bot DM to Channel using User Client
            # Requires User to have interacted with the bot.
            bot_info = await app.get_me()
            sent = await user.copy_message(
                chat_id=int(t["chat_id"]),
                from_chat_id=bot_info.id,
                message_id=t["msg_id"]
            )
            
            if t["pin"]: 
                try: await sent.pin()
                except: pass
                
            data["tasks"][t_id]["last_msg_id"] = sent.id
            save_db()
            await user.stop()
        except Exception as e:
            logging.error(f"Task Error: {e}")

    trigger = IntervalTrigger(start_date=start_dt, timezone=IST, **t["repeat_interval"]) if t["repeat_interval"] else DateTrigger(run_date=start_dt, timezone=IST)
    scheduler.add_job(worker, trigger, id=t_id, replace_existing=True)

def parse_interval(t):
    try:
        p = t.split()
        val = int(p[0])
        if "min" in p[1]: return {"minutes": val}
        if "hour" in p[1]: return {"hours": val}
        if "day" in p[1]: return {"days": val}
    except: return None

# --- Startup ---
async def boot_services():
    await app.start()
    scheduler.start()
    await idle()
    await app.stop()

if __name__ == "__main__":
    load_db()
    for k, v in data["tasks"].items():
        add_job(k, v)
    app.run(boot_services())
