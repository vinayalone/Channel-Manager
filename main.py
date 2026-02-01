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
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip()]

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)

# --- Database ---
DB_FILE = "data.json"
data = {"sessions": {}, "tasks": {}, "channels": {}}

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

async def get_user_client(user_id):
    session = data["sessions"].get(str(user_id))
    if not session: return None
    # We use :memory: so it doesn't conflict with files, but we must resolve peers manually
    user_app = Client(f":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session, no_updates=True)
    await user_app.start()
    return user_app

def get_next_run_time(start_dt, interval):
    if not interval: return "One Time"
    now = datetime.datetime.now(IST)
    next_run = start_dt
    delta = datetime.timedelta(**interval)
    while next_run < now:
        next_run += delta
    return next_run.strftime('%d-%b %H:%M')

# --- 1. COMMANDS ---

@app.on_message(filters.command("manage"))
async def cmd_manage(c, m):
    if not is_authorized(m.from_user.id): return
    uid = str(m.from_user.id)
    if uid not in data["sessions"]:
        await m.reply_text(
            "👋 **Welcome!**\nPlease login to start.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Connect Account", callback_data="login_start")]])
        )
    else:
        await show_main_menu(m)

# --- 2. CALLBACKS ---

@app.on_callback_query()
async def callback_handler(client, query):
    uid = query.from_user.id
    if not is_authorized(uid): return
    d = query.data
    
    if d == "menu_home":
        user_state[uid] = None
        await show_main_menu(query.message)

    elif d == "logout_confirm":
        await query.message.edit_text(
            "⚠️ **Confirm Logout?**",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔴 Yes", callback_data="logout_final"), InlineKeyboardButton("🔙 No", callback_data="menu_home")]])
        )
    
    elif d == "logout_final":
        if str(uid) in data["sessions"]: del data["sessions"][str(uid)]; save_db()
        await query.message.edit_text("✅ **Logged Out.**")

    elif d == "add_channel":
        user_state[uid] = {"step": "waiting_forward"}
        await query.message.edit_text("📝 **Add Channel**\nForward a message from the channel now.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]]))

    elif d == "list_channels":
        await show_channels_list(uid, query.message)

    elif d.startswith("manage_ch_"):
        await show_channel_dashboard(uid, query.message, d.split("_")[2])

    elif d.startswith("rem_ch_"):
        c_id = d.split("_")[2]
        if str(uid) in data["channels"]: del data["channels"][str(uid)][c_id]; save_db()
        await query.answer("Channel Removed")
        await show_channels_list(uid, query.message)

    elif d.startswith("new_post_"):
        c_id = d.split("_")[2]
        user_state[uid] = {"step": "waiting_content", "target_channel": c_id}
        await query.message.edit_text("1️⃣ **Step 1: Content**\nSend Text/Photo/Video now.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data=f"manage_ch_{c_id}")]]))

    elif d.startswith("view_tasks_"):
        await show_active_tasks(uid, query.message, d.split("_")[2])

    elif d.startswith("del_task_"):
        t_id = d.split("del_task_")[1]
        c_id = data["tasks"][t_id]["chat_id"]
        try: scheduler.remove_job(t_id)
        except: pass
        del data["tasks"][t_id]; save_db()
        await query.answer("Task Stopped")
        await show_active_tasks(uid, query.message, c_id)

    elif d in ["date_now", "repeat_none", "toggle_pin", "toggle_del", "confirm_schedule"]:
        await handle_schedule_logic(uid, query, d)

    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await query.message.edit_text("📱 **Enter Phone Number**\nExample: `+919876543210`")

# --- 3. MESSAGE INPUTS ---

@app.on_message(filters.private)
async def message_handler(client, message):
    uid = message.from_user.id
    if not is_authorized(uid): return
    text = message.text

    if uid in login_state:
        await handle_login_input(client, message, uid)
        return

    if user_state.get(uid, {}).get("step") == "waiting_forward":
        if message.forward_from_chat:
            chat = message.forward_from_chat
            if str(uid) not in data["channels"]: data["channels"][str(uid)] = {}
            data["channels"][str(uid)][str(chat.id)] = chat.title
            save_db()
            user_state[uid] = None
            await message.reply(f"✅ **Added:** {chat.title}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]]))
        else:
            await message.reply("⚠️ Forward from a channel please.")
        return

    if user_state.get(uid, {}).get("step") == "waiting_content":
        st = user_state[uid]
        st.update({"msg_id": message.id, "step": "waiting_date"})
        now_str = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        await message.reply(f"2️⃣ **Timing** (IST)\nCurrent: `{now_str}`\nType date `YYYY-MM-DD HH:MM` or click:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ Post Now", callback_data="date_now")]]))
        return

    if user_state.get(uid, {}).get("step") == "waiting_date":
        try:
            dt = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M")
            user_state[uid]["start_time"] = IST.localize(dt)
            await ask_repetition(message, uid, is_edit=False)
        except:
            await message.reply("⚠️ Format: `YYYY-MM-DD HH:MM`")
        return

    if user_state.get(uid, {}).get("step") == "waiting_repeat":
        interval = parse_interval(text)
        if interval:
            user_state[uid]["interval"] = interval
            await send_confirm_panel(message, uid, is_edit=False)
        else:
            await message.reply("⚠️ Try `1 day` or `12 hours`")

# --- 4. LOGIC & MENUS ---

async def handle_login_input(client, message, uid):
    st = login_state[uid]
    text = message.text
    
    if st["step"] == "waiting_phone":
        phone = text.replace(" ", "")
        status_msg = await message.reply("🔄 **Connecting to Telegram...**\nPlease wait 5-10 seconds.")
        
        temp = Client(f"sess_{uid}", api_id=API_ID, api_hash=API_HASH)
        await temp.connect()
        try:
            sent = await temp.send_code(phone)
            st.update({"client": temp, "phone": phone, "hash": sent.phone_code_hash, "step": "waiting_code"})
            await status_msg.edit_text("📩 **Code Sent!**\n\n⚠️ **Format:** Add `aa` before code.\nExample: `aa12345`")
        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {e}")
            await temp.disconnect()

    elif st["step"] == "waiting_code":
        code = text.lower().replace("aa", "").replace(" ", "")
        try:
            await st["client"].sign_in(st["phone"], st["hash"], code)
            data["sessions"][str(uid)] = await st["client"].export_session_string()
            save_db()
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Success!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Dashboard", callback_data="menu_home")]]))
        except errors.SessionPasswordNeeded:
            st["step"] = "waiting_pass"
            await message.reply("🔐 **Enter 2FA Password:**")
        except Exception as e:
            await message.reply(f"❌ Error: {e}")

    elif st["step"] == "waiting_pass":
        try:
            await st["client"].check_password(text)
            data["sessions"][str(uid)] = await st["client"].export_session_string()
            save_db()
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Success!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Dashboard", callback_data="menu_home")]]))
        except Exception as e:
            await message.reply(f"❌ Error: {e}")

# --- UI Helpers ---

async def show_main_menu(m):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")], [InlineKeyboardButton("🚪 Logout", callback_data="logout_confirm")]])
    text = "🤖 **Manager Dashboard**"
    if isinstance(m, Message): await m.reply(text, reply_markup=kb)
    else: await m.edit_text(text, reply_markup=kb)

async def show_channels_list(uid, m):
    user_chs = data["channels"].get(str(uid), {})
    if not user_chs:
        await m.edit_text("ℹ️ **No Channels**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add Channel", callback_data="add_channel"), InlineKeyboardButton("🔙 Back", callback_data="menu_home")]]))
        return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"📢 {t}", callback_data=f"manage_ch_{c}")] for c, t in user_chs.items()] + [[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]])
    await m.edit_text("**Select Channel:**", reply_markup=kb)

async def show_channel_dashboard(uid, m, c_id):
    t = data["channels"][str(uid)].get(c_id, "Unknown")
    c = sum(1 for t in data["tasks"].values() if str(t["chat_id"]) == c_id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Post", callback_data=f"new_post_{c_id}"), InlineKeyboardButton(f"📋 Tasks ({c})", callback_data=f"view_tasks_{c_id}")],
        [InlineKeyboardButton("🗑 Delete Channel", callback_data=f"rem_ch_{c_id}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ])
    await m.edit_text(f"⚙️ **{t}**", reply_markup=kb)

async def show_active_tasks(uid, m, c_id):
    tasks = {k:v for k,v in data["tasks"].items() if str(v["chat_id"]) == str(c_id)}
    if not tasks: await m.edit_text("✅ **No Schedules**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"manage_ch_{c_id}")]])); return
    
    txt = "**Active Schedules:**\n"
    kb = []
    for tid, t in tasks.items():
        nxt = get_next_run_time(datetime.datetime.fromisoformat(t["start_time_iso"]), t["repeat_interval"])
        txt += f"• Next: `{nxt}`\n"
        kb.append([InlineKeyboardButton(f"🛑 Stop Task", callback_data=f"del_task_{tid}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"manage_ch_{c_id}")])
    await m.edit_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def ask_repetition(message, uid, is_edit=True):
    user_state[uid]["step"] = "waiting_repeat"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚫 One Time Only", callback_data="repeat_none")]])
    text = "3️⃣ **Step 3: Repetition**\nType `1 day` OR click:"
    if is_edit: await message.edit_text(text, reply_markup=kb)
    else: await message.reply(text, reply_markup=kb)

async def send_confirm_panel(message, uid, is_edit=True):
    st = user_state[uid]
    st["step"] = "confirm"
    st.setdefault("pin", False); st.setdefault("del", False)
    
    txt = (
        "⚙️ **Confirm Schedule**\n\n"
        f"📅 Start: `{st['start_time'].strftime('%d-%b %H:%M')}`\n"
        f"🔁 Repeat: `{st.get('interval') or 'None'}`\n"
        f"📌 Pin: {'✅' if st['pin'] else '❌'} | 🗑 Del Old: {'✅' if st['del'] else '❌'}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Pin: {st['pin']}", callback_data="toggle_pin"), InlineKeyboardButton(f"Del: {st['del']}", callback_data="toggle_del")],
        [InlineKeyboardButton("✅ CONFIRM", callback_data="confirm_schedule")]
    ])
    if is_edit: await message.edit_text(txt, reply_markup=kb)
    else: await message.reply(txt, reply_markup=kb)

async def handle_schedule_logic(uid, query, d):
    st = user_state[uid]
    if d == "date_now":
        st["start_time"] = datetime.datetime.now(IST)
        await ask_repetition(query.message, uid, is_edit=True)
    elif d == "repeat_none":
        st["interval"] = None
        await send_confirm_panel(query.message, uid, is_edit=True)
    elif d == "toggle_pin":
        st["pin"] = not st["pin"]
        await send_confirm_panel(query.message, uid, is_edit=True)
    elif d == "toggle_del":
        st["del"] = not st["del"]
        await send_confirm_panel(query.message, uid, is_edit=True)
    elif d == "confirm_schedule":
        tid = f"task_{int(datetime.datetime.now().timestamp())}"
        task = {
            "task_id": tid, "owner_id": uid, "chat_id": st["target_channel"], "msg_id": st["msg_id"],
            "source_chat": query.message.chat.id, "pin": st["pin"], "delete_old": st["del"],
            "repeat_interval": st.get("interval"), "start_time_iso": st["start_time"].isoformat(), "last_msg_id": None
        }
        data["tasks"][tid] = task; save_db(); add_job(tid, task)
        user_state[uid] = None
        await query.message.edit_text("✅ **Scheduled!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))

# --- Worker (UPDATED FIX) ---
def add_job(t_id, t):
    start_dt = datetime.datetime.fromisoformat(t["start_time_iso"])
    async def worker():
        try:
            print(f"DEBUG: Starting Task {t_id}")
            user = await get_user_client(t["owner_id"])
            if not user: 
                print("DEBUG: No User Client found!")
                return
            
            # --- PEER RESOLUTION FIX ---
            # We force the User Bot to 'find' the channel ID.
            # Without this, it gets PeerIdInvalid because it's a fresh session.
            try:
                print(f"DEBUG: Resolving Peer {t['chat_id']}")
                target_chat = await user.get_chat(int(t["chat_id"]))
                print(f"DEBUG: Found Channel: {target_chat.title}")
            except Exception as peer_err:
                print(f"DEBUG: Failed to resolve peer: {peer_err}")
                await user.stop()
                return

            if t["delete_old"] and t["last_msg_id"]:
                try: await user.delete_messages(int(t["chat_id"]), t["last_msg_id"])
                except: pass
            
            orig = await app.get_messages(t["source_chat"], t["msg_id"])
            sent = None
            
            print("DEBUG: Sending Message...")
            if orig.text: sent = await user.send_message(int(t["chat_id"]), orig.text, entities=orig.entities)
            elif orig.photo: sent = await user.send_photo(int(t["chat_id"]), orig.photo.file_id, caption=orig.caption, caption_entities=orig.caption_entities)
            elif orig.video: sent = await user.send_video(int(t["chat_id"]), orig.video.file_id, caption=orig.caption, caption_entities=orig.caption_entities)
            elif orig.document: sent = await user.send_document(int(t["chat_id"]), orig.document.file_id, caption=orig.caption, caption_entities=orig.caption_entities)

            if sent:
                print(f"DEBUG: Sent successfully! Msg ID: {sent.id}")
                if t["pin"]: 
                    try: 
                        print("DEBUG: Attempting to Pin...")
                        await sent.pin()
                        print("DEBUG: Pinned!")
                    except Exception as pin_e:
                        print(f"DEBUG: Pin Failed: {pin_e}")

                data["tasks"][t_id]["last_msg_id"] = sent.id
                save_db()
            else:
                print("DEBUG: Failed to send (sent is None)")

            await user.stop()
        except Exception as e:
            print(f"DEBUG: CRITICAL WORKER ERROR: {e}")

    trig = IntervalTrigger(start_date=start_dt, timezone=IST, **t["repeat_interval"]) if t["repeat_interval"] else DateTrigger(run_date=start_dt, timezone=IST)
    scheduler.add_job(worker, trig, id=t_id, replace_existing=True)

def parse_interval(t):
    try:
        p = t.split(); v = int(p[0])
        if "min" in p[1]: return {"minutes": v}
        if "hour" in p[1]: return {"hours": v}
        if "day" in p[1]: return {"days": v}
    except: return None

# --- Startup ---
async def boot_services():
    await app.start(); scheduler.start(); await idle(); await app.stop()

if __name__ == "__main__":
    load_db()
    for k, v in data["tasks"].items(): add_job(k, v)
    app.run(boot_services())
