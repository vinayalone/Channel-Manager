import os
import json
import logging
import asyncio
import datetime
import pytz
from typing import Dict, Optional, Any

from pyrogram import Client, filters, idle, errors
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

# --- Configuration ---
# Fallback to defaults or raise clear errors if Env vars are missing
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

if not API_ID or not API_HASH or not BOT_TOKEN:
    print("❌ Error: Missing API_ID, API_HASH, or BOT_TOKEN environment variables.")
    exit(1)

API_ID = int(API_ID)
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip()]

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Constants ---
DB_FILE = "data.json"
DEFAULT_DATA = {"sessions": {}, "tasks": {}, "channels": {}}

# --- Global State ---
data = DEFAULT_DATA.copy()
login_state: Dict[int, Dict[str, Any]] = {}
user_state: Dict[int, Dict[str, Any]] = {}

# --- Core Bot Class ---
class BotManager:
    def __init__(self):
        self.app = Client(
            "manager_interface",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN
        )
        # Grace time handles tasks missed while bot was restarting
        self.scheduler = AsyncIOScheduler(timezone=IST, job_defaults={'misfire_grace_time': 60})

    def load_db(self) -> None:
        if os.path.exists(DB_FILE):
            try:
                with open(DB_FILE, "r") as f:
                    loaded = json.load(f)
                    data.update(loaded)
            except json.JSONDecodeError:
                logging.error("Database corrupted. Starting fresh.")
                data.update(DEFAULT_DATA)

    def save_db(self) -> None:
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=4, default=str)

    def is_authorized(self, user_id: int) -> bool:
        return not ADMIN_IDS or user_id in ADMIN_IDS

    def get_user_client(self, user_id: int) -> Optional[Client]:
        session = data["sessions"].get(str(user_id))
        if not session:
            return None
        # Use :memory: to avoid creating session files for worker threads
        return Client(
            f":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session,
            no_updates=True,
            in_memory=True
        )

    def parse_interval(self, text: str) -> Optional[Dict[str, int]]:
        try:
            parts = text.split()
            value = int(parts[0])
            unit = parts[1].lower()
            if "min" in unit: return {"minutes": value}
            elif "hour" in unit: return {"hours": value}
            elif "day" in unit: return {"days": value}
        except Exception:
            return None
        return None

    def get_next_run_time(self, start_dt: datetime.datetime, interval: Optional[Dict[str, int]]) -> str:
        if not interval:
            return "One Time"
        now = datetime.datetime.now(IST)
        next_run = start_dt
        delta = datetime.timedelta(**interval)
        while next_run < now:
            next_run += delta
        return next_run.strftime('%d-%b %H:%M')

    # --- THE WORKER ---
    def add_job(self, t_id: str, t: Dict[str, Any]) -> None:
        try:
            start_dt = datetime.datetime.fromisoformat(t["start_time_iso"])
        except Exception:
            return

        async def worker():
            try:
                logging.info(f"Starting Task {t_id}")
                user_client = self.get_user_client(t["owner_id"])
                if not user_client:
                    logging.warning(f"Task {t_id} skipped: No user client.")
                    return

                # Async Context Manager prevents memory leaks
                async with user_client as user:
                    # 1. Resolve Peer
                    try:
                        await user.get_chat(int(t["chat_id"]))
                    except Exception as e:
                        logging.warning(f"Could not resolve target chat: {e}")

                    # 2. Delete Old
                    if t.get("delete_old") and t.get("last_msg_id"):
                        try:
                            await user.delete_messages(int(t["chat_id"]), t["last_msg_id"])
                        except Exception:
                            pass
                    
                    # 3. Fetch Original & Send
                    try:
                        orig = await self.app.get_messages(t["source_chat"], t["msg_id"])
                        if not orig or orig.empty:
                            raise ValueError("Message deleted")
                    except Exception:
                        # Auto-remove broken tasks
                        logging.error(f"Source message not found for {t_id}. Removing task.")
                        try:
                            self.scheduler.remove_job(t_id)
                        except Exception:
                            pass
                        if t_id in data["tasks"]:
                            del data["tasks"][t_id]
                            self.save_db()
                        return

                    sent = None
                    try:
                        if orig.text:
                            sent = await user.send_message(int(t["chat_id"]), orig.text, entities=orig.entities)
                        elif orig.photo:
                            sent = await user.send_photo(int(t["chat_id"]), orig.photo.file_id, caption=orig.caption, caption_entities=orig.caption_entities)
                        elif orig.video:
                            sent = await user.send_video(int(t["chat_id"]), orig.video.file_id, caption=orig.caption, caption_entities=orig.caption_entities)
                        elif orig.document:
                            sent = await user.send_document(int(t["chat_id"]), orig.document.file_id, caption=orig.caption, caption_entities=orig.caption_entities)
                    except Exception as e:
                        logging.error(f"Failed to send message: {e}")

                    if sent:
                        if t.get("pin"): 
                            try:
                                await sent.pin()
                            except Exception:
                                pass
                        data["tasks"][t_id]["last_msg_id"] = sent.id
                        self.save_db()

            except Exception as e:
                logging.error(f"Worker Critical Error: {e}")

        trigger = IntervalTrigger(start_date=start_dt, timezone=IST, **t["repeat_interval"]) if t["repeat_interval"] else DateTrigger(run_date=start_dt, timezone=IST)
        self.scheduler.add_job(worker, trigger, id=t_id, replace_existing=True)

    async def boot_services(self) -> None:
        self.load_db()
        await self.app.start()
        
        # Load tasks
        for k, v in data["tasks"].items():
            self.add_job(k, v)
            
        self.scheduler.start()
        print("✅ Bot Started Successfully")
        await idle()
        await self.app.stop()

# --- Instantiate Manager ---
manager = BotManager()

# --- Commands ---
@manager.app.on_message(filters.command("manage"))
async def cmd_manage(client: Client, message: Message):
    if not manager.is_authorized(message.from_user.id): return
    uid = str(message.from_user.id)
    if uid not in data["sessions"]:
        await message.reply_text("👋 **Welcome!**\nPlease log in.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Connect Account", callback_data="login_start")]]))
    else:
        await show_main_menu(message)

@manager.app.on_message(filters.command("start"))
async def cmd_start(client: Client, message: Message):
    if not manager.is_authorized(message.from_user.id): return
    await message.reply_text("👋 **Channel Manager Online**\nType /manage to begin.")

@manager.app.on_message(filters.command("export"))
async def cmd_export(client: Client, message: Message):
    if not manager.is_authorized(message.from_user.id): return
    manager.save_db()
    if os.path.exists(DB_FILE):
        await message.reply_document(DB_FILE, caption="📁 **Database Export**")
    else:
        await message.reply("No database found.")

@manager.app.on_message(filters.document & filters.private)
async def cmd_import(client: Client, message: Message):
    if not manager.is_authorized(message.from_user.id) or message.document.file_name != "data.json": return
    file_path = await message.download()
    try:
        with open(file_path, "r") as f:
            data.update(json.load(f))
        manager.save_db()
        await message.reply_text("✅ **Imported Successfully!**")
        # Reload tasks immediately
        for k, v in data["tasks"].items():
            manager.add_job(k, v)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")
    if os.path.exists(file_path):
        os.remove(file_path)

# --- Callbacks ---
@manager.app.on_callback_query()
async def callback_handler(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    if not manager.is_authorized(uid): return
    d = query.data

    if d == "menu_home":
        user_state[uid] = None
        await show_main_menu(query.message)

    elif d == "logout_confirm":
        await query.message.edit_text("⚠️ **Confirm Logout?**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔴 Yes", callback_data="logout_final"), InlineKeyboardButton("🔙 No", callback_data="menu_home")]]))

    elif d == "logout_final":
        if str(uid) in data["sessions"]: 
            del data["sessions"][str(uid)]
            manager.save_db()
        await query.message.edit_text("✅ **Logged Out.**")

    elif d == "add_channel":
        user_state[uid] = {"step": "waiting_forward"}
        await query.message.edit_text("📝 **Add Channel**\nForward a message from the channel.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]]))

    elif d == "list_channels":
        await show_channels_list(uid, query.message)

    elif d.startswith("manage_ch_"):
        await show_channel_dashboard(uid, query.message, d.split("_")[2])

    elif d.startswith("rem_ch_"):
        c_id = d.split("_")[2]
        if str(uid) in data["channels"] and c_id in data["channels"][str(uid)]:
            del data["channels"][str(uid)][c_id]
            manager.save_db()
        await query.answer("Removed")
        await show_channels_list(uid, query.message)

    elif d.startswith("new_post_"):
        c_id = d.split("_")[2]
        user_state[uid] = {"step": "waiting_content", "target_channel": c_id}
        await query.message.edit_text("1️⃣ **Content**\nSend Text/Photo/Video.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data=f"manage_ch_{c_id}")]]))

    elif d.startswith("view_tasks_"):
        await show_active_tasks(uid, query.message, d.split("_")[2])

    elif d.startswith("del_task_"):
        t_id = d.split("del_task_")[1]
        c_id = data["tasks"][t_id]["chat_id"]
        try: 
            manager.scheduler.remove_job(t_id)
        except Exception: 
            pass
        if t_id in data["tasks"]:
            del data["tasks"][t_id]
            manager.save_db()
        await query.answer("Stopped")
        await show_active_tasks(uid, query.message, c_id)

    elif d in ["date_now", "repeat_none", "toggle_pin", "toggle_del", "confirm_schedule"]:
        await handle_schedule_logic(uid, query, d)

    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await query.message.edit_text("📱 **Phone Number**\nEx: `+919876543210`")

# --- Message Handlers ---
@manager.app.on_message(filters.private)
async def message_handler(client: Client, message: Message):
    uid = message.from_user.id
    if not manager.is_authorized(uid): return
    text = message.text

    if uid in login_state:
        await handle_login_input(client, message, uid)
        return

    st = user_state.get(uid, {})
    step = st.get("step")

    if step == "waiting_forward":
        if message.forward_from_chat:
            chat = message.forward_from_chat
            if str(uid) not in data["channels"]: data["channels"][str(uid)] = {}
            data["channels"][str(uid)][str(chat.id)] = chat.title
            manager.save_db()
            user_state[uid] = None
            await message.reply(f"✅ **Added:** {chat.title}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu_home")]]))
        else:
            await message.reply("⚠️ Forward from a channel.")

    elif step == "waiting_content":
        st.update({"msg_id": message.id, "step": "waiting_date"})
        await message.reply("2️⃣ **Time** (IST)\nType `YYYY-MM-DD HH:MM` or:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ Now", callback_data="date_now")]]))

    elif step == "waiting_date":
        try:
            dt = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M")
            st["start_time"] = IST.localize(dt)
            await ask_repetition(message, uid, is_edit=False)
        except Exception:
            await message.reply("⚠️ Format: `YYYY-MM-DD HH:MM`")

    elif step == "waiting_repeat":
        interval = manager.parse_interval(text)
        if interval:
            st["interval"] = interval
            await send_confirm_panel(message, uid, is_edit=False)
        else:
            await message.reply("⚠️ Try `1 day`")

# --- Logic & UI Helpers ---

async def handle_login_input(client, message, uid):
    st = login_state[uid]
    text = message.text
    
    if st["step"] == "waiting_phone":
        phone = text.replace(" ", "")
        status = await message.reply("🔄 Connecting...")
        # Use in_memory=True to prevent file creation issues during login
        temp = Client(f"sess_{uid}", api_id=API_ID, api_hash=API_HASH, in_memory=True)
        await temp.connect()
        try:
            sent = await temp.send_code(phone)
            st.update({"client": temp, "phone": phone, "hash": sent.phone_code_hash, "step": "waiting_code"})
            await status.edit_text("📩 **Code Sent**\nFormat: `aa12345`")
        except Exception as e:
            await status.edit_text(f"❌ {e}")
            await temp.disconnect()

    elif st["step"] == "waiting_code":
        code = text.lower().replace("aa", "").replace(" ", "")
        try:
            await st["client"].sign_in(st["phone"], st["hash"], code)
            data["sessions"][str(uid)] = await st["client"].export_session_string()
            manager.save_db()
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Success!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Menu", callback_data="menu_home")]]))
        except errors.SessionPasswordNeeded:
            st["step"] = "waiting_pass"
            await message.reply("🔐 **2FA Password:**")
        except Exception as e:
            await message.reply(f"❌ {e}")

    elif st["step"] == "waiting_pass":
        try:
            await st["client"].check_password(text)
            data["sessions"][str(uid)] = await st["client"].export_session_string()
            manager.save_db()
            await st["client"].disconnect()
            del login_state[uid]
            await message.reply("✅ **Success!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Menu", callback_data="menu_home")]]))
        except Exception as e:
            await message.reply(f"❌ {e}")

async def show_main_menu(m):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📢 My Channels", callback_data="list_channels"), InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")], [InlineKeyboardButton("🚪 Logout", callback_data="logout_confirm")]])
    text = "🤖 **Manager Dashboard**"
    if isinstance(m, Message): await m.reply(text, reply_markup=kb)
    else: await m.edit_text(text, reply_markup=kb)

async def show_channels_list(uid, m):
    user_chs = data["channels"].get(str(uid), {})
    if not user_chs: return await m.edit_text("ℹ️ **No Channels**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add", callback_data="add_channel"), InlineKeyboardButton("🔙 Back", callback_data="menu_home")]]))
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"📢 {t}", callback_data=f"manage_ch_{c}")] for c, t in user_chs.items()] + [[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]])
    await m.edit_text("**Select Channel:**", reply_markup=kb)

async def show_channel_dashboard(uid, m, c_id):
    t = data["channels"].get(str(uid), {}).get(c_id, "Unknown")
    c = sum(1 for t in data["tasks"].values() if str(t["chat_id"]) == c_id)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✨ Post", callback_data=f"new_post_{c_id}"), InlineKeyboardButton(f"📋 Tasks ({c})", callback_data=f"view_tasks_{c_id}")], [InlineKeyboardButton("🗑 Remove", callback_data=f"rem_ch_{c_id}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]])
    await m.edit_text(f"⚙️ **{t}**", reply_markup=kb)

async def show_active_tasks(uid, m, c_id):
    tasks = {k:v for k,v in data["tasks"].items() if str(v["chat_id"]) == str(c_id)}
    if not tasks: return await m.edit_text("✅ **No Schedules**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"manage_ch_{c_id}")]]) )
    txt, kb = "**Active Schedules:**\n", []
    for tid, t in tasks.items():
        try:
            start_dt = datetime.datetime.fromisoformat(t["start_time_iso"])
            nxt = manager.get_next_run_time(start_dt, t["repeat_interval"])
            txt += f"• Next: `{nxt}`\n"
            kb.append([InlineKeyboardButton("🛑 Stop", callback_data=f"del_task_{tid}")])
        except Exception: continue
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
    st.setdefault("pin", False)
    st.setdefault("del", False)
    txt = f"⚙️ **Confirm**\n📅 Start: `{st['start_time'].strftime('%d-%b %H:%M')}`\n🔁 Repeat: `{st.get('interval') or 'None'}`\n📌 Pin: {st['pin']} | 🗑 Del Old: {st['del']}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"Pin: {st['pin']}", callback_data="toggle_pin"), InlineKeyboardButton(f"Del: {st['del']}", callback_data="toggle_del")], [InlineKeyboardButton("✅ CONFIRM", callback_data="confirm_schedule")]])
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
            "task_id": tid, 
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
        data["tasks"][tid] = task
        manager.save_db()
        manager.add_job(tid, task)
        user_state[uid] = None
        await query.message.edit_text("✅ **Scheduled!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))

if __name__ == "__main__":
    asyncio.run(manager.boot_services())
