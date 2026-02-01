import os
import json
import logging
import asyncio
import datetime
import pytz
from typing import Dict, List, Optional, Any

from pyrogram import Client, filters, idle, errors, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, InputFile
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
DB_FILE = "data.json"
DEFAULT_DATA = {"sessions": {}, "tasks": {}, "channels": {}, "logs": {}}
MAX_RETRIES = 1  # For task retries

# --- Global State ---
data = DEFAULT_DATA.copy()
login_state: Dict[int, Dict[str, Any]] = {}
user_state: Dict[int, Dict[str, Any]] = {}

# --- Core Bot Class ---
class BotManager:
    def __init__(self):
        self.app = Client("manager_interface", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
        self.scheduler = AsyncIOScheduler(timezone=IST, job_defaults={'misfire_grace_time': 60})
        self.load_db()

    def load_db(self) -> None:
        """Load database from file, with error handling."""
        if os.path.exists(DB_FILE):
            try:
                with open(DB_FILE, "r") as f:
                    loaded = json.load(f)
                    data.update(loaded)  # Merge with defaults
            except json.JSONDecodeError as e:
                logging.error(f"Database corrupted: {e}. Starting fresh.")
                data.update(DEFAULT_DATA)

    def save_db(self) -> None:
        """Save database to file."""
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=4, default=str)

    def is_authorized(self, user_id: int) -> bool:
        """Check if user is authorized."""
        return not ADMIN_IDS or user_id in ADMIN_IDS

    def get_user_client(self, user_id: int) -> Optional[Client]:
        """Get a user client from session string."""
        session = data["sessions"].get(str(user_id))
        if not session:
            return None
        return Client(f":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session, no_updates=True)

    def parse_interval(self, text: str) -> Optional[Dict[str, int]]:
        """Parse interval string (e.g., '1 day' -> {'days': 1})."""
        try:
            parts = text.split()
            value = int(parts[0])
            unit = parts[1].lower()
            if "min" in unit:
                return {"minutes": value}
            elif "hour" in unit:
                return {"hours": value}
            elif "day" in unit:
                return {"days": value}
            elif "week" in unit:
                return {"weeks": value}
            elif "month" in unit:
                return {"days": value * 30}  # Approximation
        except (ValueError, IndexError):
            return None
        return None

    def get_next_run_time(self, start_dt: datetime.datetime, interval: Optional[Dict[str, int]]) -> str:
        """Calculate next run time."""
        if not interval:
            return "One Time"
        now = datetime.datetime.now(IST)
        next_run = start_dt
        delta = datetime.timedelta(**interval)
        while next_run < now:
            next_run += delta
        return next_run.strftime('%d-%b %H:%M')

    async def notify_admin(self, user_id: int, message: str) -> None:
        """Send a notification to the admin."""
        try:
            await self.app.send_message(user_id, f"🔔 **Notification:** {message}")
        except Exception as e:
            logging.error(f"Failed to notify admin {user_id}: {e}")

    async def boot_services(self) -> None:
        """Start the bot and scheduler."""
        await self.app.start()
        self.scheduler.start()
        # Reload existing tasks
        for task_id, task in data["tasks"].items():
            self.add_job(task_id, task)
        await idle()
        await self.app.stop()

# --- Instantiate Manager ---
manager = BotManager()

# --- Commands ---
@manager.app.on_message(filters.command("manage"))
async def cmd_manage(client: Client, message: Message) -> None:
    if not manager.is_authorized(message.from_user.id):
        return
    uid = str(message.from_user.id)
    if uid not in data["sessions"]:
        await message.reply_text(
            "👋 **Welcome to Channel Manager!**\nPlease log in to get started.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Connect Account", callback_data="login_start")]])
        )
    else:
        await show_main_menu(message)

@manager.app.on_message(filters.command("help"))
async def cmd_help(client: Client, message: Message) -> None:
    if not manager.is_authorized(message.from_user.id):
        return
    help_text = (
        "🤖 **Channel Manager Help**\n\n"
        "• /manage - Open dashboard\n"
        "• /help - Show this\n"
        "• /export - Download data\n"
        "• /import - Upload data\n\n"
        "Features: Schedule posts, manage channels, edit tasks, view history."
    )
    await message.reply_text(help_text)

@manager.app.on_message(filters.command("export"))
async def cmd_export(client: Client, message: Message) -> None:
    if not manager.is_authorized(message.from_user.id):
        return
    manager.save_db()
    await message.reply_document(InputFile(DB_FILE), caption="📁 **Data Exported**")

@manager.app.on_message(filters.document & filters.private)
async def cmd_import(client: Client, message: Message) -> None:
    if not manager.is_authorized(message.from_user.id) or message.document.file_name != "data.json":
        return
    # Download and merge (with confirmation)
    file_path = await message.download()
    try:
        with open(file_path, "r") as f:
            imported = json.load(f)
        # Simple merge (you can add conflict resolution)
        data.update(imported)
        manager.save_db()
        await message.reply_text("✅ **Data Imported Successfully!**")
    except Exception as e:
        await message.reply_text(f"❌ **Import Failed:** {e}")
    os.remove(file_path)

# --- Callbacks ---
@manager.app.on_callback_query()
async def callback_handler(client: Client, query: CallbackQuery) -> None:
    uid = query.from_user.id
    if not manager.is_authorized(uid):
        return
    data_cmd = query.data

    if data_cmd == "menu_home":
        user_state[uid] = None
        await show_main_menu(query.message)

    elif data_cmd == "logout_confirm":
        await query.message.edit_text(
            "⚠️ **Confirm Logout?**\nThis will remove your session.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔴 Yes", callback_data="logout_final"), InlineKeyboardButton("🔙 No", callback_data="menu_home")]])
        )

    elif data_cmd == "logout_final":
        if str(uid) in data["sessions"]:
            del data["sessions"][str(uid)]
            manager.save_db()
        await query.message.edit_text("✅ **Logged Out Successfully.**")

    elif data_cmd == "add_channel":
        user_state[uid] = {"step": "waiting_forward"}
        await query.message.edit_text(
            "📝 **Add Channel (Step 1/1)**\nForward a message from the channel to add it.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]])
        )

    elif data_cmd == "list_channels":
        await show_channels_list(uid, query.message)

    elif data_cmd.startswith("manage_ch_"):
        await show_channel_dashboard(uid, query.message, data_cmd.split("_")[2])

    elif data_cmd.startswith("rem_ch_"):
        c_id = data_cmd.split("_")[2]
        if str(uid) in data["channels"]:
            del data["channels"][str(uid)][c_id]
            manager.save_db()
        await query.answer("Channel Removed")
        await show_channels_list(uid, query.message)

    elif data_cmd.startswith("new_post_"):
        c_id = data_cmd.split("_")[2]
        user_state[uid] = {"step": "waiting_content", "target_channel": c_id}
        await query.message.edit_text(
            "1️⃣ **Step 1/4: Content**\nSend text/photo/video to post.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data=f"manage_ch_{c_id}")]])
        )

    elif data_cmd.startswith("view_tasks_"):
        await show_active_tasks(uid, query.message, data_cmd.split("_")[2], page=0)

    elif data_cmd.startswith("edit_task_"):
        t_id = data_cmd.split("_")[2]
        user_state[uid] = {"step": "editing_task", "task_id": t_id}
        await show_edit_task(uid, query.message, t_id)

    elif data_cmd.startswith("del_task_"):
        t_id = data_cmd.split("_")[1]
        c_id = data["tasks"][t_id]["chat_id"]
        try:
            manager.scheduler.remove_job(t_id)
        except:
            pass
        del data["tasks"][t_id]
        manager.save_db()
        await query.answer("Task Stopped")
        await show_active_tasks(uid, query.message, c_id, page=0)

    elif data_cmd.startswith("view_history_"):
        await show_task_history(uid, query.message, data_cmd.split("_")[2])

    elif data_cmd.startswith("broadcast_"):
        user_state[uid] = {"step": "waiting_broadcast"}
        await query.message.edit_text(
            "📢 **Broadcast Message**\nSend content to post in all channels.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]])
        )

    elif data_cmd in ["date_now", "repeat_none", "toggle_pin", "toggle_del", "confirm_schedule"]:
        await handle_schedule_logic(uid, query, data_cmd)

    elif data_cmd == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await query.message.edit_text("📱 **Login (Step 1/3)**\nEnter phone number (e.g., +919876543210)")

# --- Message Handlers ---
@manager.app.on_message(filters.private)
async def message_handler(client: Client, message: Message) -> None:
    uid = message.from_user.id
    if not manager.is_authorized(uid):
        return
    text = message.text

    if uid in login_state:
        await handle_login_input(client, message, uid)
        return

    state = user_state.get(uid, {})
    step = state.get("step")

    if step == "waiting_forward":
        if message.forward_from_chat:
            chat = message.forward_from_chat
            if str(uid) not in data["channels"]:
                data["channels"][str(uid)] = {}
            data["channels"][str(uid)][str(chat.id)] = chat.title
            manager.save_db()
            user_state[uid] = None
            await message.reply(
                f"✅ **Channel Added:** {chat.title}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]])
            )
        else:
            await message.reply("⚠️ Please forward from a channel.")

    elif step == "waiting_content":
        state.update({"msg_id": message.id, "step": "waiting_date"})
        now_str = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        await message.reply(
            f"2️⃣ **Step 2/4: Timing** (IST)\nCurrent: `{now_str}`\nType `YYYY-MM-DD HH:MM` or click:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ Post Now", callback_data="date_now")]])
        )

    elif step == "waiting_date":
        try:
            dt = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M")
            user_state[uid]["start_time"] = IST.localize(dt)
            await ask_repetition(message, uid, is_edit=False)
        except ValueError:
            await message.reply("⚠️ Invalid format. Use `YYYY-MM-DD HH:MM`.")

    elif step == "waiting_repeat":
        interval = manager.parse_interval(text)
        if interval:
            user_state[uid]["interval"] = interval
            await send_confirm_panel(message, uid, is_edit=False)
        else:
            await message.reply("⚠️ Try `1 day`, `2 hours`, `1 week`, etc.")

    elif step == "waiting_broadcast":
        # Broadcast to all channels
        user_chs = data["channels"].get(str(uid), {})
        for c_id in user_chs:
            try:
                user_client = manager.get_user_client(uid)
                async with user_client as uc:
                    await uc.copy_message(int(c_id), message.chat.id, message.id)
            except Exception as e:
                logging.error(f"Broadcast failed for {c_id}: {e}")
        user_state[uid] = None
        await message.reply("✅ **Broadcast Sent!**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Menu", callback_data="menu_home")]]))

# --- Login Logic ---
async def handle_login_input(client: Client, message: Message, uid: int) -> None:
    state = login_state[uid]
    text = message.text

    if state["step"] == "waiting_phone":
        phone = text.replace(" ", "")
        status_msg = await message.reply("🔄 Connecting... Please wait.")
        temp = Client(f"sess_{uid}", api_id=API_ID, api_hash=API_HASH)
        await temp.connect()
        try:
            sent = await temp.send_code(phone)
            state.update({"client": temp, "phone": phone, "hash": sent.phone_code_hash, "step": "waiting_code"})
            await status_msg.edit_text("📩 **Code Sent!**\nFormat: Add `aa` before code (e.g., `aa12345`)")
        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {e}")
            await temp.disconnect()

    elif state["step"] == "waiting_code":
        code = text.lower().replace("aa", "").replace(" ", "")
        try:
            await state["client"].sign
