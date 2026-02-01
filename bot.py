import os
import logging
from io import StringIO
from datetime import datetime, timedelta
from telethon import TelegramClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import timezone
import asyncio
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float
from sqlalchemy.orm import sessionmaker, declarative_base

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
DB_URL = os.getenv('DB_URL', 'sqlite:///bot.db')  # Use Railway's PostgreSQL URL

# Database setup
engine = create_engine(DB_URL)
Base = declarative_base()

class UserSession(Base):
    __tablename__ = 'sessions'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True)
    phone = Column(String)
    session_string = Column(Text)

class Channel(Base):
    __tablename__ = 'channels'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    channel_id = Column(Integer)
    channel_name = Column(String)

class ScheduledPost(Base):
    __tablename__ = 'posts'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    channel_id = Column(Integer)
    content = Column(Text)
    media_path = Column(String)  # File path for media
    schedule_time = Column(DateTime)
    repeat_interval = Column(String)  # e.g., "1h", "30m", "2d", "10s"
    pin = Column(Boolean, default=False)
    delete_old = Column(Boolean, default=False)
    last_msg_id = Column(Integer)  # For deleting old messages

class UserSettings(Base):
    __tablename__ = 'settings'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True)
    auto_pin = Column(Boolean, default=False)
    notifications = Column(Boolean, default=True)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Global variables
clients = {}  # user_id: TelegramClient
scheduler = AsyncIOScheduler(jobstores={'default': SQLAlchemyJobStore(url=DB_URL)}, timezone=timezone('Asia/Kolkata'))
scheduler.start()
app = Application.builder().token(BOT_TOKEN).build()

# Helper functions
async def load_sessions():
    """Load existing user sessions on startup."""
    session = Session()
    for user in session.query(UserSession).all():
        client = TelegramClient(StringIO(user.session_string), API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            clients[user.user_id] = client
    session.close()

async def post_to_channel(client, channel_id, content, media_path, pin, delete_old, last_msg_id=None):
    """Post content to channel using Telethon."""
    try:
        if media_path:
            msg = await client.send_file(channel_id, media_path, caption=content)
        else:
            msg = await client.send_message(channel_id, content)
        if pin:
            await client.pin_message(channel_id, msg.id)
        if delete_old and last_msg_id:
            await client.delete_messages(channel_id, [last_msg_id])
        return msg.id
    except Exception as e:
        logger.error(f"Error posting to channel {channel_id}: {e}")
        return None

def parse_repeat(interval_str):
    """Parse repeat interval (e.g., '1h' -> timedelta(hours=1))."""
    if not interval_str:
        return None
    unit = interval_str[-1]
    value = float(interval_str[:-1])
    if unit == 'h':
        return timedelta(hours=value)
    elif unit == 'm':
        return timedelta(minutes=value)
    elif unit == 'd':
        return timedelta(days=value)
    elif unit == 's':
        return timedelta(seconds=value)
    return None

# Command handlers
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provide help and usage guide."""
    text = (
        "Welcome to the Channel Manager Bot!\n\n"
        "Commands:\n"
        "/start - Begin login process.\n"
        "/manage - Access main menu (requires login).\n"
        "/help - Show this guide.\n\n"
        "Features:\n"
        "- Login with phone, OTP (reply as 'aa[code]'), and 2FA.\n"
        "- Add channels by forwarding a message.\n"
        "- Post content (text/media) to channels, with scheduling and repetition.\n"
        "- Manage scheduled posts in 'Tasks'.\n"
        "- Settings for auto-pin and notifications.\n\n"
        "Tip: Always follow the prompts for smooth operation."
    )
    await update.message.reply_text(text)

async def manage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main menu access."""
    user_id = update.effective_user.id
    session = Session()
    user_session = session.query(UserSession).filter_by(user_id=user_id).first()
    session.close()
    if user_session:
        await show_main_menu(update, context)
    else:
        await update.message.reply_text("You are not logged in. Please use /start to log in first. This will guide you through phone verification.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start login process."""
    user_id = update.effective_user.id
    session = Session()
    if session.query(UserSession).filter_by(user_id=user_id).first():
        await update.message.reply_text("You are already logged in. Use /manage to access the menu.")
    else:
        await update.message.reply_text("Let's log you in. Send your phone number in international format (e.g., +1234567890). This is required for Telegram authentication.")
        context.user_data['awaiting_phone'] = True
    session.close()

# Message handler
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    session_db = Session()

    if context.user_data.get('awaiting_phone'):
        phone = text
        client = TelegramClient(f'session_{user_id}', API_ID, API_HASH)
        await client.connect()
        try:
            sent = await client.send_code_request(phone)
            context.user_data['phone'] = phone
            context.user_data['client'] = client
            context.user_data['phone_code_hash'] = sent.phone_code_hash
            await update.message.reply_text("OTP sent to your phone. Reply with 'aa[code]' (e.g., aa12345) to prevent expiration. This code is for verification.")
            context.user_data['awaiting_otp'] = True
        except Exception as e:
            await update.message.reply_text(f"Error sending OTP: {str(e)}. Please check your phone number and try /start again.")
        del context.user_data['awaiting_phone']

    elif context.user_data.get('awaiting_otp'):
        if text.startswith('aa'):
            code = text[2:]
            client = context.user_data['client']
            try:
                await client.sign_in(context.user_data['phone'], code, phone_code_hash=context.user_data['phone_code_hash'])
                if await client.is_user_authorized():
                    session_string = client.session.save()
                    session_db.add(UserSession(user_id=user_id, phone=context.user_data['phone'], session_string=session_string))
                    session_db.commit()
                    clients[user_id] = client
                    await update.message.reply_text("Login successful! Use /manage to access your channels.")
                else:
                    await update.message.reply_text("2FA required. Send your password for two-factor authentication.")
                    context.user_data['awaiting_2fa'] = True
            except Exception as e:
                await update.message.reply_text(f"Invalid OTP or error: {str(e)}. Try /start again.")
        else:
            await update.message.reply_text("Please reply with 'aa[code]' format.")
        del context.user_data['awaiting_otp']

    elif context.user_data.get('awaiting_2fa'):
        password = text
        client = context.user_data['client']
        try:
            await client.sign_in(password=password)
            session_string = client.session.save()
            session_db.add(UserSession(user_id=user_id, phone=context.user_data['phone'], session_string=session_string))
            session_db.commit()
            clients[user_id] = client
            await update.message.reply_text("Login successful with 2FA! Use /manage to proceed.")
        except Exception as e:
            await update.message.reply_text(f"2FA error: {str(e)}. Try /start again.")
        del context.user_data['awaiting_2fa']

    elif context.user_data.get('adding_channel'):
        if update.message.forward_from_chat:
            channel_id = update.message.forward_from_chat.id
            channel_name = update.message.forward_from_chat.title
            session_db.add(Channel(user_id=user_id, channel_id=channel_id, channel_name=channel_name))
            session_db.commit()
            await update.message.reply_text(f"Channel '{channel_name}' added successfully! You can now post to it. Use /manage to return to the menu.")
            await show_main_menu(update, context)
        else:
            await update.message.reply_text("Please forward a message from the channel you want to add. This ensures the correct channel is selected.")
        del context.user_data['adding_channel']

    elif context.user_data.get('awaiting_content'):
        content = update.message.text or update.message.caption
        media = update.message.photo[-1] if update.message.photo else update.message.document
        if media:
            file = await media.get_file()
            media_path = f"temp_{user_id}_{media.file_id}.jpg" if update.message.photo else f"temp_{user_id}_{media.file_id}"
            await file.download_to_drive(media_path)
            context.user_data['post_media'] = media_path
        context.user_data['post_content'] = content
        keyboard = [
            [InlineKeyboardButton("Yes", callback_data="schedule_yes"), InlineKeyboardButton("No", callback_data="schedule_no")]
        ]
        await update.message.reply_text("Content received. Do you want to schedule this post? (Scheduling allows posting at a specific time in IST.)", reply_markup=InlineKeyboardMarkup(keyboard))
        del context.user_data['awaiting_content']

    elif context.user_data.get('awaiting_schedule'):
        try:
            schedule_time = datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone('Asia/Kolkata'))
            context.user_data['schedule_time'] = schedule_time
            keyboard = [
                [InlineKeyboardButton("Hours", callback_data="repeat_h"), InlineKeyboardButton("Minutes", callback_data="repeat_m")],
                [InlineKeyboardButton("Days", callback_data="repeat_d"), InlineKeyboardButton("Seconds", callback_data="repeat_s")],
                [InlineKeyboardButton("No Repeat", callback_data="repeat_none")]
            ]
            await update.message.reply_text("Schedule time set. Choose a repeat interval (e.g., every X hours), or select 'No Repeat' for a one-time post.", reply_markup=InlineKeyboardMarkup(keyboard))
        except ValueError:
            await update.message.reply_text("Invalid format. Please enter time as 'YYYY-MM-DD HH:MM' in IST (e.g., 2023-12-25 14:30).")
        del context.user_data['awaiting_schedule']

    session_db.close()

# Callback handler
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id
    session_db = Session()

    if data == "add_channel":
        await query.edit_message_text("To add a channel, forward any message from it. This will register the channel for posting.")
        context.user_data['adding_channel'] = True

    elif data == "list_channels":
        channels = session_db.query(Channel).filter_by(user_id=user_id).all()
        if channels:
            keyboard = [[InlineKeyboardButton(ch.channel_name, callback_data=f"channel_{ch.id}")] for ch in channels]
            keyboard.append([InlineKeyboardButton("Back", callback_data="back_menu")])
            await query.edit_message_text("Select a channel to manage:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.edit_message_text("No channels added yet. Use 'Add Channel' first.")

    elif data.startswith("channel_"):
        channel_id = int(data.split("_")[1])
        context.user_data['selected_channel'] = channel_id
        keyboard = [
            [InlineKeyboardButton("New Post", callback_data="new_post")],
            [InlineKeyboardButton("Tasks", callback_data="tasks")]
        ]
        await query.edit_message_text("Channel selected. Choose an action:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "new_post":
        await query.edit_message_text("Send the content to post (text, photo, or document). After sending, you'll be asked about scheduling and options.")

    elif data == "tasks":
        posts = session_db.query(ScheduledPost).filter_by(user_id=user_id).all()
        if posts:
            keyboard = [[InlineKeyboardButton(f"Post {p.id}: {p.content[:20]}...", callback_data=f"task_{p.id}")] for p in posts]
            keyboard.append([InlineKeyboardButton("Back", callback_data="back_menu")])
            await query.edit_message_text("Scheduled posts (Tasks): Select one to edit/delete.", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.edit_message_text("No scheduled posts. Create a new post first.")

    elif data.startswith("task_"):
        post_id = int(data.split("_")[1])
        post = session_db.query(ScheduledPost).filter_by(id=post_id, user_id=user_id).first()
        keyboard = [
            [InlineKeyboardButton("Edit", callback_data=f"edit_{post_id}"), InlineKeyboardButton("Delete", callback_data=f"delete_{post_id}")]
        ]
        await query.edit_message_text(f"Post: {post.content}\nScheduled: {post.schedule_time}\nRepeat: {post.repeat_interval}\nActions:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("delete_"):
        post_id = int(data.split("_")[1])
        session_db.query(ScheduledPost).filter_by(id=post_id).delete()
        session_db.commit()
        await query.edit_message_text("Post deleted. Returning to Tasks.")
        # Refresh tasks
        await handle_callback(update, context)  # Recursive call to refresh

    elif data == "settings":
        settings = session_db.query(UserSettings).filter_by(user_id=user_id).first()
        if not settings:
            settings = UserSettings(user_id=user_id)
            session_db.add(settings)
            session_db.commit()
        keyboard = [
            [InlineKeyboardButton(f"Auto-Pin: {'On' if settings.auto_pin else 'Off'}", callback_data="toggle_pin")],
            [InlineKeyboardButton(f"Notifications: {'On' if settings.notifications else 'Off'}", callback_data="toggle_notif")],
            [InlineKeyboardButton("Back", callback_data="back_menu")]
        ]
        await query.edit_message_text("Settings: Toggle options below.", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "toggle_pin":
        settings = session_db.query(UserSettings).filter_by(user_id=user_id).first()
        settings.auto_pin = not settings.auto_pin
        session_db.commit()
        await query.edit_message_text(f"Auto-Pin {'enabled' if settings.auto_pin else 'disabled'}. Use 'Back' to return.")

    elif data == "toggle_notif":
        settings = session_db.query(UserSettings).filter_by(user_id=user_id).first()
        settings.notifications = not settings.notifications
        session_db.commit()
        await query.edit_message_text(f"Notifications {'enabled' if settings.notifications else 'disabled'}. Use 'Back' to return.")

    elif data == "logout_confirm":
        context.user_data['logout_attempts'] = 0
        await query.edit_message_text("Are you sure you want to logout? This will end your session. (1/3) Yes/No", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Yes", callback_data="logout_yes"), InlineKeyboardButton("No", callback_data="logout_no")]
        ]))

    elif data.startswith("logout_yes"):
        attempts = context.user_data.get('logout_attempts', 0) + 1
        context.user_data['logout_attempts'] = attempts # Update the count

        if attempts < 3:
            await query.edit_message_text(
                f"Are you sure? ({attempts+1}/3) Yes/No", 
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Yes", callback_data="logout_yes"), 
                     InlineKeyboardButton("No", callback_data="logout_no")]
                ]) # This closes the list and the Markup class
            ) # This closes the edit_message_text function
        else:
            # Logic for when they finally click 'Yes' the 3rd time
            user_id = update.effective_user.id
            if user_id in clients:
                await clients[user_id].disconnect()
                del clients[user_id]
            
            session_db.query(UserSession).filter_by(user_id=user_id).delete()
            session_db.commit()
            await query.edit_message_text("You have been logged out successfully.")
