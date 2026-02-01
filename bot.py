import os
import logging
from io import StringIO
from datetime import datetime, timedelta
from pytz import timezone

# Telethon imports
from telethon import TelegramClient
from telethon.sessions import StringSession

# Telegram Bot SDK imports
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# Scheduler and Database imports
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
DB_URL = os.getenv('DB_URL', 'sqlite:///bot.db')

# --- Database Models ---
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
    media_path = Column(String)
    schedule_time = Column(DateTime)
    repeat_interval = Column(String)
    pin = Column(Boolean, default=False)
    delete_old = Column(Boolean, default=False)
    last_msg_id = Column(Integer)

class UserSettings(Base):
    __tablename__ = 'settings'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True)
    auto_pin = Column(Boolean, default=False)
    notifications = Column(Boolean, default=True)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# --- Global Logic ---
clients = {}  # user_id: TelegramClient
scheduler = AsyncIOScheduler(
    jobstores={'default': SQLAlchemyJobStore(url=DB_URL)}, 
    timezone=timezone('Asia/Kolkata')
)

async def load_sessions():
    """Load existing user sessions on startup."""
    session_db = Session()
    for user in session_db.query(UserSession).all():
        # Using StringSession as Telethon expects the saved string directly
        client = TelegramClient(StringSession(user.session_string), API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            clients[user.user_id] = client
            logger.info(f"Loaded session for user {user.user_id}")
    session_db.close()

async def post_init(application: Application):
    """Initializes async components after the event loop starts."""
    if not scheduler.running:
        scheduler.start()
    await load_sessions()
    logger.info("Bot components initialized.")

# --- Helper Logic ---
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Add Channel", callback_data="add_channel"), InlineKeyboardButton("My Channels", callback_data="list_channels")],
        [InlineKeyboardButton("Settings", callback_data="settings"), InlineKeyboardButton("Logout", callback_data="logout_confirm")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    msg_text = "Main Menu: Manage your scheduled posts and channels."
    
    if update.message:
        await update.message.reply_text(msg_text, reply_markup=reply_markup)
    else:
        await update.callback_query.edit_message_text(msg_text, reply_markup=reply_markup)

# --- Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    session_db = Session()
    if session_db.query(UserSession).filter_by(user_id=user_id).first():
        await update.message.reply_text("You are already logged in.")
        await show_main_menu(update, context)
    else:
        await update.message.reply_text("Send your phone number in international format (e.g., +1234567890).")
        context.user_data['awaiting_phone'] = True
    session_db.close()

async def manage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)

# --- Callback Handler (The fixed Logic) ---
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id
    session_db = Session()

    if data == "logout_confirm":
        context.user_data['logout_attempts'] = 0
        await query.edit_message_text(
            "Are you sure you want to logout? (1/3) Yes/No", 
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes", callback_data="logout_yes"), InlineKeyboardButton("No", callback_data="logout_no")]
            ])
        )

    elif data == "logout_yes":
        attempts = context.user_data.get('logout_attempts', 0) + 1
        context.user_data['logout_attempts'] = attempts
        
        if attempts < 3:
            # FIXED: Closed all brackets and parentheses correctly here
            await query.edit_message_text(
                f"Are you sure? ({attempts+1}/3) Yes/No", 
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Yes", callback_data="logout_yes"), InlineKeyboardButton("No", callback_data="logout_no")]
                ])
            )
        else:
            if user_id in clients:
                await clients[user_id].disconnect()
                del clients[user_id]
            session_db.query(UserSession).filter_by(user_id=user_id).delete()
            session_db.commit()
            await query.edit_message_text("Logged out successfully. Use /start to log in again.")

    elif data == "logout_no" or data == "back_menu":
        await show_main_menu(update, context)

    # ... (other callback logic for channels/settings goes here)
    session_db.close()

# --- Main Entry Point ---
def main():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init) # Handles the 'no running event loop' issue
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("manage", manage))
    application.add_handler(CallbackQueryHandler(handle_callback))
    # application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot is starting...")
    application.run_polling()

if __name__ == "__main__":
    main()
