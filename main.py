import os
import logging
import sqlite3
import re
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Fetch environment variables
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("CRITICAL: BOT_TOKEN environment variable is missing.")

DB_PATH = os.environ.get("DB_PATH", "bot_state.db")

# Existing Blacklist Features
BLACKLIST = [
    "casino", "stakeid", "stake", "bharosa", "punters", "service", 
    "download", "bonus", "right", "circle", "red", "bet", "exclusive", 
    "site", "platform", "registed", "khelo", "safe", "betting", "book"
]
BLACKLIST_REGEX = re.compile(r'\b(?:' + '|'.join(BLACKLIST) + r')\b', re.IGNORECASE)

# New Toss Winner Pattern
TOSS_REGEX = re.compile(r'toss winner', re.IGNORECASE)

def init_db():
    """Initializes the SQLite database with all tables."""
    logger.info(f"Using database at: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # Table for existing Poster/Moderation tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracked_msgs (
                channel_id INTEGER,
                msg_id INTEGER
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_state (
                channel_id INTEGER PRIMARY KEY,
                expecting_next BOOLEAN
            )
        """)
        # New Table for Toss Monitoring
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS toss_tracker (
                channel_id INTEGER,
                original_id INTEGER PRIMARY KEY,
                bot_reply_id INTEGER
            )
        """)
        conn.commit()

def has_media_and_link(message) -> bool:
    """Existing Feature: Checks if the message is a Poster (contains media AND a link)."""
    has_media = bool(message.photo or message.video or message.document)
    entities = message.caption_entities if message.caption else message.entities
    has_link = any(ent.type in ['url', 'text_link'] for ent in entities) if entities else False
    return has_media and has_link

def is_spam_message(message) -> bool:
    """Existing Feature: APKs, Links, Audio+Caption, and Blacklist."""
    entities = message.caption_entities if message.caption else message.entities
    if entities and any(ent.type in ['url', 'text_link'] for ent in entities):
        return True
    if message.document and message.document.file_name:
        if message.document.file_name.lower().endswith('.apk'):
            return True
    if (message.audio or message.voice) and message.caption:
        return True
    text_to_check = message.text or message.caption or ""
    if text_to_check and BLACKLIST_REGEX.search(text_to_check):
        return True
    return False

async def check_for_deletions(context: ContextTypes.DEFAULT_TYPE):
    """New Feature: Background Monitor runs every 2 minutes."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT channel_id, original_id, bot_reply_id FROM toss_tracker")
        active_tosses = cursor.fetchall()

    for channel_id, original_id, bot_reply_id in active_tosses:
        # Check if message is still there by attempting to delete it
        # If it fails with 'not found', YOU deleted it. 
        # If it succeeds, WE just deleted it (counting as 'gone').
        try:
            await context.bot.delete_message(chat_id=channel_id, message_id=original_id)
            await trigger_toss_finish(context, channel_id, original_id, bot_reply_id)
        except BadRequest as e:
            if "message to delete not found" in str(e).lower():
                await trigger_toss_finish(context, channel_id, original_id, bot_reply_id)

async def trigger_toss_finish(context, channel_id, original_id, bot_reply_id):
    """Deletes the reply and sends the bold follow-up message."""
    try: 
        await context.bot.delete_message(chat_id=channel_id, message_id=bot_reply_id)
    except: 
        pass
    
    follow_up = (
        "**As I Said Toss Normal Limit Se Hi Khelna Hota Hai \n\n "
        "10% Amount Hi Loss Hua Hai Overall Hum Same Limit Se Play Krte He Hai Toh Profit Me Nikalte He Hai. \n\n"
        "Baaki Session Me Cover Krte Hai...❤️**"
    )
    await context.bot.send_message(chat_id=channel_id, text=follow_up, parse_mode=ParseMode.MARKDOWN)
    
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM toss_tracker WHERE original_id = ?", (original_id,))

async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main process for all features."""
    message = update.channel_post
    if not message:
        return

    channel_id = message.chat_id
    msg_id = message.message_id
    text = (message.text or message.caption or "")
    
    # 1. New Feature: Toss Winner Check
    if TOSS_REGEX.search(text):
        reply_text = "**Always Play Toss In Small Limits \n\nAgr ID Me 10K Hai Toh Toss 1K Se Khelo Only...👆**"
        reply_msg = await message.reply_text(reply_text, parse_mode=ParseMode.MARKDOWN)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("INSERT OR REPLACE INTO toss_tracker VALUES (?, ?, ?)", 
                         (channel_id, msg_id, reply_msg.message_id))
        return

    # 2. Existing Feature: Poster/Moderation Logic
    is_poster = has_media_and_link(message)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        if is_poster:
            cursor.execute("SELECT msg_id FROM tracked_msgs WHERE channel_id = ?", (channel_id,))
            old_msgs = cursor.fetchall()
            for (old_msg_id,) in old_msgs:
                try: await context.bot.delete_message(chat_id=channel_id, message_id=old_msg_id)
                except: pass
            
            cursor.execute("DELETE FROM tracked_msgs WHERE channel_id = ?", (channel_id,))
            cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id) VALUES (?, ?)", (channel_id, msg_id))
            cursor.execute("INSERT OR REPLACE INTO channel_state (channel_id, expecting_next) VALUES (?, 1)", (channel_id,))
        else:
            cursor.execute("SELECT expecting_next FROM channel_state WHERE channel_id = ?", (channel_id,))
            row = cursor.fetchone()
            if row and row[0] == 1:
                if is_spam_message(message):
                    cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id) VALUES (?, ?)", (channel_id, msg_id))
                cursor.execute("UPDATE channel_state SET expecting_next = 0 WHERE channel_id = ?", (channel_id,))
        conn.commit()

def main():
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Existing Handler
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_channel_post))
    
    # Background Job: Checks for Toss deletions every 2 minutes (120 seconds)
    application.job_queue.run_repeating(check_for_deletions, interval=120, first=10)
    
    logger.info("Bot is running with ALL features merged...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
