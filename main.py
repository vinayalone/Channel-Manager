import os
import logging
import sqlite3
import re
from telegram import Update
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

# Compile the blacklist regex once at startup for high performance
BLACKLIST = [
    "casino", "stakeid", "stake", "bharosa", "punters", "service", 
    "download", "bonus", "right", "circle", "red", "bet", "exclusive", 
    "site", "platform", "registed", "khelo", "safe", "betting", "book"
]
# \b ensures we only match whole words (e.g., 'bet' won't match 'better')
BLACKLIST_REGEX = re.compile(r'\b(?:' + '|'.join(BLACKLIST) + r')\b', re.IGNORECASE)

def init_db():
    """Initializes the SQLite database to survive bot restarts."""
    logger.info(f"Using database at: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
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
        conn.commit()

def has_media_and_link(message) -> bool:
    """Checks if the message is a Poster (contains media AND a link)."""
    has_media = bool(message.photo or message.video or message.document)
    entities = message.caption_entities if message.caption else message.entities
    has_link = any(ent.type in ['url', 'text_link'] for ent in entities) if entities else False
    return has_media and has_link

def is_spam_message(message) -> bool:
    """Evaluates if the subsequent message violates the channel rules."""
    
    # 1. Check for Links (in text or media captions)
    entities = message.caption_entities if message.caption else message.entities
    if entities and any(ent.type in ['url', 'text_link'] for ent in entities):
        return True
        
    # 2. Check for APK files
    if message.document and message.document.file_name:
        if message.document.file_name.lower().endswith('.apk'):
            return True
            
    # 3. Check for Audio/Voice + Caption requirement
    # If it's audio or a voice note, and it HAS a caption, delete it.
    if (message.audio or message.voice) and message.caption:
        logger.info("Audio/Voice with caption detected. Triggering deletion.")
        return True
        
    # 4. Check for Blacklisted Words (case-insensitive)
    # We check both message.text (standard msg) and message.caption (media msg)
    text_to_check = message.text or message.caption or ""
    if text_to_check and BLACKLIST_REGEX.search(text_to_check):
        return True
        
    return False

async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Processes every new message in the channel."""
    message = update.channel_post
    if not message:
        return

    channel_id = message.chat_id
    msg_id = message.message_id
    is_poster = has_media_and_link(message)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        if is_poster:
            logger.info(f"New Poster+Link detected in {channel_id}. Deleting previous tracking...")
            cursor.execute("SELECT msg_id FROM tracked_msgs WHERE channel_id = ?", (channel_id,))
            old_msgs = cursor.fetchall()
            
            for (old_msg_id,) in old_msgs:
                try:
                    await context.bot.delete_message(chat_id=channel_id, message_id=old_msg_id)
                except BadRequest as e:
                    logger.warning(f"Could not delete {old_msg_id}: {e}")
            
            cursor.execute("DELETE FROM tracked_msgs WHERE channel_id = ?", (channel_id,))
            cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id) VALUES (?, ?)", (channel_id, msg_id))
            cursor.execute("INSERT OR REPLACE INTO channel_state (channel_id, expecting_next) VALUES (?, 1)", (channel_id,))
            
        else:
            cursor.execute("SELECT expecting_next FROM channel_state WHERE channel_id = ?", (channel_id,))
            row = cursor.fetchone()
            
            if row and row[0] == 1:
                # We are looking at the message immediately below the poster.
                if is_spam_message(message):
                    logger.info(f"Spam rules violated in subsequent message {msg_id}. Tracking for deletion.")
                    cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id) VALUES (?, ?)", (channel_id, msg_id))
                else:
                    logger.info(f"Subsequent message {msg_id} is clean. Ignoring.")
                
                # Reset the state so we stop evaluating 3rd, 4th, etc. messages
                cursor.execute("UPDATE channel_state SET expecting_next = 0 WHERE channel_id = ?", (channel_id,))
        
        conn.commit()

def main():
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_channel_post))
    
    logger.info("Bot is running with strict content moderation enabled...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
