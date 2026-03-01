import os
import logging
import sqlite3
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

# Allow DB path to be overridden via env var for persistent volume mounting
DB_PATH = os.environ.get("DB_PATH", "bot_state.db")

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
    """Checks if the message contains both media (poster) and a link."""
    has_media = bool(message.photo or message.video or message.document)
    entities = message.caption_entities if message.caption else message.entities
    has_link = any(ent.type in ['url', 'text_link'] for ent in entities) if entities else False
    return has_media and has_link

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
            logger.info(f"New Poster+Link detected in {channel_id}. Deleting previous...")
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
                logger.info(f"Tracking subsequent message {msg_id} in {channel_id}.")
                cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id) VALUES (?, ?)", (channel_id, msg_id))
                cursor.execute("UPDATE channel_state SET expecting_next = 0 WHERE channel_id = ?", (channel_id,))
        
        conn.commit()

def main():
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_channel_post))
    
    logger.info("Bot is running...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
