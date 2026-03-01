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

# Existing Blacklist
BLACKLIST = [
    "casino", "stakeid", "stake", "bharosa", "punters", "service", 
    "download", "bonus", "right", "circle", "red", "bet", "exclusive", 
    "site", "platform", "registed", "khelo", "safe", "betting", "book"
]
BLACKLIST_REGEX = re.compile(r'\b(?:' + '|'.join(BLACKLIST) + r')\b', re.IGNORECASE)

# New Toss Winner Regex
TOSS_REGEX = re.compile(r'toss winner', re.IGNORECASE)

def init_db():
    """Initializes the SQLite database with an extra column for Toss tracking."""
    logger.info(f"Using database at: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # Added is_toss column (0 for normal, 1 for toss messages)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracked_msgs (
                channel_id INTEGER,
                msg_id INTEGER,
                is_toss INTEGER DEFAULT 0
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
    """Existing Feature: Checks if the message is a Poster (media + link)."""
    has_media = bool(message.photo or message.video or message.document)
    entities = message.caption_entities if message.caption else message.entities
    has_link = any(ent.type in ['url', 'text_link'] for ent in entities) if entities else False
    return has_media and has_link

def is_spam_message(message) -> bool:
    """Existing Feature: APK, Audio+Caption, Links, and Blacklist."""
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

async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main logic: Moderation + Toss Winner handling."""
    message = update.channel_post
    if not message:
        return

    channel_id = message.chat_id
    msg_id = message.message_id
    content_text = message.text or message.caption or ""
    
    is_poster = has_media_and_link(message)
    is_toss_msg = bool(TOSS_REGEX.search(content_text))

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # --- NEW FEATURE: TOSS WINNER REPLY ---
        if is_toss_msg:
            reply_text = "Always Play Toss In Small Limits \n\nAgr ID Me 10K Hai Toh Toss 1K Se Khelo Only...👆"
            try:
                reply_msg = await message.reply_text(reply_text)
                # Track original toss msg and bot's reply as 'is_toss=1'
                cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id, is_toss) VALUES (?, ?, 1)", (channel_id, msg_id))
                cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id, is_toss) VALUES (?, ?, 1)", (channel_id, reply_msg.message_id))
                logger.info(f"Toss Winner detected in {channel_id}. Reply sent.")
            except Exception as e:
                logger.error(f"Error replying to toss winner: {e}")

        # --- EXISTING FEATURE: NEW POSTER CLEANUP ---
        elif is_poster:
            logger.info(f"New Poster detected in {channel_id}. Running cleanup...")
            
            # Check if any messages we are about to delete were Toss messages
            cursor.execute("SELECT msg_id, is_toss FROM tracked_msgs WHERE channel_id = ?", (channel_id,))
            rows = cursor.fetchall()
            
            was_toss_involved = any(row[1] == 1 for row in rows)

            for (old_msg_id, _) in rows:
                try:
                    await context.bot.delete_message(chat_id=channel_id, message_id=old_msg_id)
                except BadRequest:
                    pass
            
            # Clear database for this channel
            cursor.execute("DELETE FROM tracked_msgs WHERE channel_id = ?", (channel_id,))
            
            # --- NEW FEATURE: SEND FOLLOW-UP IF TOSS WAS DELETED ---
            if was_toss_involved:
                follow_up_text = (
                    "As I Said Toss Normal Limit Se Hi Khelna Hota Hai \n\n "
                    "10% Amount Hi Loss Hua Hai Overall Hum Same Limit Se Play Krte He Hai Toh Profit Me Nikalte He Hai. \n\n"
                    "Baaki Session Me Cover Krte Hai...❤️"
                )
                await context.bot.send_message(chat_id=channel_id, text=follow_up_text)

            # Track the new poster and set state for the "next" message check
            cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id, is_toss) VALUES (?, ?, 0)", (channel_id, msg_id))
            cursor.execute("INSERT OR REPLACE INTO channel_state (channel_id, expecting_next) VALUES (?, 1)", (channel_id,))
            
        else:
            # --- EXISTING FEATURE: CHECK MESSAGE BELOW POSTER ---
            cursor.execute("SELECT expecting_next FROM channel_state WHERE channel_id = ?", (channel_id,))
            row = cursor.fetchone()
            
            if row and row[0] == 1:
                if is_spam_message(message):
                    logger.info(f"Subsequent message {msg_id} is spam. Tracking for deletion.")
                    cursor.execute("INSERT INTO tracked_msgs (channel_id, msg_id, is_toss) VALUES (?, ?, 0)", (channel_id, msg_id))
                
                cursor.execute("UPDATE channel_state SET expecting_next = 0 WHERE channel_id = ?", (channel_id,))
        
        conn.commit()

def main():
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_channel_post))
    
    logger.info("Bot is running with Toss Logic and Content Moderation...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
