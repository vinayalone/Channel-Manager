import os
import logging
import asyncpg
from html import escape
import re
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("CRITICAL: BOT_TOKEN environment variable is missing.")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is missing.")

LOG_CHAT_ID = int(os.environ.get("LOG_CHAT_ID", "0"))
if not LOG_CHAT_ID:
    raise ValueError("LOG_CHAT_ID environment variable is missing.")

db_pool = None

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ================= BLACKLIST =================

BLACKLIST = [
    "casino", "stakeid", "stake", "bharosa", "punters", "service",
    "download", "bonus", "circle", "red", "bet",
    "exclusive", "platform", "registed",
    "khelo", "safe", "betting", "book", "Guranteed", "apk"
]

BLACKLIST_REGEX = re.compile(
    r'\b(?:' + '|'.join(re.escape(w) for w in BLACKLIST) + r')\b',
    re.IGNORECASE
)

TOSS_REGEX = re.compile(r'toss winner', re.IGNORECASE)

# ================= DATABASE =================

async def init_postgres(application: Application):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)

    async with db_pool.acquire() as conn:
        # tracked_msgs stores ONLY the latest poster msg_id per channel
        # channel_state table no longer needed — removed expecting_next logic
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tracked_msgs (
                channel_id BIGINT PRIMARY KEY,
                msg_id BIGINT
            );
        """)

    logger.info("PostgreSQL connected and tables ready.")

# ================= HELPERS =================

def is_poster(message) -> bool:
    """Any photo or video = poster. Documents excluded (often spam APKs)."""
    return bool(message.photo or message.video)


def is_hard_spam(message) -> bool:
    """
    Only catches unambiguous spam:
    - APK file attachment
    - Audio/voice with caption
    - Blacklisted keywords (including 'apk', 'download', etc.)
    Links alone are NOT flagged — admin may post legit links after a poster.
    """
    if message.document and message.document.file_name:
        if message.document.file_name.lower().endswith('.apk'):
            return True

    if (message.audio or message.voice) and message.caption:
        return True

    text_to_check = message.text or message.caption or ""
    if text_to_check and BLACKLIST_REGEX.search(text_to_check):
        return True

    return False


def contains_link(message) -> bool:
    entities = message.caption_entities if message.caption else message.entities
    if entities and any(ent.type in ['url', 'text_link'] for ent in entities):
        return True
    text = message.text or message.caption or ""
    if "http://" in text.lower() or "https://" in text.lower() or "t.me" in text.lower():
        return True
    return False

# ================= TOSS FINISH =================

async def trigger_toss_finish(context, channel_id, reply_id, original_text):
    try:
        await context.bot.delete_message(chat_id=channel_id, message_id=reply_id)
    except Exception as e:
        logger.warning("Could not delete toss reply (msg_id=%s): %s", reply_id, e)

    safe_text = escape(original_text)
    final_message = (
        f"<b>{safe_text}</b>"
        "<b> Loss ❌</b>\n\n"
        "<b>As I Said Toss Normal Limit Se Hi Khelna Hota Hai</b>\n\n"
        "<b>10% Amount Hi Loss Hua Hai Overall Hum Same Limit Se Play Krte He Hai "
        "Toh Profit Me Nikalte He Hai.</b>\n\n"
        "<b>Baaki Session Me Cover Krte Hai...❤️</b>"
    )
    try:
        await context.bot.send_message(
            chat_id=channel_id,
            text=final_message,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error("Failed to send toss finish message: %s", e)

# ================= TOSS DELETION CHECK =================

async def check_single_toss(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    channel_id = data["channel_id"]
    original_id = data["original_id"]
    reply_id = data["reply_id"]
    original_text = data["original_text"]

    try:
        temp = await context.bot.copy_message(
            chat_id=LOG_CHAT_ID,
            from_chat_id=channel_id,
            message_id=original_id
        )
        await context.bot.delete_message(chat_id=LOG_CHAT_ID, message_id=temp.message_id)
        logger.info("Toss message still exists (channel=%s, msg=%s)", channel_id, original_id)

    except BadRequest as e:
        error_text = str(e).lower()
        logger.info("Toss copy check (channel=%s): %s", channel_id, error_text)
        if any(x in error_text for x in ["not found", "message_id_invalid", "message to copy not found"]):
            logger.info("Toss deleted — sending loss message (channel=%s)", channel_id)
            await trigger_toss_finish(context, channel_id, reply_id, original_text)

    except Exception as e:
        logger.error("Unexpected error in check_single_toss: %s", e)

# ================= MAIN HANDLER =================

async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.channel_post
    if not message:
        return

    channel_id = message.chat_id
    msg_id = message.message_id
    text = message.text or message.caption or ""

    # ---------- TOSS CHECK ----------
    if TOSS_REGEX.search(text) and not contains_link(message):
        reply_text = (
            "<b>Always Play Toss In Small Limits</b>\n\n"
            "<b>Agr ID Me 10K Hai Toh Toss 1K Se Khelo Only...👆</b>"
        )
        try:
            reply_msg = await message.reply_text(reply_text, parse_mode=ParseMode.HTML)
            context.job_queue.run_once(
                check_single_toss,
                when=20,
                data={
                    "channel_id": channel_id,
                    "original_id": msg_id,
                    "reply_id": reply_msg.message_id,
                    "original_text": text
                }
            )
        except Exception as e:
            logger.error("Failed to handle toss message: %s", e)
        return

    # ---------- MODERATION ----------
    if not db_pool:
        logger.error("Database pool not initialized.")
        return

    msg_is_poster = is_poster(message)

    async with db_pool.acquire() as conn:

        if msg_is_poster:
            # ── Delete the PREVIOUS poster ──
            row = await conn.fetchrow(
                "SELECT msg_id FROM tracked_msgs WHERE channel_id=$1", channel_id
            )
            if row:
                try:
                    await context.bot.delete_message(
                        chat_id=channel_id,
                        message_id=row["msg_id"]
                    )
                    logger.info("Deleted old poster (channel=%s, msg=%s)", channel_id, row["msg_id"])
                except BadRequest as e:
                    logger.warning("Old poster already gone (msg=%s): %s", row["msg_id"], e)
                except Exception as e:
                    logger.error("Could not delete old poster (msg=%s): %s", row["msg_id"], e)

            # ── Store ONLY the new poster ID ──
            await conn.execute("""
                INSERT INTO tracked_msgs(channel_id, msg_id) VALUES($1, $2)
                ON CONFLICT(channel_id) DO UPDATE SET msg_id = EXCLUDED.msg_id
            """, channel_id, msg_id)

            logger.info("New poster tracked (channel=%s, msg=%s)", channel_id, msg_id)

        else:
            # ── KEY FIX: No DB flag needed. Check if this msg immediately follows the poster ──
            # msg_id is always sequential in Telegram channels.
            # If this message's ID == poster_msg_id + 1, it's the message right after the poster.
            # This is instant, atomic, and has zero race condition risk.
            row = await conn.fetchrow(
                "SELECT msg_id FROM tracked_msgs WHERE channel_id=$1", channel_id
            )

            if row and msg_id == row["msg_id"] + 1:
                if is_hard_spam(message):
                    try:
                        await context.bot.delete_message(
                            chat_id=channel_id,
                            message_id=msg_id
                        )
                        logger.info("Deleted spam after poster (channel=%s, msg=%s)", channel_id, msg_id)
                    except Exception as e:
                        logger.warning("Could not delete spam (msg=%s): %s", msg_id, e)

# ================= ENTRY POINT =================

def main():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(init_postgres)
        .build()
    )
    application.add_handler(
        MessageHandler(filters.ChatType.CHANNEL, handle_channel_post)
    )
    logger.info("Bot started successfully.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
