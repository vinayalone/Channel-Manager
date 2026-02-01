import asyncio
import logging
import re
import os
import sqlite3
from datetime import datetime, timedelta

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
import pytz

# --- CONFIGURATION (Loaded from Railway Variables) ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
except (TypeError, ValueError):
    print("Error: Missing or invalid Environment Variables in Railway.")
    exit(1)

# --- SETUP ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Define scheduler globally but DO NOT start it yet
scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Kolkata'))

# Main Bot Client
bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Database Setup
conn = sqlite3.connect('bot_data.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS sessions 
                  (user_id INTEGER PRIMARY KEY, session_string TEXT, phone TEXT)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS channels 
                  (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, channel_id INTEGER, channel_title TEXT, last_msg_id INTEGER)''')
conn.commit()

# --- STATE MANAGEMENT ---
login_states = {}
post_states = {}

# --- HELPER FUNCTIONS ---
async def get_user_client(user_id):
    cursor.execute("SELECT session_string FROM sessions WHERE user_id=?", (user_id,))
    res = cursor.fetchone()
    if res:
        client = TelegramClient(StringSession(res[0]), API_ID, API_HASH)
        await client.connect()
        return client
    return None

def save_session(user_id, client):
    s_str = StringSession.save(client.session)
    cursor.execute("REPLACE INTO sessions (user_id, session_string) VALUES (?, ?)", (user_id, s_str))
    conn.commit()

async def execute_post(user_id, post_data):
    user_client = await get_user_client(user_id)
    if not user_client: return

    cursor.execute("SELECT channel_id, last_msg_id FROM channels WHERE id=?", (post_data['channel_db_id'],))
    res = cursor.fetchone()
    if not res: return
    channel_id, last_msg_id = res

    try:
        if post_data.get('del_old') and last_msg_id:
            try:
                await user_client.delete_messages(channel_id, last_msg_id)
            except Exception as e:
                logging.error(f"Failed to delete: {e}")

        msg_content = post_data['content_msg']
        sent_msg = await user_client.send_message(channel_id, msg_content)
        
        if post_data.get('pin'):
            await user_client.pin_message(channel_id, sent_msg)
            
        cursor.execute("UPDATE channels SET last_msg_id=? WHERE id=?", (sent_msg.id, post_data['channel_db_id']))
        conn.commit()
        
    except Exception as e:
        logging.error(f"Post failed: {e}")

# --- BOT EVENTS ---

# *** FIX IS HERE: Start Scheduler when Bot connects ***
@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    if not scheduler.running:
        scheduler.start()
        logging.info("Scheduler Started inside Event Loop")
        
    user_id = event.sender_id
    login_states[user_id] = {'state': 'WAIT_PHONE'}
    await event.respond("Welcome! Please send your **Phone Number** (with country code) to login.\nExample: `+919876543210`")

@bot.on(events.NewMessage(pattern='/manage'))
async def manage_handler(event):
    user_id = event.sender_id
    client = await get_user_client(user_id)
    
    if client and await client.is_user_authorized():
        await show_main_menu(event)
    else:
        await event.respond("⚠️ You are not logged in.\nPlease type /start to begin the login process.")

async def show_main_menu(event):
    await event.respond("🤖 **Channel Manager**", buttons=[
        [Button.inline("Add Channel", b"btn_add_ch"), Button.inline("Channels", b"btn_list_ch")],
        [Button.inline("Logout", b"btn_logout")]
    ])

@bot.on(events.NewMessage)
async def message_handler(event):
    if event.text.startswith('/'): return
    
    user_id = event.sender_id
    state_data = login_states.get(user_id)
    post_data = post_states.get(user_id)

    if state_data:
        state = state_data['state']
        
        if state == 'WAIT_PHONE':
            phone = event.text.strip()
            user_client = TelegramClient(StringSession(), API_ID, API_HASH)
            await user_client.connect()
            
            try:
                await user_client.send_code_request(phone)
                login_states[user_id]['phone'] = phone
                login_states[user_id]['client'] = user_client
                login_states[user_id]['state'] = 'WAIT_CODE'
                await event.respond("✅ OTP Sent!\n\nPlease send the code in this format: `aa12345`\n(Add 'aa' before the code).")
            except Exception as e:
                await event.respond(f"Error: {e}")
                
        elif state == 'WAIT_CODE':
            raw_text = event.text.strip()
            if not raw_text.startswith('aa'):
                await event.respond("⚠️ Invalid format. Please start with 'aa' (e.g., aa12345).")
                return
            
            code = raw_text[2:]
            phone = state_data['phone']
            user_client = state_data['client']
            
            try:
                await user_client.sign_in(phone, code)
                save_session(user_id, user_client)
                await event.respond("✅ Login Successful!", buttons=[Button.inline("Open Menu", b"menu_main")])
                del login_states[user_id]
            except SessionPasswordNeededError:
                login_states[user_id]['state'] = 'WAIT_PASSWORD'
                await event.respond("🔐 Two-Step Verification enabled. Send Password.")
            except PhoneCodeInvalidError:
                await event.respond("❌ Invalid Code. Try again.")
            except Exception as e:
                await event.respond(f"Error: {e}")

        elif state == 'WAIT_PASSWORD':
            password = event.text.strip()
            user_client = state_data['client']
            try:
                await user_client.sign_in(password=password)
                save_session(user_id, user_client)
                await event.respond("✅ Login Successful!", buttons=[Button.inline("Open Menu", b"menu_main")])
                del login_states[user_id]
            except Exception as e:
                await event.respond(f"❌ Login Failed: {e}")

    elif post_data and post_data.get('state') == 'WAIT_CHANNEL_FWD':
        if event.fwd_from:
            chat_id = event.fwd_from.channel_id
            full_chat_id = int(f"-100{chat_id}")
            title = event.fwd_from.from_name or "Unknown Channel"
            
            cursor.execute("INSERT INTO channels (user_id, channel_id, channel_title) VALUES (?, ?, ?)", 
                           (user_id, full_chat_id, title))
            conn.commit()
            del post_states[user_id]
            await event.respond(f"✅ Channel **{title}** added!", buttons=[Button.inline("Back to Menu", b"menu_main")])

    elif post_data and post_data.get('state') == 'WAIT_CONTENT':
        post_states[user_id]['content_msg'] = event
        post_states[user_id]['state'] = 'WAIT_SCHEDULE_CONFIRM'
        await event.respond("Schedule this post?", 
                            buttons=[[Button.inline("Yes", b"sched_yes"), Button.inline("No, Post Now", b"sched_no")]])
    
    elif post_data and post_data.get('state') == 'WAIT_TIME':
        try:
            ist = pytz.timezone('Asia/Kolkata')
            dt = datetime.strptime(event.text, "%Y-%m-%d %H:%M")
            dt = ist.localize(dt)
            post_states[user_id]['run_date'] = dt
            post_states[user_id]['state'] = 'WAIT_REPEAT'
            await event.respond("Repetition? (e.g., `2 hours`). Type `no` to skip.")
        except ValueError:
            await event.respond("⚠️ Format: `YYYY-MM-DD HH:MM`")

    elif post_data and post_data.get('state') == 'WAIT_REPEAT':
        text = event.text.lower()
        if text != 'no':
            try:
                parts = text.split()
                amount = int(parts[0])
                unit = parts[1]
                if 'hour' in unit: kwargs = {'hours': amount}
                elif 'minute' in unit: kwargs = {'minutes': amount}
                elif 'day' in unit: kwargs = {'days': amount}
                elif 'second' in unit: kwargs = {'seconds': amount}
                else: raise ValueError
                post_states[user_id]['interval'] = kwargs
            except:
                await event.respond("⚠️ Invalid. Try `2 hours`.")
                return

        await event.respond("Final Settings:", buttons=[
            [Button.inline("📌 Pin: No", b"toggle_pin")],
            [Button.inline("🗑 Delete Old: No", b"toggle_del")],
            [Button.inline("✅ Confirm", b"finish_sched")]
        ])

@bot.on(events.CallbackQuery)
async def callback_handler(event):
    user_id = event.sender_id
    data = event.data.decode()
    
    if data == "menu_main":
        await show_main_menu(event)
    elif data == "btn_logout":
        cursor.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        conn.commit()
        await event.edit("👋 Logged out.")
    elif data == "btn_add_ch":
        post_states[user_id] = {'state': 'WAIT_CHANNEL_FWD'}
        await event.respond("Forward a message from the channel.")
    elif data == "btn_list_ch":
        cursor.execute("SELECT id, channel_title FROM channels WHERE user_id=?", (user_id,))
        channels = cursor.fetchall()
        if not channels:
            await event.edit("No channels.", buttons=[Button.inline("Back", b"menu_main")])
            return
        btns = [[Button.inline(c[1], f"ch_{c[0]}")] for c in channels]
        btns.append([Button.inline("Back", b"menu_main")])
        await event.edit("Select channel:", buttons=btns)
    elif data.startswith("ch_"):
        ch_db_id = int(data.split("_")[1])
        post_states[user_id] = {'channel_db_id': ch_db_id, 'pin': False, 'del_old': False}
        await event.edit("Options:", buttons=[[Button.inline("New Post", b"feat_new_post")], [Button.inline("Back", b"btn_list_ch")]])
    elif data == "feat_new_post":
        post_states[user_id]['state'] = 'WAIT_CONTENT'
        await event.respond("Send content.")
    elif data == "sched_no":
        await execute_post(user_id, post_states[user_id])
        await event.edit("✅ Posted!")
    elif data == "sched_yes":
        post_states[user_id]['state'] = 'WAIT_TIME'
        await event.respond("Enter time (`YYYY-MM-DD HH:MM`):")
    elif data == "toggle_pin":
        curr = post_states[user_id].get('pin', False)
        post_states[user_id]['pin'] = not curr
        await event.edit(buttons=[
            [Button.inline(f"📌 Pin: {'Yes' if not curr else 'No'}", b"toggle_pin")],
            [Button.inline(f"🗑 Delete Old: {'Yes' if post_states[user_id].get('del_old') else 'No'}", b"toggle_del")],
            [Button.inline("✅ Confirm", b"finish_sched")]
        ])
    elif data == "toggle_del":
        curr = post_states[user_id].get('del_old', False)
        post_states[user_id]['del_old'] = not curr
        await event.edit(buttons=[
            [Button.inline(f"📌 Pin: {'Yes' if post_states[user_id].get('pin') else 'No'}", b"toggle_pin")],
            [Button.inline(f"🗑 Delete Old: {'Yes' if not curr else 'No'}", b"toggle_del")],
            [Button.inline("✅ Confirm", b"finish_sched")]
        ])
    elif data == "finish_sched":
        data_store = post_states[user_id]
        if 'interval' in data_store:
            trigger = IntervalTrigger(start_date=data_store['run_date'], timezone=pytz.timezone('Asia/Kolkata'), **data_store['interval'])
        else:
            trigger = DateTrigger(run_date=data_store['run_date'], timezone=pytz.timezone('Asia/Kolkata'))
        
        # Ensure scheduler is running before adding job
        if not scheduler.running:
            scheduler.start()
            
        scheduler.add_job(execute_post, trigger, args=[user_id, data_store])
        await event.edit(f"✅ Scheduled!")

# Start the bot
bot.run_until_disconnected()
