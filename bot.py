import asyncio
import logging
import re
import os  # <--- Make sure this is imported
import sqlite3
from datetime import datetime, timedelta

from telethon import TelegramClient, events, Button, functions
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
import pytz

# --- CONFIGURATION (Loaded from Railway Variables) ---
# We use int() because ID numbers must be integers, not strings
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
scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Kolkata'))
scheduler.start()

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
# Temporary storage for login flows and post creation
login_states = {}  # {user_id: {'state': '...', 'phone': '...', 'client': ...}}
post_states = {}   # {user_id: {'channel_id': ..., 'content': ..., 'schedule': ...}}

# Helper to get User Client
async def get_user_client(user_id):
    cursor.execute("SELECT session_string FROM sessions WHERE user_id=?", (user_id,))
    res = cursor.fetchone()
    if res:
        client = TelegramClient(StringSession(res[0]), API_ID, API_HASH)
        await client.connect()
        return client
    return None

# --- AUTHENTICATION FLOW ---

@bot.on(events.NewMessage(pattern='/manage'))
async def manage_handler(event):
    user_id = event.sender_id
    client = await get_user_client(user_id)
    
    if client and await client.is_user_authorized():
        await show_main_menu(event)
    else:
        await event.respond("⚠️ You are not logged in.\nPlease type /start to begin the login process.")

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    user_id = event.sender_id
    # Initialize login state
    login_states[user_id] = {'state': 'WAIT_PHONE'}
    await event.respond("Welcome! Please send your **Phone Number** (with country code) to login.\nExample: `+919876543210`")

@bot.on(events.NewMessage)
async def message_handler(event):
    # Ignore commands
    if event.text.startswith('/'): return
    
    user_id = event.sender_id
    state_data = login_states.get(user_id)
    post_data = post_states.get(user_id)

    # --- 1. LOGIN LOGIC ---
    if state_data:
        state = state_data['state']
        
        if state == 'WAIT_PHONE':
            phone = event.text.strip()
            # Create a temporary client for this user to login
            user_client = TelegramClient(StringSession(), API_ID, API_HASH)
            await user_client.connect()
            
            try:
                await user_client.send_code_request(phone)
                login_states[user_id]['phone'] = phone
                login_states[user_id]['client'] = user_client
                login_states[user_id]['state'] = 'WAIT_CODE'
                await event.respond("✅ OTP Sent!\n\nPlease send the code in this format: `aa12345`\n(Add 'aa' before the code to prevent expiration).")
            except Exception as e:
                await event.respond(f"Error: {e}")
                
        elif state == 'WAIT_CODE':
            # Parse format "aa12345"
            raw_text = event.text.strip()
            if not raw_text.startswith('aa'):
                await event.respond("⚠️ Invalid format. Please start with 'aa' (e.g., aa12345).")
                return
            
            code = raw_text[2:] # Strip 'aa'
            phone = state_data['phone']
            user_client = state_data['client']
            
            try:
                await user_client.sign_in(phone, code)
                # If successful, save session
                save_session(user_id, user_client)
                await event.respond("✅ Login Successful!", buttons=[Button.inline("Open Menu", b"menu_main")])
                del login_states[user_id]
            except SessionPasswordNeededError:
                login_states[user_id]['state'] = 'WAIT_PASSWORD'
                await event.respond("🔐 Two-Step Verification is enabled.\nPlease send your **Password**.")
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

    # --- 2. ADD CHANNEL LOGIC ---
    elif post_data and post_data.get('state') == 'WAIT_CHANNEL_FWD':
        if event.fwd_from:
             # Capture Channel ID
            chat_id = event.fwd_from.channel_id
            # Note: Telethon channel IDs often need -100 prefix for API usage if captured this way, 
            # but fwd_from.channel_id usually gives the positive integer. We convert to standard -100 ID.
            full_chat_id = int(f"-100{chat_id}")
            title = event.fwd_from.from_name or "Unknown Channel"
            
            cursor.execute("INSERT INTO channels (user_id, channel_id, channel_title) VALUES (?, ?, ?)", 
                           (user_id, full_chat_id, title))
            conn.commit()
            del post_states[user_id]
            await event.respond(f"✅ Channel **{title}** added successfully!", buttons=[Button.inline("Back to Menu", b"menu_main")])
        else:
            await event.respond("⚠️ That is not a forwarded message from a channel. Please try again.")

    # --- 3. NEW POST CONTENT LOGIC ---
    elif post_data and post_data.get('state') == 'WAIT_CONTENT':
        # Store the message object (text or media)
        post_states[user_id]['content_msg'] = event
        post_states[user_id]['state'] = 'WAIT_SCHEDULE_CONFIRM'
        await event.respond("Content received. Do you want to schedule this post?", 
                            buttons=[
                                [Button.inline("Yes, Schedule", b"sched_yes"), Button.inline("No, Post Now", b"sched_no")]
                            ])
    
    # --- 4. SCHEDULING INPUT ---
    elif post_data and post_data.get('state') == 'WAIT_TIME':
        # Expected format: YYYY-MM-DD HH:MM
        try:
            ist = pytz.timezone('Asia/Kolkata')
            dt = datetime.strptime(event.text, "%Y-%m-%d %H:%M")
            dt = ist.localize(dt) # Make it offset-aware
            
            post_states[user_id]['run_date'] = dt
            post_states[user_id]['state'] = 'WAIT_REPEAT'
            await event.respond("Do you want repetition? \nFormat: `x hour`, `x minutes`, `x days`, `x seconds`.\nType `no` to skip.")
        except ValueError:
            await event.respond("⚠️ Invalid format. Use `YYYY-MM-DD HH:MM` (e.g., 2024-12-30 14:30)")

    elif post_data and post_data.get('state') == 'WAIT_REPEAT':
        text = event.text.lower()
        if text != 'no':
            try:
                parts = text.split()
                amount = int(parts[0])
                unit = parts[1]
                
                # Normalize unit
                if 'hour' in unit: kwargs = {'hours': amount}
                elif 'minute' in unit: kwargs = {'minutes': amount}
                elif 'day' in unit: kwargs = {'days': amount}
                elif 'second' in unit: kwargs = {'seconds': amount}
                else: raise ValueError
                
                post_states[user_id]['interval'] = kwargs
            except:
                await event.respond("⚠️ Invalid format. Try `2 hours` or `30 minutes`. Type `no` to skip.")
                return

        # Final Options
        await event.respond("Final Settings:", buttons=[
            [Button.inline("📌 Pin: No", b"toggle_pin")],
            [Button.inline("🗑 Delete Old: No", b"toggle_del")],
            [Button.inline("✅ Confirm & Schedule", b"finish_sched")]
        ])

def save_session(user_id, client):
    s_str = StringSession.save(client.session)
    cursor.execute("REPLACE INTO sessions (user_id, session_string) VALUES (?, ?)", (user_id, s_str))
    conn.commit()

async def show_main_menu(event):
    await event.respond("🤖 **Channel Manager**", buttons=[
        [Button.inline("Add Channel", b"btn_add_ch"), Button.inline("Channels", b"btn_list_ch")],
        [Button.inline("Logout", b"btn_logout")]
    ])

# --- CALLBACK HANDLERS ---

@bot.on(events.CallbackQuery)
async def callback_handler(event):
    user_id = event.sender_id
    data = event.data.decode()
    
    if data == "menu_main":
        await show_main_menu(event)

    # --- LOGOUT FLOW ---
    elif data == "btn_logout":
        await event.edit("Are you sure you want to logout? (1/3)", buttons=[Button.inline("Yes", b"logout_1"), Button.inline("No", b"menu_main")])
    elif data == "logout_1":
        await event.edit("Really sure? Session will be terminated. (2/3)", buttons=[Button.inline("Yes", b"logout_2"), Button.inline("No", b"menu_main")])
    elif data == "logout_2":
        await event.edit("Final warning. Logout? (3/3)", buttons=[Button.inline("Yes, Logout", b"logout_final"), Button.inline("No", b"menu_main")])
    elif data == "logout_final":
        cursor.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        conn.commit()
        await event.edit("👋 You have been logged out.")

    # --- ADD CHANNEL ---
    elif data == "btn_add_ch":
        post_states[user_id] = {'state': 'WAIT_CHANNEL_FWD'}
        await event.respond("Please **Forward** a message from the channel you want to add.")

    # --- LIST CHANNELS ---
    elif data == "btn_list_ch":
        cursor.execute("SELECT id, channel_title FROM channels WHERE user_id=?", (user_id,))
        channels = cursor.fetchall()
        if not channels:
            await event.respond("No channels added.", buttons=[Button.inline("Back", b"menu_main")])
            return
        
        btns = [[Button.inline(c[1], f"ch_{c[0]}")] for c in channels]
        btns.append([Button.inline("Back", b"menu_main")])
        await event.respond("Select a channel:", buttons=btns)

    # --- CHANNEL MENU ---
    elif data.startswith("ch_"):
        ch_db_id = int(data.split("_")[1])
        # Save selected channel to state
        post_states[user_id] = {'channel_db_id': ch_db_id, 'pin': False, 'del_old': False}
        
        await event.edit("Channel Options:", buttons=[
            [Button.inline("New Post", b"feat_new_post"), Button.inline("Tasks", b"feat_tasks")],
            [Button.inline("Back", b"btn_list_ch")]
        ])

    # --- NEW POST ---
    elif data == "feat_new_post":
        post_states[user_id]['state'] = 'WAIT_CONTENT'
        await event.respond("Send the content (Text, Image, etc.) you want to post.")

    # --- SCHEDULING FLOW ---
    elif data == "sched_no":
        # Post Immediately
        await execute_post(user_id, post_states[user_id])
        await event.edit("✅ Posted successfully!")
    
    elif data == "sched_yes":
        post_states[user_id]['state'] = 'WAIT_TIME'
        await event.respond("Enter time in IST (`YYYY-MM-DD HH:MM`):")

    # --- TOGGLES ---
    elif data == "toggle_pin":
        curr = post_states[user_id].get('pin', False)
        post_states[user_id]['pin'] = not curr
        label = f"📌 Pin: {'Yes' if not curr else 'No'}"
        await event.edit(buttons=[
            [Button.inline(label, b"toggle_pin")],
            [Button.inline(f"🗑 Delete Old: {'Yes' if post_states[user_id].get('del_old') else 'No'}", b"toggle_del")],
            [Button.inline("✅ Confirm & Schedule", b"finish_sched")]
        ])
        
    elif data == "toggle_del":
        curr = post_states[user_id].get('del_old', False)
        post_states[user_id]['del_old'] = not curr
        label = f"🗑 Delete Old: {'Yes' if not curr else 'No'}"
        await event.edit(buttons=[
            [Button.inline(f"📌 Pin: {'Yes' if post_states[user_id].get('pin') else 'No'}", b"toggle_pin")],
            [Button.inline(label, b"toggle_del")],
            [Button.inline("✅ Confirm & Schedule", b"finish_sched")]
        ])

    elif data == "finish_sched":
        # Add to Scheduler
        data = post_states[user_id]
        trigger = None
        
        if 'interval' in data:
            trigger = IntervalTrigger(start_date=data['run_date'], timezone=pytz.timezone('Asia/Kolkata'), **data['interval'])
        else:
            trigger = DateTrigger(run_date=data['run_date'], timezone=pytz.timezone('Asia/Kolkata'))
            
        scheduler.add_job(execute_post, trigger, args=[user_id, data])
        await event.edit(f"✅ Scheduled for {data['run_date']}!")

# --- EXECUTION ENGINE ---
async def execute_post(user_id, post_data):
    # Retrieve User Client
    user_client = await get_user_client(user_id)
    if not user_client: return

    # Get Channel ID from DB
    cursor.execute("SELECT channel_id, last_msg_id FROM channels WHERE id=?", (post_data['channel_db_id'],))
    res = cursor.fetchone()
    if not res: return
    channel_id, last_msg_id = res

    try:
        # Delete Old
        if post_data.get('del_old') and last_msg_id:
            try:
                await user_client.delete_messages(channel_id, last_msg_id)
            except Exception as e:
                logging.error(f"Failed to delete: {e}")

        # Send New
        msg_content = post_data['content_msg']
        sent_msg = await user_client.send_message(channel_id, msg_content)
        
        # Pin
        if post_data.get('pin'):
            await user_client.pin_message(channel_id, sent_msg)
            
        # Update DB with new last_msg_id
        cursor.execute("UPDATE channels SET last_msg_id=? WHERE id=?", (sent_msg.id, post_data['channel_db_id']))
        conn.commit()
        
    except Exception as e:
        logging.error(f"Post failed: {e}")

# Keep bot running
bot.run_until_disconnected()
