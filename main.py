import logging
import sqlite3
import httpx
import phonenumbers
import websocket
import requests
import json
import ssl
import re
import html
import threading
import time
from datetime import datetime, timedelta
from phonenumbers import geocoder
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, 
    ContextTypes, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler, 
    ConversationHandler,
    filters
)

# ------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------
BOT_TOKEN = "8056264478:AAG5DGsG-1Tjp_e3pUClNHgWC8kxVJxyOOs"
ADMIN_ID = 8563280306
OTP_GROUP_LINK = "https://t.me/+fgafLxcumRBhN2Vl"
DB_NAME = "inventory.db"

# ------------------------------------------------------------------------
# WEBSOCKET CONFIGURATION
# ------------------------------------------------------------------------
WS_BOT_TOKEN = "8781880934:AAGWsBhjIXM3G6VuXKfWdQrnsP9BdEhJnWo"
WS_CHAT_ID = "-1003593058615"
WS_TOKEN = "eyJpdiI6InJxVWpJOHA1QVJHWkxqTVFqK0REOWc9PSIsInZhbHVlIjoiNzltdDFnblNsMDUrdjhGaHlueFpoWUoveUNQd0hKbzZ0NFQ3ZklUaXNkV2I5aTFid2tDbFFzZUh3eVpHcFVldUJGcEpmY0d1cG8rNjg1UGJKeXZzMDVVMVBMVnpMZkdhTjFKck1EYXA5WXJmYzNGREc1RkNjN0E2MVozYU5ZK2pta0EySjVpb0hxTjVINmFGQ0ZUNVdoOEVkd29NMHVEV0E1SVBoeS9NTG1LdjZZNTdtd3kyZUFFSnRnenAxMFZScUR0dUtnMFVOZWFsODJIWXdjMUlwNnkveURPa3Fqd3pYRG9HNi9nMjZlWDJxQWp2WXhLcGtid3VUR1hnSXFwaytuYjgzNGNPZTB4MGJmQWJ0U2VEVmhUOHlpNW8yUUlkcXdYOUpQRmFzYUdQVTRmTmFkc1NaT281Yk43cHgzLzJFODhDcGthVkdlTjJId29leGd1bVdwVEttUjUyQjBUR0o4dDJGSzhUYXNXOVNZOXJkcExZblRzYU9pNEovOG9MeE9GME9pM01DSnplaVVNc3dyRXFZTS96cU85YWxvTytKVEFkcE9nSmdDS0xzdnYzRGtsWHZNMlA3dHJqTVltMkMvVUNLMFp6OGY3TjhRaWw0VDlkNHVEaGJvMjEwTTNvdUd0TW1kaDNZaUJWdytnbzdGOEx6ekZRVWZ5Q1R2WUhHZE1DVnhMK3ozSHlKb0pOakp3bG9RPT0iLCJtYWMiOiI5MGMwNWQxZjcxYTgzNjk5NWFmNGJjODY3MTZkYzM5ZTg0Y2ZlYzI2ZWI2MjcyZTY4NzI2N2JiNjEyZDBmMmY5IiwidGFnIjoiIn0="
WS_USER = "b379f2c2f583e4d524b8dad4fdbf9d3a"
WS_URL = f"wss://ivasms.com:2087/socket.io/?token={WS_TOKEN}&user={WS_USER}&EIO=4&transport=websocket"

# ------------------------------------------------------------------------
# WEBSOCKET GLOBAL STORAGE
# ------------------------------------------------------------------------
# Store OTP messages by phone number
otp_messages = {}  # {phone_number: {'message': '...', 'timestamp': datetime, 'service': '...', 'otp': '...'}}
otp_lock = threading.Lock()

# ------------------------------------------------------------------------
# COUNTRY FLAGS FOR WEBSOCKET
# ------------------------------------------------------------------------
country_flags = {
    'ci': '🇨🇮', 'bd': '🇧🇩', 'in': '🇮🇳', 'us': '🇺🇸', 'gb': '🇬🇧',
    'ca': '🇨🇦', 'au': '🇦🇺', 'de': '🇩🇪', 'fr': '🇫🇷', 'jp': '🇯🇵',
    'cn': '🇨🇳', 'ru': '🇷🇺', 'br': '🇧🇷', 'za': '🇿🇦', 'ng': '🇳🇬',
    'ke': '🇰🇪', 'eg': '🇪🇬', 'sa': '🇸🇦', 'ae': '🇦🇪', 'pk': '🇵🇰',
    'lk': '🇱🇰', 'np': '🇳🇵', 'my': '🇲🇾', 'sg': '🇸🇬', 'ph': '🇵🇭',
    'th': '🇹🇭', 'vn': '🇻🇳', 'id': '🇮🇩',
}

# ------------------------------------------------------------------------
# STATES FOR CONVERSATIONS
# ------------------------------------------------------------------------
ASK_TAG = 0
DELETE_SELECT, DELETE_AMOUNT = range(1, 3)
BROADCAST_MSG = 3

# ------------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------
# DATABASE FUNCTIONS (SQLite)
# ------------------------------------------------------------------------

def init_db():
    """Initialize the SQL database tables."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS numbers
                 (phone TEXT PRIMARY KEY, country TEXT, tags TEXT, status TEXT, user_id INTEGER, last_msg TEXT, message_id INTEGER)''')
    
    try:
        c.execute("SELECT last_msg FROM numbers LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE numbers ADD COLUMN last_msg TEXT")
        
    try:
        c.execute("SELECT message_id FROM numbers LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE numbers ADD COLUMN message_id INTEGER")

    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY)''')
                 
    conn.commit()
    conn.close()

def add_user_to_db(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
    except sqlite3.IntegrityError:
        pass 
    conn.close()

def get_all_users():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id FROM users")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def add_numbers_to_db(number_list, tag):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    count = 0
    countries_found = set()
    
    for phone_raw in number_list:
        phone = phone_raw if phone_raw.startswith('+') else f"+{phone_raw}"
        country_label = detect_country_label(phone)
        countries_found.add(country_label)
        
        try:
            c.execute("INSERT INTO numbers (phone, country, tags, status, user_id, last_msg, message_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (phone, country_label, tag, 'available', None, None, None))
            count += 1
        except sqlite3.IntegrityError:
            pass 

    conn.commit()
    conn.close()
    return count, countries_found

def get_inventory_stats():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT country, tags, COUNT(*) FROM numbers WHERE status='available' GROUP BY country, tags")
    rows = c.fetchall()
    conn.close()
    return {(r[0], r[1]): r[2] for r in rows}

def get_stock_counts(country, tag):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM numbers WHERE country=? AND tags=? AND status='available'", (country, tag))
    left = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM numbers WHERE country=? AND tags=? AND (status='assigned' OR status='used')", (country, tag))
    used = c.fetchone()[0]
    conn.close()
    return left, used

def assign_number(user_id, country, tag):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    c.execute("SELECT phone FROM numbers WHERE country=? AND tags=? AND status='available' ORDER BY RANDOM() LIMIT 1", (country, tag))
    result = c.fetchone()
    
    if result:
        phone = result[0]
        c.execute("UPDATE numbers SET status='assigned', user_id=?, last_msg=NULL WHERE phone=?", (user_id, phone))
        conn.commit()
        conn.close()
        return {'number': phone, 'country': country, 'tags': tag, 'last_msg': None}
    
    conn.close()
    return None

def get_user_active_number(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT phone, country, tags, last_msg, message_id FROM numbers WHERE user_id=? AND status='assigned'", (user_id,))
    result = c.fetchone()
    conn.close()
    
    if result:
        return {'number': result[0], 'country': result[1], 'tags': result[2], 'last_msg': result[3], 'message_id': result[4]}
    return None

def update_number_msg(phone, msg):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE numbers SET last_msg=? WHERE phone=?", (msg, phone))
    conn.commit()
    conn.close()

def update_number_message_id(phone, message_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE numbers SET message_id=? WHERE phone=?", (message_id, phone))
    conn.commit()
    conn.close()

def replace_user_number(user_id):
    current = get_user_active_number(user_id)
    if not current:
        return None

    country = current['country']
    tag = current['tags']
    old_phone = current['number']

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM numbers WHERE country=? AND tags=? AND status='available'", (country, tag))
    stock_count = c.fetchone()[0]

    if stock_count == 0:
        conn.close()
        return "NO_STOCK"

    c.execute("UPDATE numbers SET status='used', user_id=NULL WHERE phone=?", (old_phone,))
    
    c.execute("SELECT phone FROM numbers WHERE country=? AND tags=? AND status='available' ORDER BY RANDOM() LIMIT 1", (country, tag))
    new_result = c.fetchone()
    
    if new_result:
        new_phone = new_result[0]
        c.execute("UPDATE numbers SET status='assigned', user_id=?, last_msg=NULL WHERE phone=?", (user_id, new_phone))
        conn.commit()
        conn.close()
        return {'number': new_phone, 'country': country, 'tags': tag, 'last_msg': None}
    
    conn.rollback()
    conn.close()
    return "ERROR"

def delete_stock_db(country, tag, amount_str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    if amount_str == 'ALL':
        c.execute("DELETE FROM numbers WHERE country=? AND tags=? AND status='available'", (country, tag))
        deleted = c.rowcount
    else:
        limit = int(amount_str)
        c.execute("SELECT phone FROM numbers WHERE country=? AND tags=? AND status='available' LIMIT ?", (country, tag, limit))
        rows = c.fetchall()
        deleted = len(rows)
        if deleted > 0:
            phones = [r[0] for r in rows]
            placeholders = ','.join('?' * len(phones))
            c.execute(f"DELETE FROM numbers WHERE phone IN ({placeholders})", phones)
            
    conn.commit()
    conn.close()
    return deleted

# ------------------------------------------------------------------------
# WEBSOCKET HELPER FUNCTIONS
# ------------------------------------------------------------------------

def mask_number(number):
    """Mask phone number for privacy"""
    if len(number) > 8:
        return number[:4] + '*' * (len(number) - 8) + number[-4:]
    return number

def extract_otp(message_text):
    """Extract OTP code from message"""
    patterns = [
        r'code:?\s*(\d{4,8})',
        r'code[:\s]*(\d{4,8})',
        r'OTP:?\s*(\d{4,8})',
        r'is:?\s*(\d{4,8})',
        r'(\d{4,8})\s*is',
        r'(\d{4,8})$',
        r'<#>\s*(\d{4,8})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, message_text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    digits = re.findall(r'\b(\d{4,8})\b', message_text)
    if digits:
        return digits[0]
    
    return 'No OTP Found'

def detect_service(originator, message):
    """Detect the service name"""
    message_lower = message.lower()
    
    services = {
        'Claude AI': ['claude', 'anthropic'],
        'Facebook': ['facebook', 'fb'],
        'Google': ['google', 'g-', 'gmail', 'youtube'],
        'WhatsApp': ['whatsapp', 'wa'],
        'Instagram': ['instagram', 'ig'],
        'Twitter': ['twitter', 'x.com'],
        'Telegram': ['telegram'],
        'Amazon': ['amazon', 'aws'],
        'Microsoft': ['microsoft', 'ms', 'outlook', 'hotmail', 'azure'],
        'Apple': ['apple', 'icloud', 'imessage'],
        'LinkedIn': ['linkedin'],
        'Snapchat': ['snapchat'],
        'TikTok': ['tiktok'],
        'Discord': ['discord'],
        'Spotify': ['spotify'],
        'Netflix': ['netflix'],
        'PayPal': ['paypal'],
        'Bank': ['bank', 'sbi', 'hdfc', 'icici', 'axis', 'kotak'],
    }
    
    for service, keywords in services.items():
        for keyword in keywords:
            if keyword in message_lower:
                return service
    
    return originator if originator and originator != 'Unknown' else 'Premium SMS'

def extract_sms_data(raw_message):
    """Extract SMS data from raw message"""
    try:
        start_idx = raw_message.find('{')
        end_idx = raw_message.rfind('}') + 1
        
        if start_idx != -1 and end_idx > start_idx:
            json_str = raw_message[start_idx:end_idx]
            data = json.loads(json_str)
            return data
    except:
        pass
    
    data = {}
    
    originator_match = re.search(r'originator["\s:]+([^",}]+)', raw_message)
    if originator_match:
        data['originator'] = originator_match.group(1).strip('" ')
    
    recipient_match = re.search(r'recipient["\s:]+([^",}]+)', raw_message)
    if recipient_match:
        data['recipient'] = recipient_match.group(1).strip('" ')
    
    message_match = re.search(r'message["\s:]+([^",}]+)', raw_message)
    if message_match:
        data['message'] = message_match.group(1).strip('" ')
    
    country_match = re.search(r'country_iso["\s:]+([^",}]+)', raw_message)
    if country_match:
        data['country_iso'] = country_match.group(1).strip('" ')
    
    return data if data else None

def process_websocket_message(recipient_number, message_text, originator, country):
    """Process incoming SMS and store in memory for bot to retrieve"""
    with otp_lock:
        # Clean phone number (ensure it has + prefix)
        if recipient_number and not recipient_number.startswith('+'):
            recipient_number = f"+{recipient_number}"
        
        otp_code = extract_otp(message_text)
        service = detect_service(originator, message_text)
        
        otp_messages[recipient_number] = {
            'message': message_text,
            'timestamp': datetime.now(),
            'service': service,
            'otp': otp_code,
            'originator': originator,
            'country': country
        }
        
        # Keep only last 100 messages to avoid memory issues
        if len(otp_messages) > 100:
            oldest_key = min(otp_messages.keys(), key=lambda k: otp_messages[k]['timestamp'])
            del otp_messages[oldest_key]
        
        logger.info(f"📱 Stored OTP for {recipient_number}: {otp_code} from {service}")

def get_otp_for_number(phone_number):
    """Retrieve OTP for a specific phone number from cache"""
    with otp_lock:
        if phone_number in otp_messages:
            return otp_messages[phone_number]['message']
    return None

def clear_otp_for_number(phone_number):
    """Clear OTP cache for a number after it's been sent"""
    with otp_lock:
        if phone_number in otp_messages:
            del otp_messages[phone_number]
            return True
    return False

# ------------------------------------------------------------------------
# WEBSOCKET CONNECTION HANDLER
# ------------------------------------------------------------------------

def on_websocket_message(ws, message):
    """Handle incoming WebSocket messages"""
    if message == '2':
        ws.send('3')
        return
    
    if message.startswith('42/livesms'):
        try:
            sms_data = extract_sms_data(message)
            
            if sms_data and isinstance(sms_data, dict):
                originator = sms_data.get('originator', 'Unknown')
                recipient = sms_data.get('recipient', '')
                message_text = sms_data.get('message', '')
                country = sms_data.get('country_iso', 'Unknown')
                
                if recipient and message_text:
                    process_websocket_message(recipient, message_text, originator, country)
                    
        except Exception as e:
            logger.error(f"WebSocket message processing error: {e}")

def on_websocket_error(ws, error):
    logger.error(f"WebSocket error: {error}")

def on_websocket_close(ws, close_status_code, close_msg):
    logger.warning(f"WebSocket disconnected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("🔄 Reconnecting in 5 seconds...")

def on_websocket_open(ws):
    logger.info("="*60)
    logger.info("🚀 WebSocket SMS Listener Connected")
    logger.info("="*60)
    logger.info(f"📱 User: {WS_USER[:8]}...")
    logger.info("="*60)
    
    ws.send("40")
    time.sleep(0.1)
    ws.send("40/livesms,")

def run_websocket():
    """Run WebSocket connection in separate thread"""
    while True:
        try:
            websocket.enableTrace(False)
            
            headers = {
                'Origin': 'https://www.ivasms.com',
                'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36',
                'Connection': 'Upgrade',
                'Upgrade': 'websocket'
            }
            
            ws = websocket.WebSocketApp(
                WS_URL,
                header=headers,
                on_open=on_websocket_open,
                on_message=on_websocket_message,
                on_error=on_websocket_error,
                on_close=on_websocket_close
            )
            
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            
        except Exception as e:
            logger.error(f"WebSocket fatal error: {e}")
            logger.info("🔄 Restarting in 5 seconds...")
        
        time.sleep(5)

# ------------------------------------------------------------------------
# BACKGROUND JOB - CHECK OTPs FROM WEBSOCKET CACHE
# ------------------------------------------------------------------------

async def check_active_otps_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Background job that checks WebSocket cache for OTPs.
    If a new message arrives, it updates the user's message instantly.
    """
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT phone, user_id, message_id, country, tags, last_msg FROM numbers WHERE status='assigned'")
    active_assignments = c.fetchall()
    conn.close()

    for row in active_assignments:
        phone, user_id, message_id, country, tags, last_msg = row
        
        if not message_id:
            continue
            
        # Check WebSocket cache for OTP
        new_msg_text = get_otp_for_number(phone)
        
        if new_msg_text and new_msg_text != last_msg:
            # Update DB
            update_number_msg(phone, new_msg_text)
            
            # Clear the OTP from cache after sending
            clear_otp_for_number(phone)
            
            # Reconstruct the message
            parts = country.split()
            flag = ""
            name = country
            if len(parts) >= 2 and parts[0].startswith('+'):
                flag = parts[-1]
                name = " ".join(parts[1:-1])

            new_text = (
                f"{flag} {name} {tags} Number Assigned:\n"
                f"`{phone}`\n\n"
                f"📩 **Message:**\n`{new_msg_text}`"
            )
            
            keyboard = [
                [InlineKeyboardButton("🔄 Change Number", callback_data='change_number')],
                [InlineKeyboardButton("🌍 Change Country", callback_data='main_menu')],
                [InlineKeyboardButton("👥 OTP Group", url=OTP_GROUP_LINK)]
            ]
            
            try:
                await context.bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=new_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
                logger.info(f"✅ Updated OTP for {phone} to user {user_id}")
            except Exception as e:
                logger.warning(f"Could not update message for {user_id}: {e}")

# ------------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------------

def detect_country_label(phone_number):
    try:
        parsed_number = phonenumbers.parse(phone_number, None)
        region_code = phonenumbers.region_code_for_number(parsed_number)
        if region_code:
            country_name = geocoder.description_for_number(parsed_number, "en")
            country_code = parsed_number.country_code
            flag = "".join([chr(ord(c) + 127397) for c in region_code.upper()])
            return f"+{country_code} {country_name} {flag}"
    except:
        pass
    return "Unknown 🌍"

async def broadcast_to_all(context, text):
    users = get_all_users()
    count = 0
    for uid in users:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode='Markdown')
            count += 1
        except Exception:
            pass
    return count

# ------------------------------------------------------------------------
# ADMIN CONVERSATION: DELETE STOCK
# ------------------------------------------------------------------------

async def delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END

    stats = get_inventory_stats()
    keyboard = []

    if not stats:
        await update.message.reply_text("🗑️ **Stock is empty.**", parse_mode='Markdown')
        return ConversationHandler.END

    text = "🗑️ **Delete Mode**\nSelect a category:\n"
    for (country, tag), count in sorted(stats.items()):
        btn_text = f"🗑️ {country} {tag} ({count})"
        callback_data = f"{country}||{tag}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data='cancel')])
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    return DELETE_SELECT

async def delete_select_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == 'cancel':
        await query.edit_message_text("❌ Cancelled.")
        return ConversationHandler.END
        
    context.user_data['delete_target'] = query.data
    country, tag = query.data.split('||')
    await query.edit_message_text(f"Selected: **{country}** - **{tag}**\n\nType amount to delete (e.g. `5`) or `ALL`.", parse_mode='Markdown')
    return DELETE_AMOUNT

async def delete_perform(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().upper()
    target_data = context.user_data.get('delete_target')
    if not target_data:
        return ConversationHandler.END

    target_country, target_tag = target_data.split('||')
    
    try:
        deleted_count = delete_stock_db(target_country, target_tag, text)
        await update.message.reply_text(f"✅ **Deleted {deleted_count} numbers.**")
    except ValueError:
        await update.message.reply_text("⚠️ Invalid number.")
        return DELETE_AMOUNT

    context.user_data.clear()
    return ConversationHandler.END

# ------------------------------------------------------------------------
# ADMIN CONVERSATION: BROADCAST
# ------------------------------------------------------------------------

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    
    await update.message.reply_text(
        "📢 **Broadcast Mode**\n\n"
        "Please type the message you want to send to **ALL** users.\n"
        "Type /cancel to abort."
    , parse_mode='Markdown')
    return BROADCAST_MSG

async def broadcast_perform(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message.text
    status_msg = await update.message.reply_text("⏳ Sending broadcast...")
    count = await broadcast_to_all(context, message)
    await status_msg.edit_text(f"✅ **Broadcast Sent!**\n\n📨 Delivered to: {count} users.")
    return ConversationHandler.END

# ------------------------------------------------------------------------
# ADMIN CONVERSATION: UPLOAD
# ------------------------------------------------------------------------

async def admin_upload_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    
    document = update.message.document
    if not document or not document.file_name.endswith('.txt'):
        await update.message.reply_text("⚠️ Upload a .txt file.")
        return ConversationHandler.END

    file = await context.bot.get_file(document.file_id)
    byte_array = await file.download_as_bytearray()
    content = byte_array.decode('utf-8')
    lines = [l.strip() for l in content.split('\n') if l.strip()]

    if not lines:
        await update.message.reply_text("⚠️ Empty file.")
        return ConversationHandler.END

    context.user_data['upload_lines'] = lines
    await update.message.reply_text(f"📂 **Received {len(lines)} numbers.**\n\n1️⃣ **Enter Tag:** (e.g., `WS+FB`)", parse_mode='Markdown')
    return ASK_TAG

async def admin_receive_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tag = update.message.text.strip()
    lines = context.user_data.get('upload_lines')
    
    added, countries = add_numbers_to_db(lines, tag)
    countries_str = "\n".join(countries)
    
    await update.message.reply_text(
        f"✅ **Done!**\n📥 Added: {added}\n🏷️ Tag: {tag}\n🌍 **Regions:**\n{countries_str}", 
        parse_mode='Markdown'
    )
    
    notification_text = (
        f"New Number Added!!\n"
        f"Country: {countries_str}\n"
        f"Capacity: {added}\n\n"
        f"All Numbers are New and Fresh."
    )
    await broadcast_to_all(context, notification_text)
    
    context.user_data.clear()
    return ConversationHandler.END

async def cancel_op(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END

# ------------------------------------------------------------------------
# USER HANDLERS
# ------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    add_user_to_db(user_id)
    await show_inventory_menu(update, context)

async def show_inventory_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False):
    stats = get_inventory_stats()
    keyboard = []

    if not stats:
        text = "🚫 No numbers available right now."
    else:
        text = "🌍 **Select your country:**"
        for (country, tag), count in sorted(stats.items()):
            parts = country.split()
            if len(parts) >= 2 and parts[0].startswith('+'):
                code = parts[0]
                flag = parts[-1]
                name = " ".join(parts[1:-1])
                btn_text = f"{flag} {name} {tag} {code} ({count})"
            else:
                btn_text = f"{country} {tag} ({count})"

            callback_data = f"buy||{country}||{tag}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])

    reply_markup = InlineKeyboardMarkup(keyboard)

    if is_callback:
        try:
            await update.callback_query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode='Markdown')
        except:
            pass
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=reply_markup, parse_mode='Markdown')

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data.startswith('buy||'):
        _, country_req, tag_req = data.split('||')

        active = get_user_active_number(user_id)
        if active:
            left, _ = get_stock_counts(country_req, tag_req)
            if left == 0:
                await query.answer("❌ That package is sold out!", show_alert=True)
                return

            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("UPDATE numbers SET status='used', user_id=NULL WHERE phone=?", (active['number'],))
            conn.commit()
            conn.close()

        entry = assign_number(user_id, country_req, tag_req)
        
        if entry:
            await show_number_panel(update, context, entry, is_edit=True)
        else:
            await query.answer("❌ Sold out just now!", show_alert=True)
            await show_inventory_menu(update, context, is_callback=True)

    elif data == 'change_number':
        result = replace_user_number(user_id)
        if result == "NO_STOCK":
            await query.answer("🚫 No replacement numbers available!", show_alert=True)
        elif result == "ERROR" or result is None:
            await query.answer("⚠️ Error finding number.", show_alert=True)
        else:
            await show_number_panel(update, context, result, is_edit=True)

    elif data == 'main_menu':
        await show_inventory_menu(update, context, is_callback=True)

async def show_number_panel(update, context, entry, is_edit=False, extra_info=None):
    parts = entry['country'].split()
    flag = ""
    name = entry['country']
    
    if len(parts) >= 2 and parts[0].startswith('+'):
        flag = parts[-1]
        name = " ".join(parts[1:-1])

    if entry.get('last_msg'):
        otp_display = f"📩 **Message:**\n`{entry['last_msg']}`"
    else:
        otp_display = extra_info if extra_info else "Waiting for OTP..."

    text = (
        f"{flag} {name} {entry['tags']} Number Assigned:\n"
        f"`{entry['number']}`\n\n"
        f"{otp_display}"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔄 Change Number", callback_data='change_number')],
        [InlineKeyboardButton("🌍 Change Country", callback_data='main_menu')],
        [InlineKeyboardButton("👥 OTP Group", url=OTP_GROUP_LINK)]
    ]
    
    msg = None
    if is_edit:
        try:
            msg = await update.callback_query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except:
            pass
    else:
        msg = await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    if msg:
        update_number_message_id(entry['number'], msg.message_id)
    elif is_edit and update.callback_query and update.callback_query.message:
        update_number_message_id(entry['number'], update.callback_query.message.message_id)

# ------------------------------------------------------------------------
# MAIN APPLICATION
# ------------------------------------------------------------------------

if __name__ == '__main__':
    init_db()
    
    # Start WebSocket listener in background thread
    websocket_thread = threading.Thread(target=run_websocket, daemon=True)
    websocket_thread.start()
    logger.info("✅ WebSocket listener started in background")
    
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Add Background Job for Auto-Checking OTPs
    job_queue = application.job_queue
    job_queue.run_repeating(check_active_otps_job, interval=5, first=2)

    upload_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Document.FileExtension("txt"), admin_upload_start)],
        states={ ASK_TAG: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_receive_tag)] },
        fallbacks=[CommandHandler('cancel', cancel_op)],
        per_message=False
    )

    delete_conv = ConversationHandler(
        entry_points=[CommandHandler('delete', delete_start)],
        states={
            DELETE_SELECT: [CallbackQueryHandler(delete_select_category)],
            DELETE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_perform)],
        },
        fallbacks=[CommandHandler('cancel', cancel_op)],
        per_message=False
    )

    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler('broadcast', broadcast_start)],
        states={
            BROADCAST_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_perform)],
        },
        fallbacks=[CommandHandler('cancel', cancel_op)],
        per_message=False
    )

    application.add_handler(upload_conv)
    application.add_handler(delete_conv)
    application.add_handler(broadcast_conv)
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CallbackQueryHandler(button_handler))

    print("Bot is running with WebSocket OTP Listener...")
    application.run_polling()
