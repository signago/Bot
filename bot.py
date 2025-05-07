import logging
import re
import sys
import asyncio
import sqlite3
import json
import socket
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ConversationHandler
from telegram.request import HTTPXRequest
import aiohttp
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import pkg_resources
from telegram.error import TimedOut, BadRequest, NetworkError
import httpx

# Verify Python version
if sys.version_info.major != 3 or sys.version_info.minor != 11:
    raise SystemExit(f"ERROR: This bot requires Python 3.11, but {sys.version} is installed. Please use Python 3.11.9.")

# Configure logging with UTF-8 encoding
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.FileHandler('bot.log', encoding='utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logging.getLogger('telegram.ext.ConversationHandler').setLevel(logging.ERROR)
logging.getLogger('httpcore').setLevel(logging.INFO)
logging.getLogger('httpx').setLevel(logging.INFO)

# Log execution context
logger.info(f"Running script in {'interactive' if sys.flags.interactive else 'non-interactive'} mode")

# Check dependency versions
EXPECTED_VERSIONS = {
    'python-telegram-bot': '20.8',
    'aiohttp': '3.10.5'
}
for pkg, expected_version in EXPECTED_VERSIONS.items():
    try:
        installed_version = pkg_resources.get_distribution(pkg).version
        if installed_version != expected_version:
            logger.warning(f"Dependency {pkg} version {installed_version} detected; expected {expected_version}.")
    except pkg_resources.DistributionNotFound:
        logger.error(f"Dependency {pkg} not found. Please install {pkg}=={expected_version}.")
        raise SystemExit(f"Missing dependency: {pkg}")

# Set WindowsSelectorEventLoopPolicy for Windows compatibility
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Conversation states
(ENTER_ADDRESS, SELECT_CHAIN, SELECT_MONITOR_TYPE, ENTER_VALUE, BROADCAST_TYPE, BROADCAST_MESSAGE, CONFIRM_UNMONITOR, CONFIRM_TOKEN, ENTER_SYMBOL, CLEAR_WATCHLIST, POST_AD_MESSAGE, POST_AD_DURATION, POST_AD_VIEWS, DELETE_AD, CONFIRM_DELETE_AD) = range(15)

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('data.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            watchlist TEXT NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT NOT NULL,
            duration_days INTEGER NOT NULL,
            max_views INTEGER NOT NULL,
            current_views INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            active INTEGER NOT NULL
        )
    ''')
    conn.commit()
    return conn

db = init_db()

ADMIN_IDS = [6945482583]  # Your Telegram user ID

CHAINS = ['solana', 'ethereum', 'base', 'bsc', 'polygon', 'ton']

CHAIN_IDS = {
    'ethereum': 1,
    'base': 8453,
    'bsc': 56,
    'solana': 'solana',
    'polygon': 137,
    'ton': 'ton-mainnet'
}

COINGECKO_PLATFORMS = {
    'ethereum': 'ethereum',
    'bsc': 'binance-smart-chain',
    'polygon': 'polygon-pos',
    'base': 'base',
    'ton': 'the-open-network'
}

ALCHEMY_CHAINS = {
    'ethereum': 'eth-mainnet',
    'polygon': 'polygon-mainnet',
    'base': 'base-mainnet'
}

DEXSCREENER_CHAINS = {
    'solana': 'solana',
    'ethereum': 'ethereum',
    'base': 'base',
    'bsc': 'bsc',
    'polygon': 'polygon',
    'ton': 'ton'
}

DEXSCREENER_SEARCH_API = "https://api.dexscreener.com/latest/dex/search"
DEXSCREENER_TOKEN_API = "https://api.dexscreener.com/latest/dex/tokens/"
GMGN_SOLANA_API = "https://gmgn.ai/defi/router/v1/sol/tx/get_swap_route"
GMGN_ETH_BSC_BASE_API = "https://gmgn.ai/defi/router/v1/tx/available_routes_exact_in"
GMGN_SLIPPAGE_API = "https://api.gmgn.ai/api/v1/recommend_slippage/"

price_cache = {}
symbol_cache = {}
market_cap_cache = {}
price_history_cache = {}
last_value_cache = {}
failed_attempts = {}
top_monitored_cache = {'result': None, 'timestamp': 0}
CACHE_DURATION = 300

def get_user(user_id):
    cursor = db.cursor()
    cursor.execute('SELECT watchlist FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    if not result:
        watchlist = []
        cursor.execute('INSERT INTO users (user_id, watchlist) VALUES (?, ?)', (user_id, json.dumps(watchlist)))
        db.commit()
        return {'user_id': user_id, 'watchlist': watchlist}
    watchlist = json.loads(result[0])
    for token in watchlist:
        if 'address' in token and 'full_address' not in token:
            token['full_address'] = token.pop('address')
        if 'market_cap' not in token:
            token['market_cap'] = 0.0
        if 'symbol' not in token:
            token['symbol'] = f"Unknown_{token.get('full_address', 'token')[-6:]}"
        if 'chain' not in token:
            token['chain'] = 'unknown'
        if 'type' not in token:
            token['type'] = 'price'
        if 'value' not in token:
            token['value'] = 0.0
    save_watchlist(user_id, watchlist)
    return {'user_id': user_id, 'watchlist': watchlist}

def save_watchlist(user_id, watchlist):
    cursor = db.cursor()
    cursor.execute('UPDATE users SET watchlist = ? WHERE user_id = ?', (json.dumps(watchlist), user_id))
    db.commit()
    global top_monitored_cache
    top_monitored_cache = {'result': None, 'timestamp': 0}
    logger.debug(f"Invalidated top_monitored_cache for user {user_id}")

def get_active_ad():
    cursor = db.cursor()
    now = datetime.utcnow()
    cursor.execute('SELECT id, message, duration_days, max_views, current_views, created_at, active FROM ads WHERE active = 1')
    ads = [
        {
            'id': row[0],
            'message': row[1],
            'duration_days': row[2],
            'max_views': row[3],
            'current_views': row[4],
            'created_at': row[5],
            'active': bool(row[6])
        }
        for row in cursor.fetchall()
    ]
    if not ads:
        logger.debug("No active ads found")
        return None
    valid_ads = []
    for ad in sorted(ads, key=lambda x: x['created_at']):
        created_at = datetime.fromisoformat(ad['created_at'])
        expires_at = created_at + timedelta(days=ad['duration_days'])
        if now > expires_at or ad['current_views'] >= ad['max_views']:
            cursor.execute('UPDATE ads SET active = 0 WHERE id = ?', (ad['id'],))
            db.commit()
            logger.info(f"Ad {ad['id']} deactivated: expired or reached {ad['max_views']} views")
        else:
            valid_ads.append(ad)
    selected_ad = valid_ads[0] if valid_ads else None
    logger.debug(f"Selected active ad: {selected_ad}")
    return selected_ad

def sanitize_markdown(text):
    if not text:
        return "Unknown"
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    logger.error(f"Update {update} caused error {error}", exc_info=error)
    error_details = f"Error: {error}"
    if isinstance(error, httpx.ConnectTimeout):
        error_details += f"\nURL: {getattr(error.request, 'url', 'unknown')}"
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text("An error occurred. Please use /start to try again.")
            context.user_data.clear()
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"Bot error: {error_details}\nUpdate: {update}"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    logger.debug(f"Received /start from user {user_id}")
    get_user(user_id)
    context.user_data.clear()
    keyboard = [
        [InlineKeyboardButton("Monitor", callback_data='monitor')],
        [InlineKeyboardButton("Unmonitor", callback_data='unmonitor')],
        [InlineKeyboardButton("Watchlist", callback_data='watchlist')],
        [InlineKeyboardButton("Top Monitored", callback_data='top_monitored')],
        [InlineKeyboardButton("Leaderboard", callback_data='leaderboard')],
    ]
    if user_id in ADMIN_IDS:
        keyboard.append([InlineKeyboardButton("📢 Broadcast Message", callback_data='broadcast')])
        keyboard.append([InlineKeyboardButton("🗑️ Clear User Watchlist", callback_data='clear_watchlist')])
        keyboard.append([InlineKeyboardButton("📣 Post Ad", callback_data='post_ad')])
        keyboard.append([InlineKeyboardButton("📜 List Ads", callback_data='list_ads')])
        keyboard.append([InlineKeyboardButton("🗑️ Delete Ad", callback_data='delete_ad')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Welcome to the Signago - your token monitoring bot! 📈\n"
        "Follow us on X for updates: https://x.com/signagobot\n"
        "Join our Telegram group: https://t.me/signagobot\n"
        "Choose an option:",
        reply_markup=reply_markup
    )
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.debug(f"Received command: {update.message.text}")
    await update.message.reply_text("Debug: Command received. Check bot.log for details.")
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.debug(f"User {user_id} clicked Back button")
    context.user_data.clear()
    keyboard = [
        [InlineKeyboardButton("Monitor", callback_data='monitor')],
        [InlineKeyboardButton("Unmonitor", callback_data='unmonitor')],
        [InlineKeyboardButton("Watchlist", callback_data='watchlist')],
        [InlineKeyboardButton("Top Monitored", callback_data='top_monitored')],
        [InlineKeyboardButton("Leaderboard", callback_data='leaderboard')],
    ]
    if user_id in ADMIN_IDS:
        keyboard.append([InlineKeyboardButton("📢 Broadcast Message", callback_data='broadcast')])
        keyboard.append([InlineKeyboardButton("🗑️ Clear User Watchlist", callback_data='clear_watchlist')])
        keyboard.append([InlineKeyboardButton("📣 Post Ad", callback_data='post_ad')])
        keyboard.append([InlineKeyboardButton("📜 List Ads", callback_data='list_ads')])
        keyboard.append([InlineKeyboardButton("🗑️ Delete Ad", callback_data='delete_ad')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await query.edit_message_text("Welcome! Choose an option:", reply_markup=reply_markup)
    except BadRequest as e:
        logger.error(f"Failed to edit message for back_to_menu: {e}")
        await query.message.reply_text("Welcome! Choose an option:", reply_markup=reply_markup)
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def list_ads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        if query:
            await query.answer()
            await query.edit_message_text("Unauthorized access.")
        else:
            await update.message.reply_text("Unauthorized access.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END if query else None

    cursor = db.cursor()
    cursor.execute('SELECT id, message, duration_days, max_views, current_views, created_at, active FROM ads')
    ads = [
        {
            'id': row[0],
            'message': row[1],
            'duration_days': row[2],
            'max_views': row[3],
            'current_views': row[4],
            'created_at': row[5],
            'active': bool(row[6])
        }
        for row in cursor.fetchall()
    ]
    if not ads:
        msg = "No ads found."
        keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if query:
            await query.answer()
            try:
                await query.edit_message_text(msg, reply_markup=reply_markup)
            except BadRequest as e:
                logger.error(f"Failed to edit message for list_ads: {e}")
                await query.message.reply_text(msg, reply_markup=reply_markup)
        else:
            await update.message.reply_text(msg, reply_markup=reply_markup)
        await asyncio.sleep(0.1)
        logger.info(f"Admin {user_id} viewed queued ads: none found")
        return ConversationHandler.END if query else None

    page = context.user_data.get('ads_page', 0)
    ads_per_page = 5
    total_pages = (len(ads) + ads_per_page - 1) // ads_per_page
    start_idx = page * ads_per_page
    end_idx = min(start_idx + ads_per_page, len(ads))
    sorted_ads = sorted(ads, key=lambda x: x['created_at'])

    msg = f"📣 Queued Ads (Page {page + 1}/{total_pages}):\n\n"
    now = datetime.utcnow()
    for ad in sorted_ads[start_idx:end_idx]:
        ad_id = ad.get('id', 'Unknown')
        created_at = datetime.fromisoformat(ad['created_at'])
        expires_at = created_at + timedelta(days=ad['duration_days'])
        status = "Active" if ad['active'] and ad['current_views'] < ad['max_views'] and now < expires_at else "Inactive"
        msg += (
            f"ID: {ad_id}\n"
            f"Message: {ad['message']}\n"
            f"Duration: {ad['duration_days']} days\n"
            f"Max Views: {ad['max_views']}\n"
            f"Current Views: {ad['current_views']}\n"
            f"Status: {status}\n"
            f"Created: {ad['created_at']}\n\n"
        )

    keyboard = []
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f'list_ads:{page - 1}'))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next", callback_data=f'list_ads:{page + 1}'))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("Back", callback_data='back_to_menu')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    if query:
        await query.answer()
        try:
            await query.edit_message_text(msg, parse_mode='Markdown', reply_markup=reply_markup)
        except BadRequest as e:
            logger.error(f"Failed to edit message for list_ads: {e}")
            await query.message.reply_text(msg, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        context.user_data['ads_page'] = 0
        await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=reply_markup)
    logger.info(f"Admin {user_id} viewed queued ads: page {page + 1}/{total_pages}")
    await asyncio.sleep(0.1)
    return ConversationHandler.END if query else None

async def delete_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if user_id not in ADMIN_IDS:
        await query.edit_message_text("Unauthorized access.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    cursor = db.cursor()
    cursor.execute('SELECT id, message, created_at FROM ads')
    ads = [{'id': row[0], 'message': row[1], 'created_at': row[2]} for row in cursor.fetchall()]
    if not ads:
        keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.edit_message_text("No ads found to delete.", reply_markup=reply_markup)
        except BadRequest as e:
            logger.error(f"Failed to edit message for delete_ad: {e}")
            await query.message.reply_text("No ads found to delete.", reply_markup=reply_markup)
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    keyboard = [
        [InlineKeyboardButton(f"ID {ad['id']}: {ad['message'][:20]}...", callback_data=f'delete_ad:{ad["id"]}')]
        for ad in sorted(ads, key=lambda x: x['created_at'])
    ]
    keyboard.append([InlineKeyboardButton("Back", callback_data='back_to_menu')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await query.edit_message_text("Select an ad to delete:", reply_markup=reply_markup)
    except BadRequest as e:
        logger.error(f"Failed to edit message for delete_ad: {e}")
        await query.message.reply_text("Select an ad to delete:", reply_markup=reply_markup)
    await asyncio.sleep(0.1)
    return DELETE_AD

async def confirm_delete_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if user_id not in ADMIN_IDS:
        await query.edit_message_text("Unauthorized access.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    if query.data.startswith('delete_ad:'):
        ad_id = query.data.split(':')[1]
        try:
            ad_id = int(ad_id)
        except ValueError:
            await query.edit_message_text("Invalid ad ID.")
            await asyncio.sleep(0.1)
            return ConversationHandler.END

        cursor = db.cursor()
        cursor.execute('SELECT message FROM ads WHERE id = ?', (ad_id,))
        ad = cursor.fetchone()
        if not ad:
            await query.edit_message_text("Ad not found.")
            await asyncio.sleep(0.1)
            return ConversationHandler.END

        context.user_data['delete_ad_id'] = ad_id
        keyboard = [
            [InlineKeyboardButton("Yes", callback_data='confirm_delete:yes')],
            [InlineKeyboardButton("No", callback_data='confirm_delete:no')],
            [InlineKeyboardButton("Back", callback_data='back_to_menu')]
        ]
        try:
            await query.edit_message_text(
                f"Are you sure you want to delete ad ID {ad_id}: {ad[0][:30]}...?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except BadRequest as e:
            logger.error(f"Failed to edit message for confirm_delete_ad: {e}")
            await query.message.reply_text(
                f"Are you sure you want to delete ad ID {ad_id}: {ad[0][:30]}...?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        await asyncio.sleep(0.1)
        return CONFIRM_DELETE_AD

    if query.data == 'confirm_delete:yes':
        ad_id = context.user_data.get('delete_ad_id')
        if ad_id is None:
            await query.edit_message_text("Error: No ad selected.")
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        cursor = db.cursor()
        cursor.execute('SELECT message FROM ads WHERE id = ?', (ad_id,))
        ad = cursor.fetchone()
        if ad:
            try:
                cursor.execute('DELETE FROM ads WHERE id = ?', (ad_id,))
                db.commit()
                logger.info(f"Admin {user_id} deleted ad {ad_id}")
                await query.edit_message_text(f"Ad ID {ad_id} deleted successfully.")
            except sqlite3.Error as e:
                logger.error(f"Database error deleting ad {ad_id}: {e}")
                await query.edit_message_text("Error: Failed to delete ad.")
        else:
            await query.edit_message_text("Error: Ad not found.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    if query.data == 'confirm_delete:no':
        await query.edit_message_text("Delete action cancelled.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    if query.data == 'back_to_menu':
        context.user_data.clear()
        return await back_to_menu(update, context)

    await query.edit_message_text("Invalid option.")
    context.user_data.clear()
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user = get_user(user_id)
    logger.debug(f"Callback query from user {user_id}: {query.data}")

    try:
        context.user_data.clear()  # Clear user_data to prevent state mismatches
        if query.data == 'monitor':
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="Please send the contract address to monitor:",
                    reply_markup=ReplyKeyboardRemove()
                )
            except (BadRequest, NetworkError) as e:
                logger.error(f"Failed to send message to user {user_id}: {e}")
                await query.edit_message_text("Failed to proceed. Please try /start.")
                return ConversationHandler.END
            context.user_data['state'] = ENTER_ADDRESS
            await asyncio.sleep(0.1)
            return ENTER_ADDRESS
        elif query.data == 'unmonitor':
            watchlist = user.get('watchlist', [])
            if not watchlist:
                await query.edit_message_text("Your watchlist is empty.")
                return ConversationHandler.END
            keyboard = [
                [InlineKeyboardButton(f"{sanitize_markdown(t['symbol'])} ({t['chain']})", callback_data=f"unmonitor:{i}")]
                for i, t in enumerate(watchlist)
            ]
            keyboard.append([InlineKeyboardButton("Back", callback_data='back_to_menu')])
            try:
                await query.edit_message_text(
                    "Select a token to unmonitor:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except BadRequest as e:
                logger.error(f"Failed to edit message for unmonitor: {e}")
                await query.message.reply_text(
                    "Select a token to unmonitor:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            context.user_data['state'] = CONFIRM_UNMONITOR
            await asyncio.sleep(0.1)
            return CONFIRM_UNMONITOR
        elif query.data == 'watchlist':
            watchlist = user.get('watchlist', [])
            if not watchlist:
                msg = "Your watchlist is empty."
            else:
                msg = "📋 **Your Watchlist**:\n\n"
                for item in watchlist:
                    try:
                        chain_id = DEXSCREENER_CHAINS.get(item['chain'], item['chain'])
                        address = item.get('full_address', item.get('address', ''))
                        dexscreener_link = f"https://dexscreener.com/{chain_id}/{address}"
                        sanitized_symbol = sanitize_markdown(item['symbol'])
                        msg += (
                            f"**{sanitized_symbol}** ({item['chain']}) - {item['type']} - {item['value']} - "
                            f"Market Cap: ${item['market_cap']:,.2f}\n"
                            f"[View on DexScreener]({dexscreener_link})\n"
                        )
                    except Exception as e:
                        logger.error(f"Error processing watchlist item {item}: {e}")
                        msg += f"Error displaying token {item.get('symbol', 'Unknown')}\n"
            try:
                await query.edit_message_text(
                    msg,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
            except BadRequest as e:
                logger.error(f"Markdown error in watchlist: {e}")
                plain_msg = "📋 Your Watchlist:\n\n"
                for item in watchlist:
                    chain_id = DEXSCREENER_CHAINS.get(item['chain'], item['chain'])
                    address = item.get('full_address', item.get('address', ''))
                    dexscreener_link = f"https://dexscreener.com/{chain_id}/{address}"
                    plain_msg += (
                        f"{item['symbol']} ({item['chain']}) - {item['type']} - {item['value']} - "
                        f"Market Cap: ${item['market_cap']:,.2f}\n"
                        f"View: {dexscreener_link}\n"
                    )
                await query.edit_message_text(plain_msg, disable_web_page_preview=True)
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        elif query.data == 'top_monitored':
            current_time = time.time()
            if top_monitored_cache['result'] and current_time - top_monitored_cache['timestamp'] < CACHE_DURATION:
                msg = top_monitored_cache['result']
            else:
                cursor = db.cursor()
                cursor.execute('SELECT user_id, watchlist FROM users')
                monitored_tokens = {}
                for user_id, watchlist_json in cursor.fetchall():
                    watchlist = json.loads(watchlist_json)
                    for t in watchlist:
                        address = t.get('full_address', t.get('address', ''))
                        key = f"{t['chain']}:{address}"
                        monitored_tokens.setdefault(key, []).append(user_id)
                top_tokens = sorted(monitored_tokens.items(), key=lambda x: len(x[1]), reverse=True)[:5]
                msg = "🔝 **Top Monitored Tokens**:\n\n"
                for key, users in top_tokens:
                    chain, address = key.split(":")
                    _, symbol, market_cap = await get_token_price(address, chain, context.bot)
                    symbol = sanitize_markdown(symbol or "Unknown")
                    market_cap = market_cap or 0.0
                    chain_id = DEXSCREENER_CHAINS.get(chain, chain)
                    dexscreener_link = f"https://dexscreener.com/{chain_id}/{address}"
                    msg += (
                        f"**{symbol}** on {chain} - {len(users)} users - "
                        f"Market Cap: ${market_cap:,.2f}\n"
                        f"[View on DexScreener]({dexscreener_link})\n"
                    )
                if not top_tokens:
                    msg = "No tokens monitored yet."
                top_monitored_cache['result'] = msg
                top_monitored_cache['timestamp'] = current_time

            ad = get_active_ad()
            if ad:
                msg += f"\n📣 Ad: {sanitize_markdown(ad['message'])}"
                cursor = db.cursor()
                cursor.execute('UPDATE ads SET current_views = current_views + 1 WHERE id = ?', (ad['id'],))
                db.commit()
                cursor.execute('SELECT current_views, max_views FROM ads WHERE id = ?', (ad['id'],))
                updated_ad = cursor.fetchone()
                if updated_ad and updated_ad[0] >= updated_ad[1]:
                    cursor.execute('UPDATE ads SET active = 0 WHERE id = ?', (ad['id'],))
                    db.commit()

            try:
                await query.edit_message_text(msg, parse_mode='Markdown', disable_web_page_preview=True)
            except BadRequest as e:
                logger.error(f"Failed to edit message for top_monitored: {e}")
                await query.message.reply_text(msg, parse_mode='Markdown', disable_web_page_preview=True)
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        elif query.data == 'leaderboard':
            current_time = time.time()
            leaderboard_cache_key = 'leaderboard'
            if (leaderboard_cache_key in top_monitored_cache and
                current_time - top_monitored_cache[leaderboard_cache_key].get('timestamp', 0) < CACHE_DURATION):
                msg = top_monitored_cache[leaderboard_cache_key]['result']
            else:
                cursor = db.cursor()
                cursor.execute('SELECT watchlist FROM users')
                monitored_tokens = {}
                for row in cursor.fetchall():
                    watchlist = json.loads(row[0])
                    for t in watchlist:
                        address = t.get('full_address', t.get('address', ''))
                        key = f"{t['chain']}:{address}"
                        monitored_tokens[key] = {
                            'symbol': t['symbol'],
                            'chain': t['chain'],
                            'address': address
                        }

                all_changes = []
                async with aiohttp.ClientSession() as session:
                    for key, token_info in monitored_tokens.items():
                        chain, address = key.split(":")
                        try:
                            url = f"{DEXSCREENER_TOKEN_API}{address}"
                            async with session.get(url, timeout=10) as response:
                                await asyncio.sleep(0.1)
                                if response.status != 200:
                                    logger.error(f"DexScreener API error: Status {response.status}")
                                    continue
                                res = await response.json()
                                pairs = res.get('pairs', [])
                                if not pairs:
                                    continue
                                target_pair = next(
                                    (pair for pair in pairs if pair.get('chainId') == DEXSCREENER_CHAINS.get(chain, chain)),
                                    max(pairs, key=lambda p: p.get('liquidity', {}).get('usd', 0), default=None)
                                )
                                if not target_pair:
                                    continue
                                price_change_24h = float(target_pair.get('priceChange', {}).get('h24', 0))
                                chain_id = DEXSCREENER_CHAINS.get(chain, chain)
                                dexscreener_link = f"https://dexscreener.com/{chain_id}/{address}"
                                all_changes.append((
                                    key,
                                    token_info['symbol'],
                                    price_change_24h,
                                    dexscreener_link,
                                    chain
                                ))
                        except Exception as e:
                            logger.error(f"Error fetching DexScreener data for {key}: {e}")
                            continue

                gainers = sorted(all_changes, key=lambda x: x[2], reverse=True)[:30]
                losers = sorted(all_changes, key=lambda x: x[2])[:30]
                msg = "🏆 **Top 30 Gainers (24h)**:\n\n"
                for _, symbol, change, link, chain in gainers:
                    symbol = sanitize_markdown(symbol)
                    msg += f"**{symbol}** on {chain}: {change:.2f}%\n[View on DexScreener]({link})\n"
                msg += "\n🔻 **Top 30 Losers (24h)**:\n\n"
                for _, symbol, change, link, chain in losers:
                    symbol = sanitize_markdown(symbol)
                    msg += f"**{symbol}** on {chain}: {change:.2f}%\n[View on DexScreener]({link})\n"
                if not gainers and not losers:
                    msg = "No price change data available."

                top_monitored_cache[leaderboard_cache_key] = {
                    'result': msg,
                    'timestamp': current_time
                }

            ad = get_active_ad()
            if ad:
                msg += f"\n📣 Ad: {sanitize_markdown(ad['message'])}"
                cursor = db.cursor()
                cursor.execute('UPDATE ads SET current_views = current_views + 1 WHERE id = ?', (ad['id'],))
                db.commit()
                cursor.execute('SELECT current_views, max_views FROM ads WHERE id = ?', (ad['id'],))
                updated_ad = cursor.fetchone()
                if updated_ad and updated_ad[0] >= updated_ad[1]:
                    cursor.execute('UPDATE ads SET active = 0 WHERE id = ?', (ad['id'],))
                    db.commit()

            try:
                await query.edit_message_text(
                    msg,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
            except BadRequest as e:
                logger.error(f"Failed to edit message for leaderboard: {e}")
                await query.message.reply_text(
                    msg,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        elif query.data == 'broadcast' and user_id in ADMIN_IDS:
            keyboard = [
                [InlineKeyboardButton("Text", callback_data='broadcast_type:text')],
                [InlineKeyboardButton("Photo", callback_data='broadcast_type:photo')],
                [InlineKeyboardButton("Video", callback_data='broadcast_type:video')],
                [InlineKeyboardButton("Back", callback_data='back_to_menu')]
            ]
            try:
                await query.edit_message_text(
                    "Choose the broadcast type:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except BadRequest as e:
                logger.error(f"Failed to edit message for broadcast: {e}")
                await query.message.reply_text(
                    "Choose the broadcast type:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            await asyncio.sleep(0.1)
            return BROADCAST_TYPE
        elif query.data == 'clear_watchlist' and user_id in ADMIN_IDS:
            await query.edit_message_text("Enter the Telegram user ID to clear their watchlist:")
            await asyncio.sleep(0.1)
            return CLEAR_WATCHLIST
        elif query.data == 'post_ad' and user_id in ADMIN_IDS:
            await query.edit_message_text("Enter the ad message (up to 160 characters):")
            await asyncio.sleep(0.1)
            return POST_AD_MESSAGE
        elif query.data == 'list_ads' and user_id in ADMIN_IDS:
            await list_ads(update, context)
            return ConversationHandler.END
        elif query.data == 'delete_ad' and user_id in ADMIN_IDS:
            await delete_ad(update, context)
            return DELETE_AD
        elif query.data.startswith('list_ads:'):
            page = int(query.data.split(':')[1])
            context.user_data['ads_page'] = page
            await list_ads(update, context)
            return ConversationHandler.END
        elif query.data == 'back_to_menu':
            return await back_to_menu(update, context)
        logger.warning(f"Unrecognized callback data from user {user_id}: {query.data}")
        try:
            await query.edit_message_text("Invalid option. Please try again.")
        except BadRequest as e:
            logger.error(f"Failed to edit message for invalid callback: {e}")
            await query.message.reply_text("Invalid option. Please try again.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in menu_handler for user {user_id}, callback {query.data}: {e}", exc_info=True)
        try:
            await query.edit_message_text("An error occurred. Please use /start.")
        except Exception as e2:
            logger.error(f"Failed to send error message: {e2}")
            await query.message.reply_text("An error occurred. Please use /start.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END

async def readd_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.debug(f"Re-add token callback from user {user_id}: {query.data}")

    try:
        if query.data.startswith('readd_token:'):
            token_data = query.data.split(':', 2)[1]
            try:
                token = json.loads(token_data)
                user = get_user(user_id)
                watchlist = user.get('watchlist', [])
                required_fields = ['full_address', 'chain', 'type', 'value', 'symbol', 'initial_price', 'market_cap', 'last_price', 'last_market_cap']
                if not all(field in token for field in required_fields):
                    logger.error(f"Invalid token data for re-add: {token}")
                    await query.edit_message_text("Error: Invalid token data.")
                    return ConversationHandler.END
                watchlist.append(token)
                save_watchlist(user_id, watchlist)
                try:
                    await query.edit_message_text(
                        f"**{sanitize_markdown(token['symbol'])}** re-added to watchlist!",
                        parse_mode='Markdown'
                    )
                except BadRequest as e:
                    logger.error(f"Failed to edit message for readd_token: {e}")
                    await query.message.reply_text(
                        f"**{sanitize_markdown(token['symbol'])}** re-added to watchlist!",
                        parse_mode='Markdown'
                    )
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing token data: {e}")
                await query.edit_message_text("Error: Unable to re-add token.")
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        await query.edit_message_text("Invalid option.")
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in readd_token: {e}")
        try:
            await query.edit_message_text("An error occurred. Please use /start.")
        except BadRequest as e2:
            logger.error(f"Failed to send error message: {e2}")
            await query.message.reply_text("An error occurred. Please use /start.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END

async def confirm_unmonitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user = get_user(user_id)
    watchlist = user.get('watchlist', [])
    logger.debug(f"Confirm unmonitor callback: {query.data}")

    try:
        if query.data.startswith('unmonitor:'):
            try:
                index = int(query.data.split(':')[1])
                if 0 <= index < len(watchlist):
                    token = watchlist[index]
                    context.user_data['unmonitor_index'] = index
                    keyboard = [
                        [InlineKeyboardButton("Yes", callback_data='confirm_unmonitor:yes')],
                        [InlineKeyboardButton("No", callback_data='confirm_unmonitor:no')],
                        [InlineKeyboardButton("Back", callback_data='back_to_menu')]
                    ]
                    try:
                        await query.edit_message_text(
                            f"Remove **{sanitize_markdown(token['symbol'])}** ({token['chain']}) from watchlist?",
                            parse_mode='Markdown',
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                    except BadRequest as e:
                        logger.error(f"Failed to edit message for confirm_unmonitor: {e}")
                        await query.message.reply_text(
                            f"Remove **{sanitize_markdown(token['symbol'])}** ({token['chain']}) from watchlist?",
                            parse_mode='Markdown',
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                    await asyncio.sleep(0.1)
                    return CONFIRM_UNMONITOR
            except (ValueError, IndexError) as e:
                logger.error(f"Error in unmonitor selection: {e}")
                await query.edit_message_text("Invalid selection.")
                return ConversationHandler.END
        elif query.data == 'confirm_unmonitor:yes':
            index = context.user_data.get('unmonitor_index')
            if index is not None and 0 <= index < len(watchlist):
                removed_token = watchlist.pop(index)
                save_watchlist(user_id, watchlist)
                try:
                    await query.edit_message_text(
                        f"**{sanitize_markdown(removed_token['symbol'])}** removed from watchlist.",
                        parse_mode='Markdown'
                    )
                except BadRequest as e:
                    logger.error(f"Failed to edit message for confirm_unmonitor: {e}")
                    await query.message.reply_text(
                        f"**{sanitize_markdown(removed_token['symbol'])}** removed from watchlist.",
                        parse_mode='Markdown'
                    )
            else:
                await query.edit_message_text("Error: Invalid token selection.")
            context.user_data.clear()
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        elif query.data == 'confirm_unmonitor:no':
            await query.edit_message_text("Unmonitor action cancelled.")
            context.user_data.clear()
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        elif query.data == 'back_to_menu':
            context.user_data.clear()
            return await back_to_menu(update, context)
        await query.edit_message_text("Invalid option.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in confirm_unmonitor: {e}")
        try:
            await query.edit_message_text("An error occurred. Please use /start.")
        except BadRequest as e2:
            logger.error(f"Failed to send error message: {e2}")
            await query.message.reply_text("An error occurred. Please use /start.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END

async def receive_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    address = update.message.text.strip()
    user_id = update.message.from_user.id
    logger.debug(f"Received address from user {user_id}: {address}")
    # Supports EVM (0x...), Solana (base58), TON (raw 0:..., user-friendly 48 chars)
    if not re.match(r'^0x[a-fA-F0-9]{40}$|^[1-9A-HJ-NP-Za-km-z]{32,44}$|^0:[a-fA-F0-9]{64}$|^[A-Za-z0-9_-]{48}$', address):
        await update.message.reply_text(
            "Invalid address. Please enter a valid Ethereum, BSC, Polygon, Base, Solana, or TON address.\n"
            "TON addresses start with '0:' or are 48-char base64 strings (e.g., EQ...)."
        )
        await asyncio.sleep(0.1)
        return ENTER_ADDRESS
    context.user_data['address'] = address
    keyboard = [[InlineKeyboardButton(chain.title(), callback_data=chain)] for chain in CHAINS]
    keyboard.append([InlineKeyboardButton("Back", callback_data='back_to_menu')])
    await update.message.reply_text("Select the blockchain:", reply_markup=InlineKeyboardMarkup(keyboard))
    await asyncio.sleep(0.1)
    return SELECT_CHAIN

async def select_chain(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.debug(f"User {user_id} selected chain: {query.data}")
    if query.data == 'back_to_menu':
        context.user_data.clear()
        return await back_to_menu(update, context)
    context.user_data['chain'] = query.data
    keyboard = [
        [InlineKeyboardButton("Price Change", callback_data='price')],
        [InlineKeyboardButton("% Increase", callback_data='increase')],
        [InlineKeyboardButton("% Decrease", callback_data='decrease')],
        [InlineKeyboardButton("Market Cap", callback_data='market_cap')],
        [InlineKeyboardButton("Back", callback_data='back_to_menu')]
    ]
    try:
        await query.edit_message_text(
            "How do you want to monitor the token?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except BadRequest as e:
        logger.error(f"Failed to edit message for select_chain: {e}")
        await query.message.reply_text(
            "How do you want to monitor the token?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    await asyncio.sleep(0.1)
    return SELECT_MONITOR_TYPE

async def select_monitor_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.debug(f"User {user_id} selected monitor type: {query.data}")
    if query.data == 'back_to_menu':
        context.user_data.clear()
        return await back_to_menu(update, context)
    context.user_data['type'] = query.data
    prompt = "Enter the market cap trigger value (in USD):" if query.data == 'market_cap' else f"Enter the value for {query.data} trigger:"
    try:
        await query.edit_message_text(prompt)
    except BadRequest as e:
        logger.error(f"Failed to edit message for select_monitor_type: {e}")
        await query.message.reply_text(prompt)
    await asyncio.sleep(0.1)
    return ENTER_VALUE

async def enter_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    user = get_user(user_id)
    logger.debug(f"Received value from user {user_id}: {update.message.text}")
    try:
        context.user_data['value'] = float(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("Please enter a valid number.")
        await asyncio.sleep(0.1)
        return ENTER_VALUE
    address = context.user_data['address']
    chain = context.user_data['chain']
    price, symbol, market_cap = await get_token_price(address, chain, context.bot)
    if price is None or symbol is None or symbol.startswith('Unknown_'):
        logger.warning(f"Invalid token: price={price}, symbol={symbol}, market_cap={market_cap} for {address} on {chain}")
        keyboard = [
            [InlineKeyboardButton("Enter Custom Symbol", callback_data='confirm_token:custom')],
            [InlineKeyboardButton("Confirm Address", callback_data='confirm_token:yes')],
            [InlineKeyboardButton("Enter New Address", callback_data='confirm_token:no')],
            [InlineKeyboardButton("Back", callback_data='back_to_menu')]
        ]
        await update.message.reply_text(
            f"Could not retrieve token details for address {address}. Choose an option:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await asyncio.sleep(0.1)
        return CONFIRM_TOKEN
    token = {
        'full_address': address,
        'chain': chain,
        'type': context.user_data['type'],
        'value': context.user_data['value'],
        'symbol': symbol,
        'initial_price': price,
        'market_cap': market_cap,
        'last_price': price,
        'last_market_cap': market_cap
    }
    user['watchlist'].append(token)
    save_watchlist(user_id, user['watchlist'])
    await update.message.reply_text(
        f"**{sanitize_markdown(symbol)}** added to watchlist! Market Cap: ${market_cap:,.2f}",
        parse_mode='Markdown'
    )
    context.user_data.clear()
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def confirm_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.debug(f"Confirm token callback: {query.data}")
    try:
        if query.data == 'confirm_token:custom':
            await query.edit_message_text("Enter a custom symbol (e.g., USDC, ETH):")
            await asyncio.sleep(0.1)
            return ENTER_SYMBOL
        elif query.data == 'confirm_token:yes':
            address = context.user_data['address']
            chain = context.user_data['chain']
            symbol = f"Token_{len(get_user(user_id)['watchlist']) + 1}"
            price = 0.0
            market_cap = 0.0
            token = {
                'full_address': address,
                'chain': chain,
                'type': context.user_data['type'],
                'value': context.user_data['value'],
                'symbol': symbol,
                'initial_price': price,
                'market_cap': market_cap,
                'last_price': price,
                'last_market_cap': market_cap
            }
            user = get_user(user_id)
            user['watchlist'].append(token)
            save_watchlist(user_id, user['watchlist'])
            try:
                await query.edit_message_text(
                    f"**{sanitize_markdown(symbol)}** added to watchlist! Market Cap: ${market_cap:,.2f}",
                    parse_mode='Markdown'
                )
            except BadRequest as e:
                logger.error(f"Failed to edit message for confirm_token: {e}")
                await query.message.reply_text(
                    f"**{sanitize_markdown(symbol)}** added to watchlist! Market Cap: ${market_cap:,.2f}",
                    parse_mode='Markdown'
                )
            context.user_data.clear()
            await asyncio.sleep(0.1)
            return ConversationHandler.END
        elif query.data == 'confirm_token:no':
            await query.edit_message_text("Please send a new contract address:")
            context.user_data.clear()
            await asyncio.sleep(0.1)
            return ENTER_ADDRESS
        elif query.data == 'back_to_menu':
            context.user_data.clear()
            return await back_to_menu(update, context)
        await query.edit_message_text("Invalid option.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in confirm_token: {e}")
        try:
            await query.edit_message_text("An error occurred. Please use /start.")
        except BadRequest as e2:
            logger.error(f"Failed to send error message: {e2}")
            await query.message.reply_text("An error occurred. Please use /start.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END

async def enter_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    user = get_user(user_id)
    symbol = update.message.text.strip()
    logger.debug(f"Received symbol: {symbol}")
    if not re.match(r'^[a-zA-Z0-9]{1,10}$', symbol):
        await update.message.reply_text("Invalid symbol. Use 1-10 alphanumeric characters.")
        await asyncio.sleep(0.1)
        return ENTER_SYMBOL
    address = context.user_data['address']
    chain = context.user_data['chain']
    price, _, market_cap = await get_token_price(address, chain, context.bot)
    price = price or 0.0
    market_cap = market_cap or 0.0
    token = {
        'full_address': address,
        'chain': chain,
        'type': context.user_data['type'],
        'value': context.user_data['value'],
        'symbol': symbol,
        'initial_price': price,
        'market_cap': market_cap,
        'last_price': price,
        'last_market_cap': market_cap
    }
    user['watchlist'].append(token)
    save_watchlist(user_id, user['watchlist'])
    await update.message.reply_text(
        f"**{sanitize_markdown(symbol)}** added to watchlist! Market Cap: ${market_cap:,.2f}",
        parse_mode='Markdown'
    )
    context.user_data.clear()
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def broadcast_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if user_id not in ADMIN_IDS:
        await query.edit_message_text("Unauthorized access.")
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    if query.data == 'back_to_menu':
        context.user_data.clear()
        return await back_to_menu(update, context)

    broadcast_type = query.data.split(':')[1]
    context.user_data['broadcast_type'] = broadcast_type
    prompt = {
        'text': "Send the text message to broadcast:",
        'photo': "Send the photo to broadcast (max 20 MB):",
        'video': "Send the video to broadcast (max 50 MB):"
    }[broadcast_type]
    try:
        await query.edit_message_text(prompt)
    except BadRequest as e:
        logger.error(f"Failed to edit message for broadcast_type: {e}")
        await query.message.reply_text(prompt)
    await asyncio.sleep(0.1)
    return BROADCAST_MESSAGE

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("Unauthorized access.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    broadcast_type = context.user_data.get('broadcast_type')
    if not broadcast_type:
        await update.message.reply_text("Error: Broadcast type not set. Use /start.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    sent = 0
    cursor = db.cursor()
    cursor.execute('SELECT user_id FROM users')
    user_ids = [row[0] for row in cursor.fetchall()]

    if broadcast_type == 'text':
        text = update.message.text
        if not text or len(text) > 4096:
            await update.message.reply_text("Text must be 1-4096 characters.")
            await asyncio.sleep(0.1)
            return BROADCAST_MESSAGE
        for target_user_id in user_ids:
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=f"📢 Admin Message:\n{sanitize_markdown(text)}",
                    parse_mode='Markdown'
                )
                sent += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(f"Failed to send text broadcast to user {target_user_id}: {e}")
                continue
    elif broadcast_type == 'photo':
        if not update.message.photo:
            await update.message.reply_text("Please send a photo (max 20 MB).")
            await asyncio.sleep(0.1)
            return BROADCAST_MESSAGE
        photo = update.message.photo[-1]
        if photo.file_size > 20 * 1024 * 1024:
            await update.message.reply_text("Photo exceeds 20 MB.")
            await asyncio.sleep(0.1)
            return BROADCAST_MESSAGE
        caption = update.message.caption or ""
        if len(caption) > 1024:
            await update.message.reply_text("Caption must be 0-1024 characters.")
            await asyncio.sleep(0.1)
            return BROADCAST_MESSAGE
        for target_user_id in user_ids:
            try:
                await context.bot.send_photo(
                    chat_id=target_user_id,
                    photo=photo.file_id,
                    caption=f"📢 Admin Broadcast:\n{sanitize_markdown(caption)}",
                    parse_mode='Markdown'
                )
                sent += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(f"Failed to send photo broadcast to user {target_user_id}: {e}")
                continue
    elif broadcast_type == 'video':
        if not update.message.video:
            await update.message.reply_text("Please send a video (max 50 MB).")
            await asyncio.sleep(0.1)
            return BROADCAST_MESSAGE
        video = update.message.video
        if video.file_size > 50 * 1024 * 1024:
            await update.message.reply_text("Video exceeds 50 MB.")
            await asyncio.sleep(0.1)
            return BROADCAST_MESSAGE
        caption = update.message.caption or ""
        if len(caption) > 1024:
            await update.message.reply_text("Caption must be 0-1024 characters.")
            await asyncio.sleep(0.1)
            return BROADCAST_MESSAGE
        for target_user_id in user_ids:
            try:
                await context.bot.send_video(
                    chat_id=target_user_id,
                    video=video.file_id,
                    caption=f"📢 Admin Broadcast:\n{sanitize_markdown(caption)}",
                    parse_mode='Markdown'
                )
                sent += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(f"Failed to send video broadcast to user {target_user_id}: {e}")
                continue

    await update.message.reply_text(f"Broadcast sent to {sent} users.")
    context.user_data.clear()
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def clear_user_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("Unauthorized access.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    try:
        target_user_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("Invalid user ID. Enter a numeric Telegram user ID.")
        await asyncio.sleep(0.1)
        return CLEAR_WATCHLIST

    target_user = get_user(target_user_id)
    if not target_user:
        await update.message.reply_text(f"No user found with ID {target_user_id}.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    if not target_user.get('watchlist'):
        await update.message.reply_text(f"User {target_user_id}'s watchlist is empty.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    save_watchlist(target_user_id, [])
    await update.message.reply_text(f"Watchlist for user {target_user_id} cleared.")
    context.user_data.clear()
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def post_ad_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("Unauthorized access.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    message = update.message.text.strip()
    if len(message) > 160:
        await update.message.reply_text("Ad message must be 160 characters or less.")
        await asyncio.sleep(0.1)
        return POST_AD_MESSAGE

    context.user_data['ad_message'] = message
    keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_menu')]]
    await update.message.reply_text(
        "Enter the duration in days (1-30):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    await asyncio.sleep(0.1)
    return POST_AD_DURATION

async def post_ad_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("Unauthorized access.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    try:
        duration = int(update.message.text.strip())
        if not 1 <= duration <= 30:
            raise ValueError("Duration out of range")
    except ValueError:
        await update.message.reply_text("Enter a valid number of days (1-30).")
        await asyncio.sleep(0.1)
        return POST_AD_DURATION

    context.user_data['ad_duration'] = duration
    keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_menu')]]
    await update.message.reply_text(
        "Enter the maximum number of views (1-10000):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    await asyncio.sleep(0.1)
    return POST_AD_VIEWS

async def post_ad_views(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("Unauthorized access.")
        context.user_data.clear()
        await asyncio.sleep(0.1)
        return ConversationHandler.END

    try:
        max_views = int(update.message.text.strip())
        if not 1 <= max_views <= 10000:
            raise ValueError("Views out of range")
    except ValueError:
        await update.message.reply_text("Enter a valid number of views (1-10000).")
        await asyncio.sleep(0.1)
        return POST_AD_VIEWS

    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO ads (message, duration_days, max_views, current_views, created_at, active)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        context.user_data['ad_message'],
        context.user_data['ad_duration'],
        max_views,
        0,
        datetime.utcnow().isoformat(),
        1
    ))
    ad_id = cursor.lastrowid
    db.commit()
    await update.message.reply_text(
        f"Ad posted! ID: {ad_id}, Message: {context.user_data['ad_message']}, Duration: {context.user_data['ad_duration']} days, Max Views: {max_views}."
    )
    context.user_data.clear()
    await asyncio.sleep(0.1)
    return ConversationHandler.END

async def monitor_tokens(bot):
    while True:
        start_time = time.time()
        try:
            current_time = time.time()
            notifications = []
            cursor = db.cursor()
            cursor.execute('SELECT user_id, watchlist FROM users')
            for user_id, watchlist_json in cursor.fetchall():
                watchlist = json.loads(watchlist_json)
                tokens_to_remove = []
                removed_tokens = []
                for idx, token in enumerate(watchlist):
                    address = token.get('full_address', token.get('address', ''))
                    cache_key = f"{token['chain']}:{address}"
                    if failed_attempts.get(cache_key, 0) >= 5:
                        continue
                    price, _, market_cap = await get_token_price(address, token['chain'], bot)
                    if price is None or token['initial_price'] is None:
                        continue
                    if cache_key not in price_history_cache:
                        price_history_cache[cache_key] = []
                    price_history_cache[cache_key].append((current_time, price))
                    price_history_cache[cache_key] = [
                        (t, p) for t, p in price_history_cache[cache_key]
                        if current_time - t <= 25 * 3600
                    ]
                    last_price = token.get('last_price', token['initial_price'])
                    last_market_cap = token.get('last_market_cap', token.get('market_cap', 0.0))
                    token['last_price'] = price
                    token['last_market_cap'] = market_cap or 0.0
                    token['market_cap'] = market_cap or 0.0
                    try:
                        message = None
                        chain_id = DEXSCREENER_CHAINS.get(token['chain'], token['chain'])
                        dexscreener_link = f"https://dexscreener.com/{chain_id}/{address}"
                        if token['type'] == 'price':
                            if ((last_price < token['value'] and price >= token['value']) or
                                (last_price > token['value'] and price <= token['value'])):
                                full_price = f"{token['value']:.20f}".rstrip('0')
                                message = (
                                    f"💹 **{sanitize_markdown(token['symbol'])}** reached ${full_price} on {token['chain']}.\n"
                                    f"Market Cap: ${market_cap:,.2f}\n"
                                    f"[View on DexScreener]({dexscreener_link})"
                                )
                        elif token['type'] == 'increase':
                            pct = ((price - token['initial_price']) / token['initial_price']) * 100
                            last_pct = ((last_price - token['initial_price']) / token['initial_price']) * 100
                            if last_pct < token['value'] and pct >= token['value']:
                                full_price = f"{price:.20f}".rstrip('0')
                                message = (
                                    f"📈 **{sanitize_markdown(token['symbol'])}** increased {token['value']:.2f}% to ${full_price} on {token['chain']}.\n"
                                    f"Market Cap: ${market_cap:,.2f}\n"
                                    f"[View on DexScreener]({dexscreener_link})"
                                )
                        elif token['type'] == 'decrease':
                            pct = ((price - token['initial_price']) / token['initial_price']) * 100
                            last_pct = ((last_price - token['initial_price']) / token['initial_price']) * 100
                            if last_pct > -token['value'] and pct <= -token['value']:
                                full_price = f"{price:.20f}".rstrip('0')
                                message = (
                                    f"📉 **{sanitize_markdown(token['symbol'])}** decreased {token['value']:.2f}% to ${full_price} on {token['chain']}.\n"
                                    f"Market Cap: ${market_cap:,.2f}\n"
                                    f"[View on DexScreener]({dexscreener_link})"
                                )
                        elif token['type'] == 'market_cap':
                            if last_market_cap < token['value'] and market_cap >= token['value']:
                                full_price = f"{price:.20f}".rstrip('0')
                                message = (
                                    f"💰 **{sanitize_markdown(token['symbol'])}** market cap reached ${token['value']:,.2f} on {token['chain']}.\n"
                                    f"Price: ${full_price}\n"
                                    f"Market Cap: ${market_cap:,.2f}\n"
                                    f"[View on DexScreener]({dexscreener_link})"
                                )
                        if message:
                            keyboard = [[InlineKeyboardButton("Re-add Token", callback_data=f'readd_token:{json.dumps(token)}')]]
                            reply_markup = InlineKeyboardMarkup(keyboard)
                            notifications.append((user_id, message, reply_markup))
                            tokens_to_remove.append(idx)
                            removed_tokens.append(token)
                    except Exception as e:
                        logger.warning(f"Failed to process token for user {user_id}: {e}")
                        for admin_id in ADMIN_IDS:
                            await bot.send_message(chat_id=admin_id, text=f"Error for user {user_id}: {e}")
                if tokens_to_remove:
                    watchlist = [t for i, t in enumerate(watchlist) if i not in tokens_to_remove]
                    save_watchlist(user_id, watchlist)
            for user_id, message, reply_markup in notifications:
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode='Markdown',
                        disable_web_page_preview=True,
                        reply_markup=reply_markup
                    )
                    ad = get_active_ad()
                    if ad:
                        await bot.send_message(
                            chat_id=user_id,
                            text=f"📣 Ad: {sanitize_markdown(ad['message'])}",
                            parse_mode='Markdown'
                        )
                        cursor = db.cursor()
                        cursor.execute('UPDATE ads SET current_views = current_views + 1 WHERE id = ?', (ad['id'],))
                        db.commit()
                except Exception as e:
                    logger.warning(f"Failed to send message to user {user_id}: {e}")
                    for admin_id in ADMIN_IDS:
                        await bot.send_message(chat_id=admin_id, text=f"Error for user {user_id}: {e}")
            elapsed = time.time() - start_time
            if elapsed > 10:
                logger.warning(f"monitor_tokens iteration took {elapsed:.2f} seconds")
        except Exception as e:
            logger.error(f"Error in monitor_tokens: {e}")
            for admin_id in ADMIN_IDS:
                await bot.send_message(chat_id=admin_id, text=f"Monitor tokens error: {e}")
        await asyncio.sleep(35)

async def get_token_price(address, chain, bot):
    cache_key = f"{chain}:{address}"
    current_time = time.time()

    if (cache_key in price_cache and cache_key in symbol_cache and cache_key in market_cap_cache and
        current_time - price_cache[cache_key][1] < CACHE_DURATION and
        current_time - symbol_cache[cache_key][1] < CACHE_DURATION and
        current_time - market_cap_cache[cache_key][1] < CACHE_DURATION):
        price, _ = price_cache[cache_key]
        symbol, _ = symbol_cache[cache_key]
        market_cap, _ = market_cap_cache[cache_key]
        logger.debug(f"Using cached data for {cache_key}: price=${price}, symbol={symbol}, market_cap=${market_cap}")
        return price, symbol, market_cap

    if not re.match(r'^0x[a-fA-F0-9]{40}$|^[1-9A-HJ-NP-Za-km-z]{32,44}$|^0:[a-fA-F0-9]{64}$|^[A-Za-z0-9_-]{48}$', address):
        logger.warning(f"Invalid address format for {cache_key}")
        return None, None, None

    price = None
    symbol = None
    market_cap = None
    async with aiohttp.ClientSession() as session:
        try:
            if chain == 'ton':
                # Use CoinGecko for TON tokens
                platform = COINGECKO_PLATFORMS.get(chain, 'the-open-network')
                url = f"https://api.coingecko.com/api/v3/coins/{platform}/contract/{address}"
                logger.debug(f"Fetching CoinGecko data for {cache_key}: {url}")
                async with session.get(url, timeout=10) as response:
                    await asyncio.sleep(0.1)
                    if response.status != 200:
                        logger.error(f"CoinGecko API error for TON: Status {response.status}, URL: {url}")
                        failed_attempts[cache_key] = failed_attempts.get(cache_key, 0) + 1
                        return None, None, None
                    res = await response.json()
                    if 'error' in res:
                        logger.warning(f"CoinGecko error for {cache_key}: {res['error']}")
                        failed_attempts[cache_key] = failed_attempts.get(cache_key, 0) + 1
                        return None, None, None
                    price = float(res.get('market_data', {}).get('current_price', {}).get('usd', 0))
                    symbol = res.get('symbol', f"Unknown_{address[-6:]}").upper()
                    market_cap = float(res.get('market_data', {}).get('market_cap', {}).get('usd', 0))
                    failed_attempts[cache_key] = 0
                    logger.info(f"CoinGecko data for {cache_key}: price=${price}, symbol={symbol}, market_cap=${market_cap}")
            elif chain in DEXSCREENER_CHAINS:
                chain_id = DEXSCREENER_CHAINS[chain]
                url = f"{DEXSCREENER_TOKEN_API}{address}"
                logger.debug(f"Fetching DexScreener data for {cache_key}: {url}")
                async with session.get(url, timeout=10) as response:
                    await asyncio.sleep(0.1)
                    if response.status != 200:
                        logger.error(f"DexScreener API error: Status {response.status} for {url}")
                    else:
                        res = await response.json()
                        pairs = res.get('pairs', [])
                        if pairs:
                            target_pair = next(
                                (pair for pair in pairs if pair.get('chainId') == chain_id),
                                max(pairs, key=lambda p: p.get('liquidity', {}).get('usd', 0), default=None)
                            )
                            if target_pair:
                                price = float(target_pair.get('priceUsd', 0))
                                symbol = target_pair.get('baseToken', {}).get('symbol', f"Unknown_{address[-6:]}").upper()
                                market_cap = float(target_pair.get('marketCap', 0))
                                failed_attempts[cache_key] = 0
                                logger.info(f"DexScreener data for {cache_key}: price=${price}, symbol={symbol}, market_cap=${market_cap}")
            else:
                logger.warning(f"Unsupported chain for {cache_key}")
                return None, None, None
        except Exception as e:
            logger.error(f"Error fetching price for {cache_key}: {e}")
            failed_attempts[cache_key] = failed_attempts.get(cache_key, 0) + 1
            if failed_attempts[cache_key] >= 5:
                for admin_id in ADMIN_IDS:
                    await bot.send_message(
                        chat_id=admin_id,
                        text=f"Token {address} on {chain} failed 5 times. Skipping."
                    )
                    await asyncio.sleep(0.1)
            return None, None, None

        if chain in ALCHEMY_CHAINS and price is None:
            try:
                alchemy_chain = ALCHEMY_CHAINS[chain]
                alchemy_url = f"https://{_get_alchemy_api_key(alchemy_chain)}/v2/{alchemy_chain}"
                payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [
                        {
                            "to": address,
                            "data": "0x06fdde03"  # name() function
                        },
                        "latest"
                    ],
                    "id": 1
                }
                headers = {"Content-Type": "application/json"}
                async with session.post(alchemy_url, json=payload, headers=headers, timeout=10) as response:
                    await asyncio.sleep(0.1)
                    if response.status != 200:
                        logger.error(f"Alchemy API error: Status {response.status} for {alchemy_url}")
                    else:
                        res = await response.json()
                        if 'result' in res and res['result'] != '0x':
                            name_hex = res['result'][130:]  # Skip offset and length
                            symbol = bytearray.fromhex(name_hex).decode('utf-8', errors='ignore').strip()
                            if symbol:
                                logger.debug(f"Alchemy symbol for {cache_key}: {symbol}")
                if price is None:
                    logger.debug(f"Alchemy price fetch not implemented for {cache_key}")
            except Exception as e:
                logger.error(f"Alchemy error for {cache_key}: {e}")

        if price is None or symbol is None or market_cap is None:
            failed_attempts[cache_key] = failed_attempts.get(cache_key, 0) + 1
            logger.warning(f"Failed to retrieve valid data for {cache_key}: price={price}, symbol={symbol}, market_cap={market_cap}")
            return None, None, None

        price_cache[cache_key] = (price, current_time)
        symbol_cache[cache_key] = (symbol, current_time)
        market_cap_cache[cache_key] = (market_cap, current_time)
        logger.info(f"Cached data for {cache_key}: price=${price}, symbol={symbol}, market_cap=${market_cap}")
        return price, symbol, market_cap

def _get_alchemy_api_key(chain):
    api_keys = {
        'eth-mainnet': os.getenv('ALCHEMY_API_KEY_ETHEREUM'),
        'polygon-mainnet': os.getenv('ALCHEMY_API_KEY_POLYGON'),
        'base-mainnet': os.getenv('ALCHEMY_API_KEY_BASE')
    }
    return api_keys.get(chain, os.getenv('ALCHEMY_API_KEY_ETHEREUM', ''))

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    logger.debug(f"User {user_id} issued /cancel command")
    context.user_data.clear()
    keyboard = [[InlineKeyboardButton("Back to Menu", callback_data='back_to_menu')]]
    try:
        await update.message.reply_text(
            "Operation cancelled. Return to main menu?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except BadRequest as e:
        logger.error(f"Failed to send cancel message: {e}")
        await update.message.reply_text("Operation cancelled. Use /start to continue.")
    await asyncio.sleep(0.1)
    return ConversationHandler.END

def main():
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        logger.error("BOT_TOKEN environment variable not set")
        raise SystemExit("BOT_TOKEN not set")

    request = HTTPXRequest(
        connection_pool_size=20,
        read_timeout=10.0,
        write_timeout=10.0,
        connect_timeout=10.0,
        pool_timeout=30.0
    )
    app = ApplicationBuilder().token(bot_token).request(request).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CommandHandler('debug', debug_command),
            CommandHandler('cancel', cancel),
            CallbackQueryHandler(menu_handler, pattern='^(monitor|unmonitor|watchlist|top_monitored|leaderboard|broadcast|clear_watchlist|post_ad|list_ads|delete_ad|back_to_menu)$'),
            CallbackQueryHandler(list_ads, pattern='^list_ads:'),
            CallbackQueryHandler(delete_ad, pattern='^delete_ad:'),
            CallbackQueryHandler(confirm_delete_ad, pattern='^confirm_delete:'),
        ],
        states={
            ENTER_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_address)],
            SELECT_CHAIN: [CallbackQueryHandler(select_chain)],
            SELECT_MONITOR_TYPE: [CallbackQueryHandler(select_monitor_type)],
            ENTER_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_value)],
            BROADCAST_TYPE: [CallbackQueryHandler(broadcast_type)],
            BROADCAST_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, broadcast_message)],
            CONFIRM_UNMONITOR: [CallbackQueryHandler(confirm_unmonitor, pattern='^(unmonitor:|confirm_unmonitor:|back_to_menu$)')],
            CONFIRM_TOKEN: [CallbackQueryHandler(confirm_token, pattern='^confirm_token:')],
            ENTER_SYMBOL: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_symbol)],
            CLEAR_WATCHLIST: [MessageHandler(filters.TEXT & ~filters.COMMAND, clear_user_watchlist)],
            POST_AD_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, post_ad_message)],
            POST_AD_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, post_ad_duration)],
            POST_AD_VIEWS: [MessageHandler(filters.TEXT & ~filters.COMMAND, post_ad_views)],
            DELETE_AD: [CallbackQueryHandler(confirm_delete_ad, pattern='^delete_ad:')],
            CONFIRM_DELETE_AD: [CallbackQueryHandler(confirm_delete_ad, pattern='^confirm_delete:|^back_to_menu$')],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CallbackQueryHandler(back_to_menu, pattern='^back_to_menu$'),
            CallbackQueryHandler(readd_token, pattern='^readd_token:'),
        ],
        per_user=True,
        per_chat=True
    )

    app.add_handler(conv_handler)
    app.add_error_handler(error_handler)

    async def start_monitoring():
        try:
            await monitor_tokens(app.bot)
        except Exception as e:
            logger.error(f"Error in monitor_tokens: {e}")
            for admin_id in ADMIN_IDS:
                try:
                    await app.bot.send_message(chat_id=admin_id, text=f"Monitor tokens error: {e}")
                except Exception as e2:
                    logger.error(f"Failed to notify admin {admin_id}: {e2}")

    app.job_queue.run_repeating(lambda context: asyncio.create_task(start_monitoring()), interval=35, first=10)

    app.run_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Bot started polling")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        db.close()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        db.close()
        raise
