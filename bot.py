import os
import io
import asyncio
from aiohttp import web
import psycopg
from psycopg_pool import AsyncConnectionPool
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode

# ─────────────────────────────────────────────────────────────
# CONFIG / ENV VARS
# ─────────────────────────────────────────────────────────────
BOT_TOKEN  = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", "10000"))

WELCOME_MESSAGE_ID      = os.getenv("WELCOME_MESSAGE_ID")
WELCOME_MESSAGE_CHAT_ID = os.getenv("WELCOME_MESSAGE_CHAT_ID")

auth_users_str = os.getenv("AUTHORIZED_USERS", "").strip()
if auth_users_str:
    try:
        AUTHORIZED_USERS = [int(uid.strip()) for uid in auth_users_str.split(",") if uid.strip()]
    except ValueError:
        print(f"⚠️ Warning: Invalid AUTHORIZED_USERS format: {auth_users_str}")
        AUTHORIZED_USERS = []
else:
    AUTHORIZED_USERS = []

print(f"🔐 Authorization: {AUTHORIZED_USERS if AUTHORIZED_USERS else 'Open to all users'}", flush=True)

ALL_QUALITIES = ["480p", "720p", "1080p", "4K", "2160p"]

# ─────────────────────────────────────────────────────────────
# IN-MEMORY STATE
# ─────────────────────────────────────────────────────────────
progress = {
    "target_chat_id": None,
    "season": 1,
    "episode": 1,
    "total_episode": 1,
    "video_count": 0,
    "selected_qualities": ["480p", "720p", "1080p"],
    "base_caption": (
        "<b>◈ Anime Name</b>\n\n"
        "<b>- Season:</b> {season}\n"
        "<b>- Episode:</b> {episode}\n"
        "<b>- Audio track:</b> Hindi | Official\n"
        "<b>- Quality:</b> {quality}\n"
        "<blockquote>\n"
        "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
        "📢 <b>POWERED BY:</b> @beeetanime\n"
        "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
        "📺 <b>MAIN Channel:</b> @Beat_Hindi_Dubbed\n"
        "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
        "📢 <b>Group :</b> @Beat_Anime_Discussion\n"
        "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
        "</blockquote>"
    ),
    "auto_caption_enabled": True,
    "thumbnail_file_id": None,   # ← NEW: global thumbnail for videos
}

waiting_for_input  = {}   # user_id → input type string
last_bot_messages  = {}   # chat_id → message_id
upload_lock = asyncio.Lock()
db_pool = None


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────
async def init_db():
    global db_pool
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is required")

    db_pool = AsyncConnectionPool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        open=False,
        kwargs={"autocommit": True, "prepare_threshold": None},
    )
    await db_pool.open()
    print("Database connection pool opened", flush=True)

    async with db_pool.connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS bot_progress")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_progress (
                id                  INTEGER PRIMARY KEY,
                target_chat_id      BIGINT,
                season              INTEGER  DEFAULT 1,
                episode             INTEGER  DEFAULT 1,
                total_episode       INTEGER  DEFAULT 1,
                video_count         INTEGER  DEFAULT 0,
                selected_qualities  TEXT     DEFAULT '480p,720p,1080p',
                base_caption        TEXT,
                auto_caption_enabled BOOLEAN DEFAULT TRUE,
                thumbnail_file_id   TEXT     DEFAULT NULL,
                CONSTRAINT single_row CHECK (id = 1)
            )
        """)
        await conn.execute("""
            INSERT INTO bot_progress (id, base_caption, auto_caption_enabled)
            VALUES (1, %s, TRUE)
            ON CONFLICT (id) DO UPDATE SET base_caption = EXCLUDED.base_caption
        """, (progress["base_caption"],))

    print("Database ready ✅", flush=True)
    await load_progress()


async def load_progress():
    global progress
    for attempt in range(3):
        try:
            async with db_pool.connection() as conn:
                row = await conn.execute("""
                    SELECT target_chat_id, season, episode, total_episode,
                           video_count, selected_qualities, base_caption,
                           auto_caption_enabled, thumbnail_file_id
                    FROM bot_progress WHERE id = 1
                """)
                data = await row.fetchone()
                if data:
                    progress["target_chat_id"]       = data[0]
                    progress["season"]               = data[1]
                    progress["episode"]              = data[2]
                    progress["total_episode"]        = data[3]
                    progress["video_count"]          = data[4]
                    progress["selected_qualities"]   = data[5].split(",") if data[5] else []
                    progress["base_caption"]         = data[6] or progress["base_caption"]
                    progress["auto_caption_enabled"] = data[7]
                    progress["thumbnail_file_id"]    = data[8]
                    print(f"Progress loaded – S{progress['season']}E{progress['episode']} "
                          f"| Auto-Caption: {progress['auto_caption_enabled']} "
                          f"| Thumb: {'✅' if progress['thumbnail_file_id'] else '❌'}", flush=True)
            return
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                raise


async def save_progress():
    for attempt in range(3):
        try:
            async with db_pool.connection() as conn:
                await conn.execute("""
                    UPDATE bot_progress SET
                        target_chat_id       = %s,
                        season               = %s,
                        episode              = %s,
                        total_episode        = %s,
                        video_count          = %s,
                        selected_qualities   = %s,
                        base_caption         = %s,
                        auto_caption_enabled = %s,
                        thumbnail_file_id    = %s
                    WHERE id = 1
                """, (
                    progress["target_chat_id"],
                    progress["season"],
                    progress["episode"],
                    progress["total_episode"],
                    progress["video_count"],
                    ",".join(progress["selected_qualities"]),
                    progress["base_caption"],
                    progress["auto_caption_enabled"],
                    progress["thumbnail_file_id"],
                ))
            return
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                raise


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def is_authorized(user_id: int) -> bool:
    if not AUTHORIZED_USERS:
        return True
    return user_id in AUTHORIZED_USERS


async def delete_last_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    if chat_id in last_bot_messages:
        try:
            loading = await context.bot.send_message(chat_id, "⏳ <i>Please wait...</i>", parse_mode=ParseMode.HTML)
            await asyncio.sleep(1.0)
            await context.bot.delete_message(chat_id, last_bot_messages[chat_id])
            await context.bot.delete_message(chat_id, loading.message_id)
        except Exception:
            pass
        del last_bot_messages[chat_id]


def build_caption(quality: str) -> str:
    return (
        progress["base_caption"]
        .replace("{season}",       f"{progress['season']:02}")
        .replace("{episode}",      f"{progress['episode']:02}")
        .replace("{total_episode}",f"{progress['total_episode']:02}")
        .replace("{quality}",      quality)
    )


def current_quality() -> str:
    if not progress["selected_qualities"]:
        return "N/A"
    return progress["selected_qualities"][progress["video_count"] % len(progress["selected_qualities"])]


async def advance_counters():
    progress["video_count"] += 1
    if progress["video_count"] >= len(progress["selected_qualities"]):
        progress["episode"]       += 1
        progress["total_episode"] += 1
        progress["video_count"]   = 0
    await save_progress()


async def get_cover_input(context: ContextTypes.DEFAULT_TYPE):
    """
    Download the stored thumbnail photo and return it as a fresh InputFile.

    WHY:  Telegram Bot API explicitly states "Thumbnails can't be reused and
          can be only uploaded as a new file."  Passing a file_id to
          thumbnail= or cover= is silently ignored — the cover never appears.
          We MUST download the raw bytes and re-upload them fresh every time.
    """
    file_id = progress.get("thumbnail_file_id")
    if not file_id:
        return None
    try:
        tg_file = await context.bot.get_file(file_id)
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        buf.seek(0)
        return InputFile(buf, filename="cover.jpg")
    except Exception as e:
        print(f"\u26a0\ufe0f Could not prepare cover image: {e}", flush=True)
        return None



# ─────────────────────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────────────────────
def get_menu_markup() -> InlineKeyboardMarkup:
    ac_status   = "✅ ON"  if progress.get("auto_caption_enabled", True) else "❌ OFF"
    thumb_status = "✅ Set" if progress.get("thumbnail_file_id")          else "❌ Not Set"

    keyboard = [
        [InlineKeyboardButton("Preview Caption", callback_data="preview")],
        [InlineKeyboardButton("Set Caption",     callback_data="set_caption")],
        [
            InlineKeyboardButton("Set Season",   callback_data="set_season"),
            InlineKeyboardButton("Set Episode",  callback_data="set_episode"),
        ],
        [InlineKeyboardButton("Set Total Episode",   callback_data="set_total_episode")],
        [InlineKeyboardButton("Quality Settings",    callback_data="quality_menu")],
        [InlineKeyboardButton("Set Target Channel",  callback_data="set_target_channel")],
        [InlineKeyboardButton(f"Auto-Caption: {ac_status}", callback_data="toggle_auto_caption")],
        # ── Thumbnail row ──────────────────────────────────────
        [
            InlineKeyboardButton(f"🖼️ Thumbnail: {thumb_status}", callback_data="thumb_menu"),
        ],
        # ──────────────────────────────────────────────────────
        [InlineKeyboardButton("Reset Episode",   callback_data="reset")],
        [InlineKeyboardButton("🗑️ Clear Database", callback_data="clear_db")],
        [InlineKeyboardButton("Cancel",          callback_data="cancel")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_quality_markup() -> InlineKeyboardMarkup:
    keyboard = []
    for quality in ALL_QUALITIES:
        checkmark = "✅ " if quality in progress["selected_qualities"] else ""
        keyboard.append([InlineKeyboardButton(f"{checkmark}{quality}", callback_data=f"toggle_quality_{quality}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="back_to_main")])
    return InlineKeyboardMarkup(keyboard)


def get_thumb_menu_markup() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("📸 Set / Update Thumbnail", callback_data="set_thumbnail")],
    ]
    if progress.get("thumbnail_file_id"):
        buttons.append([InlineKeyboardButton("👁️ View Thumbnail",   callback_data="view_thumbnail")])
        buttons.append([InlineKeyboardButton("🗑️ Remove Thumbnail", callback_data="remove_thumbnail")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="back_to_main")])
    return InlineKeyboardMarkup(buttons)


def settings_text() -> str:
    target_status = f"✅ Set: {progress['target_chat_id']}" if progress["target_chat_id"] else "❌ Not Set"
    ac_status     = "✅ ON" if progress.get("auto_caption_enabled", True) else "❌ OFF"
    thumb_status  = "✅ Set" if progress.get("thumbnail_file_id") else "❌ Not Set"
    return (
        f"<b>Target Channel:</b> {target_status}\n"
        f"<b>Auto-Caption:</b> {ac_status}\n"
        f"<b>Thumbnail:</b> {thumb_status}\n\n"
        "Use the buttons below to manage captions, thumbnails and episodes."
    )


# ─────────────────────────────────────────────────────────────
# HANDLERS – /start
# ─────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if not is_authorized(user_id):
        await update.message.reply_text("❌ You are not authorized to use this bot.")
        return

    try:
        await update.message.delete()
    except Exception:
        pass

    await delete_last_message(context, chat_id)

    prefix = "👋 <b>Welcome to the Anime Caption + Thumbnail Bot!</b>\n\n"

    if WELCOME_MESSAGE_ID and WELCOME_MESSAGE_CHAT_ID:
        try:
            await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=WELCOME_MESSAGE_CHAT_ID,
                message_id=int(WELCOME_MESSAGE_ID),
            )
            prefix = "👆 <b>Here is the main control menu:</b>\n\n"
        except Exception as e:
            print(f"⚠️ Could not copy welcome message: {e}", flush=True)
            prefix = "⚠️ <b>Warning:</b> Failed to copy custom welcome message.\n\n"

    sent = await context.bot.send_message(
        chat_id,
        prefix + settings_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=get_menu_markup(),
    )
    last_bot_messages[chat_id] = sent.message_id


# ─────────────────────────────────────────────────────────────
# HANDLERS – inline buttons
# ─────────────────────────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id

    if not is_authorized(user_id):
        await query.answer("❌ You are not authorized.", show_alert=True)
        return

    await query.answer()
    chat_id = query.message.chat_id
    data    = query.data

    await delete_last_message(context, chat_id)

    # ── toggle auto-caption ────────────────────────────────────
    if data == "toggle_auto_caption":
        progress["auto_caption_enabled"] = not progress.get("auto_caption_enabled", True)
        await save_progress()
        status = "enabled" if progress["auto_caption_enabled"] else "disabled"
        sent = await query.message.reply_text(
            f"✅ Auto-Caption has been <b>{status}</b>.\n\n" + settings_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    # ── thumbnail menu ─────────────────────────────────────────
    if data == "thumb_menu":
        thumb_status = "✅ Thumbnail is set" if progress.get("thumbnail_file_id") else "❌ No thumbnail set"
        sent = await query.message.reply_text(
            f"🖼️ <b>Thumbnail Settings</b>\n\n{thumb_status}\n\n"
            "The thumbnail is applied to every video sent to the target channel.\n"
            "For channel auto-mode, the bot deletes the original post and re-sends it with the thumbnail + caption.",
            parse_mode=ParseMode.HTML,
            reply_markup=get_thumb_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    if data == "set_thumbnail":
        waiting_for_input[user_id] = "thumbnail"
        sent = await query.message.reply_text(
            "📸 <b>Send me a photo</b> to use as the thumbnail for all videos.\n\n"
            "<i>This image will appear as the cover of every video posted to the target channel.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    if data == "view_thumbnail":
        if progress.get("thumbnail_file_id"):
            sent = await context.bot.send_photo(
                chat_id=chat_id,
                photo=progress["thumbnail_file_id"],
                caption="🖼️ <b>Current Thumbnail</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_thumb_menu_markup(),
            )
        else:
            sent = await query.message.reply_text(
                "❌ No thumbnail set. Use 'Set / Update Thumbnail' to add one.",
                parse_mode=ParseMode.HTML,
                reply_markup=get_thumb_menu_markup(),
            )
        last_bot_messages[chat_id] = sent.message_id
        return

    if data == "remove_thumbnail":
        progress["thumbnail_file_id"] = None
        await save_progress()
        sent = await query.message.reply_text(
            "🗑️ Thumbnail removed. Videos will be posted without a custom thumbnail.",
            parse_mode=ParseMode.HTML,
            reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    # ── preview ────────────────────────────────────────────────
    if data == "preview":
        quality = current_quality()
        preview = build_caption(quality)
        target_status = f"✅ {progress['target_chat_id']}" if progress["target_chat_id"] else "❌ Not Set"
        ac_status     = "✅ ON" if progress.get("auto_caption_enabled", True) else "❌ OFF"
        thumb_status  = "✅ Set" if progress.get("thumbnail_file_id") else "❌ Not Set"
        sent = await query.message.reply_text(
            f"📝 <b>Preview Caption:</b>\n\n{preview}\n\n"
            f"<b>Current Settings:</b>\n"
            f"Target Channel: {target_status}\n"
            f"Auto-Caption: {ac_status}\n"
            f"Thumbnail: {thumb_status}\n"
            f"Season: {progress['season']} | Episode: {progress['episode']} | Total: {progress['total_episode']}\n"
            f"Qualities: {', '.join(progress['selected_qualities']) or 'None'}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_caption":
        waiting_for_input[user_id] = "caption"
        sent = await query.message.reply_text(
            "✏️ Please send the new base caption now (HTML supported).\n"
            "Placeholders: <code>{season}</code>, <code>{episode}</code>, "
            "<code>{total_episode}</code>, <code>{quality}</code>",
            parse_mode=ParseMode.HTML,
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_season":
        waiting_for_input[user_id] = "season"
        sent = await query.message.reply_text(
            f"✏️ Current season: <b>{progress['season']}</b>\n\nSend the new season number.",
            parse_mode=ParseMode.HTML,
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_episode":
        waiting_for_input[user_id] = "episode"
        sent = await query.message.reply_text(
            f"✏️ Current episode: <b>{progress['episode']}</b>\n\nSend the new episode number.",
            parse_mode=ParseMode.HTML,
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_total_episode":
        waiting_for_input[user_id] = "total_episode"
        sent = await query.message.reply_text(
            f"✏️ Current total episode: <b>{progress['total_episode']}</b>\n\nSend the new value.",
            parse_mode=ParseMode.HTML,
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_target_channel":
        waiting_for_input[user_id] = "set_channel"
        sent = await query.message.reply_text(
            "📢 <b>Set Target Channel</b>\n\n"
            "<b>Option 1:</b> Forward any message from the target channel\n"
            "<b>Option 2:</b> Send the channel ID (e.g., <code>-1001234567890</code>)\n"
            "<b>Option 3:</b> Send the channel username (e.g., <code>@yourchannel</code>)\n\n"
            "Make sure the bot is an admin in that channel!",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "quality_menu":
        sent = await query.message.reply_text(
            "🎬 <b>Quality Settings</b>\n\nToggle qualities on/off.\n\n"
            f"<b>Selected:</b> {', '.join(progress['selected_qualities']) or 'None'}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_quality_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data.startswith("toggle_quality_"):
        quality = data.replace("toggle_quality_", "")
        if quality in progress["selected_qualities"]:
            progress["selected_qualities"].remove(quality)
        else:
            progress["selected_qualities"].append(quality)
        progress["selected_qualities"] = [q for q in ALL_QUALITIES if q in progress["selected_qualities"]]
        await save_progress()
        try:
            await query.edit_message_text(
                "🎬 <b>Quality Settings</b>\n\nToggle qualities on/off.\n\n"
                f"<b>Selected:</b> {', '.join(progress['selected_qualities']) or 'None'}",
                parse_mode=ParseMode.HTML,
                reply_markup=get_quality_markup(),
            )
        except Exception:
            sent = await query.message.reply_text(
                "🎬 <b>Quality Settings</b>\n\nToggle qualities on/off.\n\n"
                f"<b>Selected:</b> {', '.join(progress['selected_qualities']) or 'None'}",
                parse_mode=ParseMode.HTML,
                reply_markup=get_quality_markup(),
            )
            last_bot_messages[chat_id] = sent.message_id

    elif data == "back_to_main":
        try:
            await query.message.delete()
        except Exception:
            pass
        sent = await context.bot.send_message(
            chat_id,
            "👋 <b>Main Menu</b>\n\n" + settings_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "reset":
        progress["episode"]     = 1
        progress["video_count"] = 0
        await save_progress()
        sent = await query.message.reply_text(
            f"🔄 Episode counter reset → Episode {progress['episode']} (Season {progress['season']}).",
            parse_mode=ParseMode.HTML,
            reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "clear_db":
        try:
            async with db_pool.connection() as conn:
                r1 = await conn.execute("SELECT COUNT(*) FROM bot_progress")
                row_count = (await r1.fetchone())[0]
                r2 = await conn.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                db_size = (await r2.fetchone())[0]
        except Exception:
            row_count = db_size = "Unknown"

        sent = await query.message.reply_text(
            f"⚠️ <b>Clear Database</b>\n\n"
            f"• DB size: {db_size}  • Rows: {row_count}\n\n"
            "This will reset all counters and remove the target channel.\n"
            "Caption, qualities and thumbnail are preserved.\n\n"
            "<b>Are you sure?</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Clear & Optimize", callback_data="confirm_clear_db")],
                [InlineKeyboardButton("❌ No, Go Back",          callback_data="back_to_main")],
            ]),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "confirm_clear_db":
        async with db_pool.connection() as conn:
            await conn.execute("DELETE FROM bot_progress")
            await conn.execute("""
                INSERT INTO bot_progress (id, target_chat_id, season, episode, total_episode,
                                         video_count, selected_qualities, base_caption,
                                         auto_caption_enabled, thumbnail_file_id)
                VALUES (1, NULL, 1, 1, 1, 0, %s, %s, %s, %s)
            """, (
                ",".join(progress["selected_qualities"]),
                progress["base_caption"],
                progress["auto_caption_enabled"],
                progress["thumbnail_file_id"],
            ))
            await conn.execute("VACUUM FULL bot_progress")
        await load_progress()
        sent = await query.message.reply_text(
            "✅ <b>Database Cleared & Optimized!</b>\n\n"
            "• All counters reset  • Space reclaimed\n"
            "• Caption, qualities and thumbnail preserved",
            parse_mode=ParseMode.HTML,
            reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "cancel":
        waiting_for_input.pop(user_id, None)
        sent = await query.message.reply_text("❌ Cancelled.", reply_markup=get_menu_markup())
        last_bot_messages[chat_id] = sent.message_id


# ─────────────────────────────────────────────────────────────
# HANDLERS – text / photo messages (menu input)
# ─────────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        return
    if user_id not in waiting_for_input:
        return

    chat_id    = update.effective_chat.id
    input_type = waiting_for_input[user_id]

    try:
        await update.message.delete()
    except Exception:
        pass
    await delete_last_message(context, chat_id)

    # ── photo → thumbnail ──────────────────────────────────────
    if input_type == "thumbnail":
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            progress["thumbnail_file_id"] = file_id
            await save_progress()
            del waiting_for_input[user_id]
            sent = await context.bot.send_message(
                chat_id,
                "✅ <b>Thumbnail saved!</b> It will now be applied to every video sent to the channel.",
                parse_mode=ParseMode.HTML,
                reply_markup=get_menu_markup(),
            )
            last_bot_messages[chat_id] = sent.message_id
        else:
            sent = await context.bot.send_message(
                chat_id,
                "❌ Please send a <b>photo</b> (not a file or video) to set as thumbnail.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]),
            )
            last_bot_messages[chat_id] = sent.message_id
        return

    # ── forwarded message → channel detection ──────────────────
    if input_type == "set_channel" and update.message.forward_origin:
        if hasattr(update.message.forward_origin, "chat"):
            channel    = update.message.forward_origin.chat
            channel_id = channel.id
            try:
                info = await context.bot.get_chat(channel_id)
                progress["target_chat_id"] = channel_id
                await save_progress()
                del waiting_for_input[user_id]
                sent = await context.bot.send_message(
                    chat_id,
                    f"✅ <b>Target channel set!</b>\n\n"
                    f"Channel: {channel.title}\nID: <code>{channel_id}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_main")]]),
                )
                last_bot_messages[chat_id] = sent.message_id
            except Exception as e:
                await context.bot.send_message(
                    chat_id,
                    f"❌ Cannot access channel!\n\nError: {e}\n\nMake sure the bot is admin.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try Again", callback_data="set_target_channel")]]),
                )
        else:
            await context.bot.send_message(
                chat_id,
                "❌ Forward a message from a channel, not from a user.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try Again", callback_data="set_target_channel")]]),
            )
        return

    # ── text input for channel ID / username ───────────────────
    if input_type == "set_channel" and update.message.text:
        ch = update.message.text.strip()
        try:
            if ch.startswith("@") or ch.lstrip("-").isdigit():
                ch_id = int(ch) if ch.lstrip("-").isdigit() else ch
                info  = await context.bot.get_chat(ch_id)
                progress["target_chat_id"] = info.id
                await save_progress()
                del waiting_for_input[user_id]
                sent = await context.bot.send_message(
                    chat_id,
                    f"✅ <b>Target channel set!</b>\n\n"
                    f"Channel: {info.title}\nID: <code>{info.id}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_main")]]),
                )
                last_bot_messages[chat_id] = sent.message_id
            else:
                await context.bot.send_message(
                    chat_id,
                    "❌ Invalid format. Send a channel ID or @username.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]),
                )
        except Exception as e:
            await context.bot.send_message(
                chat_id,
                f"❌ Cannot access channel!\nError: {e}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Try Again", callback_data="set_target_channel")],
                    [InlineKeyboardButton("❌ Cancel",    callback_data="cancel")],
                ]),
            )
        return

    # ── numeric / text inputs ──────────────────────────────────
    if not update.message.text:
        return

    text = update.message.text

    if input_type == "caption":
        progress["base_caption"] = text
        await save_progress()
        del waiting_for_input[user_id]
        sent = await context.bot.send_message(chat_id, "✅ Caption updated!", reply_markup=get_menu_markup())
        last_bot_messages[chat_id] = sent.message_id

    elif input_type in ("season", "episode", "total_episode"):
        if text.isdigit():
            progress[input_type] = int(text)
            await save_progress()
            del waiting_for_input[user_id]
            label = input_type.replace("_", " ").title()
            sent = await context.bot.send_message(
                chat_id, f"✅ {label} updated to {text}!",
                parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
            )
            last_bot_messages[chat_id] = sent.message_id
        else:
            sent = await context.bot.send_message(
                chat_id, "❌ Please enter a valid number.", parse_mode=ParseMode.HTML,
            )
            last_bot_messages[chat_id] = sent.message_id


# ─────────────────────────────────────────────────────────────
# HANDLERS – video sent directly to bot (private chat)
# ─────────────────────────────────────────────────────────────
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        await update.message.reply_text("❌ Authentication Error: no user info available.")
        return

    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("❌ You are not authorized to use this bot.")
        return

    async with upload_lock:
        if not progress["target_chat_id"]:
            await update.message.reply_text("❌ Target channel not set! Use 'Set Target Channel' first.")
            return
        if not progress["selected_qualities"]:
            await update.message.reply_text("❌ No qualities selected! Use Quality Settings menu.")
            return

        file_id = update.message.video.file_id
        quality = current_quality()
        caption = build_caption(quality)
        print(f"📤 Sending video to {progress['target_chat_id']} | quality={quality} | thumb={'yes' if thumb else 'no'}", flush=True)

        try:
            await context.bot.get_chat(progress["target_chat_id"])
        except Exception as e:
            await update.message.reply_text(
                f"❌ Cannot access target channel!\nError: {e}\n\n"
                "Make sure the bot is admin with 'Post Messages' permission."
            )
            return

        try:
            # Download cover bytes fresh — Telegram rejects reused file_ids for covers
            cover_input = await get_cover_input(context)

            send_kwargs = dict(
                chat_id=progress["target_chat_id"],
                video=file_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
            if cover_input:
                # thumbnail= sets the file-list icon; cover= sets the video preview image
                # Pass both so it works on all client versions
                send_kwargs["thumbnail"] = cover_input
                send_kwargs["api_kwargs"] = {"cover": cover_input}

            sent_msg = await context.bot.send_video(**send_kwargs)
            print(f"✅ Video sent. Message ID: {sent_msg.message_id}", flush=True)

            await update.message.reply_text(
                f"✅ <b>Video posted with caption + {'cover' if cover_input else 'no thumbnail'}!</b>\n\n"
                f"{caption}\n\n"
                f"Progress: {progress['video_count'] + 1}/{len(progress['selected_qualities'])} for this episode",
                parse_mode=ParseMode.HTML,
            )
            await advance_counters()

        except Exception as e:
            err = str(e).lower()
            if "not enough rights" in err or "admin" in err:
                await update.message.reply_text("❌ Bot is not admin in the target channel!")
            elif "chat not found" in err:
                await update.message.reply_text("❌ Target channel not found! Please reset it.")
            else:
                await update.message.reply_text(f"❌ Error: {e}")
            import traceback; traceback.print_exc()


# ─────────────────────────────────────────────────────────────
# HANDLERS – channel post (auto-caption + auto-thumbnail)
# ─────────────────────────────────────────────────────────────
async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    When a video is posted directly in the target channel:
      1. Check auto-caption is enabled and this is the right channel.
      2. If a thumbnail is set → delete original post and re-send with caption + thumbnail.
      3. If no thumbnail → just edit the caption in place (fast, no re-post needed).
    """
    channel_post = update.channel_post
    if not channel_post or not channel_post.video:
        return

    chat_id = channel_post.chat.id

    if chat_id != progress.get("target_chat_id"):
        return
    if not progress.get("auto_caption_enabled", False):
        return

    async with upload_lock:
        await load_progress()   # ensure latest state

        if not progress["selected_qualities"]:
            print("❌ No qualities selected, skipping auto-caption", flush=True)
            return

        quality = current_quality()
        caption = build_caption(quality)
        msg_id  = channel_post.message_id

        # Download cover bytes fresh — Telegram rejects reused file_ids for covers
        cover_input = await get_cover_input(context)

        print(f"🤖 Channel auto-post: msg_id={msg_id} | quality={quality} | cover={'yes' if cover_input else 'no'}", flush=True)

        try:
            if cover_input:
                # ── Re-post with cover ────────────────────────────────
                # Must delete + resend: Telegram has no API to change video cover in-place
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    print(f"🗑️ Deleted original post {msg_id}", flush=True)
                except Exception as del_err:
                    print(f"⚠️ Could not delete original post: {del_err}", flush=True)

                await context.bot.send_video(
                    chat_id=chat_id,
                    video=channel_post.video.file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    thumbnail=cover_input,
                    api_kwargs={"cover": cover_input},
                )
                print("✅ Re-posted with cover + caption", flush=True)

            else:
                # ── Just edit the caption (no cover to set) ───────────
                await context.bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=msg_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                )
                print("✅ Caption edited (no cover)", flush=True)

            await advance_counters()

        except Exception as e:
            print(f"❌ Error processing channel post {msg_id}: {e}", flush=True)
            import traceback; traceback.print_exc()


# ─────────────────────────────────────────────────────────────
# WEB SERVER & SELF-PING
# ─────────────────────────────────────────────────────────────
async def health_check(request):
    return web.Response(text="Bot is running! ✅")


async def self_ping():
    import aiohttp
    render_url = os.getenv("RENDER_EXTERNAL_URL", "https://beat-caption-bot.onrender.com")
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{render_url}/health") as r:
                    print(f"🏓 Self-ping {'ok' if r.status == 200 else f'status {r.status}'}", flush=True)
        except Exception as e:
            print(f"❌ Self-ping error: {e}", flush=True)


async def start_web_server():
    app_web = web.Application()
    app_web.router.add_get("/",       health_check)
    app_web.router.add_get("/health", health_check)
    runner = web.AppRunner(app_web)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    print(f"✅ Web server started on port {PORT}", flush=True)
    asyncio.create_task(self_ping())


async def post_init(application: Application):
    await init_db()
    await start_web_server()


async def post_shutdown(application: Application):
    global db_pool
    print("🛑 Shutting down…", flush=True)
    if db_pool:
        try:
            await db_pool.close()
            print("✅ DB pool closed", flush=True)
        except Exception as e:
            print(f"⚠️ Error closing DB pool: {e}", flush=True)


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"❌ Error: {context.error}", flush=True)
    import traceback; traceback.print_exc()
    if update and update.effective_chat and update.effective_chat.type == "private":
        try:
            await context.bot.send_message(
                update.effective_chat.id,
                f"❌ An error occurred. Please try again.\n\nError: {str(context.error)[:100]}",
            )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 55, flush=True)
    print("  BEAT ANIME CAPTION + THUMBNAIL BOT  ", flush=True)
    print("=" * 55, flush=True)

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

    # Videos sent directly to the bot (private chat)
    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.VIDEO & ~filters.COMMAND, handle_video)
    )

    # Text + photo messages for menu input (private chat)
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & (filters.TEXT | filters.PHOTO | filters.FORWARDED) & ~filters.VIDEO & ~filters.COMMAND,
            handle_message,
        )
    )

    # Channel posts – auto-caption + auto-thumbnail
    application.add_handler(
        MessageHandler(filters.ChatType.CHANNEL & filters.VIDEO, handle_channel_post)
    )

    application.add_error_handler(error_handler)

    print(f"Authorized users: {AUTHORIZED_USERS or 'All (no restriction)'}", flush=True)
    print("=" * 55, flush=True)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    import signal, sys

    def sig_handler(sig, frame):
        print("\n🛑 Shutdown signal received…", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGINT,  sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user", flush=True)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}", flush=True)
        import traceback; traceback.print_exc()
    finally:
        print("👋 Goodbye!", flush=True)
