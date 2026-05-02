import os
import io
import re
import asyncio
import html as _html
from aiohttp import web
import psycopg
from psycopg_pool import AsyncConnectionPool
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode

# ══════════════════════════════════════════════════════════════
#  CONFIG / ENV VARS
# ══════════════════════════════════════════════════════════════
BOT_TOKEN               = os.getenv("BOT_TOKEN")
DATABASE_URL            = os.getenv("DATABASE_URL")
PORT                    = int(os.getenv("PORT", "10000"))
WELCOME_MESSAGE_ID      = os.getenv("WELCOME_MESSAGE_ID")
WELCOME_MESSAGE_CHAT_ID = os.getenv("WELCOME_MESSAGE_CHAT_ID")

auth_users_str = os.getenv("AUTHORIZED_USERS", "").strip()
if auth_users_str:
    try:
        AUTHORIZED_USERS = [int(u.strip()) for u in auth_users_str.split(",") if u.strip()]
    except ValueError:
        print(f"Warning: Invalid AUTHORIZED_USERS: {auth_users_str}")
        AUTHORIZED_USERS = []
else:
    AUTHORIZED_USERS = []

print(f"Auth: {AUTHORIZED_USERS or 'Open to all'}", flush=True)

ALL_QUALITIES = ["480p", "720p", "1080p", "4K", "2160p"]

# ══════════════════════════════════════════════════════════════
#  EPISODE RECOGNITION
#
#  Leech bots (raretoons / toon4all) put episode info in
#  video.file_name, NOT in the message caption.
#  We check BOTH fields.
#
#  Patterns:  S01E25 · Season 1 Episode 25 · 1x25
#  Quality is intentionally ignored — rotation always wins.
# ══════════════════════════════════════════════════════════════
_SE_PATTERNS = [
    re.compile(r'S(\d{1,2})E(\d{1,3})', re.IGNORECASE),
    re.compile(r'Season\s*(\d{1,2})\s*Episode\s*(\d{1,3})', re.IGNORECASE),
    re.compile(r'(?<!\d)(\d{1,2})x(\d{1,3})(?!\d)', re.IGNORECASE),
]


def parse_episode_info(caption: str, filename: str):
    """Check caption then filename. Returns (season, episode) or (None, None)."""
    for text in (caption or "", filename or ""):
        if not text:
            continue
        for pat in _SE_PATTERNS:
            m = pat.search(text)
            if m:
                s, ep = int(m.group(1)), int(m.group(2))
                print(f"Recognised S{s:02}E{ep:02} from: '{text[:70]}'", flush=True)
                return s, ep
    print(f"No episode info — cap='{(caption or '')[:40]}' file='{(filename or '')[:40]}'", flush=True)
    return None, None


# ══════════════════════════════════════════════════════════════
#  IN-MEMORY STATE
# ══════════════════════════════════════════════════════════════
progress = {
    "target_chat_id":      None,
    "season":              1,
    "episode":             1,
    "total_episode":       1,
    "video_count":         0,
    "selected_qualities":  ["480p", "720p", "1080p"],
    "base_caption": (
        "<b>╔══════════════════════╗</b>\n"
        "<blockquote><b>            ║✦ Spy X Family ✦║</b></blockquote>\n"
        "<b>╚══════════════════════╝</b>\n"
        "┌─➤<b>▰▰▰▰▰▰▰▰▰▰▰▰▰</b>\n"
        "<blockquote><b>➤ sᴇᴀsᴏɴ : {season} </b> </blockquote>\n"
        "<blockquote><b>➤ ᴇᴘɪsᴏᴅᴇ : {episode}  </b></blockquote>\n"
        "<blockquote><b>➤ ᴀᴜᴅɪᴏ : [ʜɪɴ] ᴅᴜʙ| <span class='tg-spoiler'>#ᴏғғɪᴄɪᴀʟ ᴅᴜʙ</span> </b></blockquote>\n"
        "<blockquote><b>➤ ǫᴜᴀʟɪᴛʏ {quality} </b></blockquote>\n"
        "└─➤<b>▰▰▰▰▰▰▰▰▰▰▰▰▰</b>\n"
        "<blockquote expandable><b>▣ ᴍᴀɪɴ ᴄʜᴀɴɴᴇʟ : ▌<a href='https://t.me/Beat_Hindi_Dubbed'>ʙᴇᴀᴛ_ʜɪɴᴅɪ_ᴅᴜʙʙᴇᴅ</a>▌</b>\n"
        "<b>▣ ᴘᴏᴡᴇʀᴇᴅ ʙʏ : ▌<a href='https://t.me/BeeetAnime'>ʙᴇᴇᴇᴛᴀɴɪᴍᴇ</a>▌</b>\n"
        "<b>▣ ᴄᴏᴍᴍᴜɴɪᴛʏ : ▌<a href='https://t.me/Beat_Anime_Discussion'>ʙᴇᴀᴛ_ᴀɴɪᴍᴇ_ᴅɪsᴄᴜssɪᴏɴ</a>▌</b></blockquote>\n"
        "<b>▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰</b>"
    ),
    "auto_caption_enabled": True,
    "thumbnail_file_id":    None,
}

# ── Recognised-mode episode boundary tracking (in-memory only) ──
# Tracks the last episode we processed so we can detect when a new
# episode starts and reset the quality-rotation slot back to 0.
_last_recog_season:  int = None
_last_recog_episode: int = None

waiting_for_input  = {}
last_bot_messages  = {}
db_pool            = None

# ══════════════════════════════════════════════════════════════
#  SERIAL QUEUE
#  One asyncio.Queue + one worker = strict one-at-a-time
#  processing, preventing duplicate episode numbers even when
#  Telegram delivers many channel updates simultaneously.
#
#  Queue cap: 200 jobs (leech bots rarely exceed this per batch).
#  If the cap is hit the new job is still added — cap is just for
#  logging a warning.
# ══════════════════════════════════════════════════════════════
_QUEUE_WARN_DEPTH   = 10
_channel_post_queue: asyncio.Queue = asyncio.Queue()
_queue_worker_task:  asyncio.Task  = None


async def _channel_post_worker():
    """Drain the queue sequentially — never two jobs at once."""
    while True:
        job = await _channel_post_queue.get()
        try:
            await _process_channel_post(*job)
        except Exception as e:
            print(f"Worker error: {e}", flush=True)
            import traceback; traceback.print_exc()
        finally:
            _channel_post_queue.task_done()


# ══════════════════════════════════════════════════════════════
#  SMALL CAPS
# ══════════════════════════════════════════════════════════════
_SC_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyz",
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᶻ"
)
def sc(t: str) -> str:
    return t.lower().translate(_SC_MAP)

# ══════════════════════════════════════════════════════════════
#  DATABASE
#
#  FIX: removed DROP TABLE IF EXISTS — that line wiped all saved
#  progress (season, episode, counters) on every single restart.
#  CREATE TABLE IF NOT EXISTS is idempotent and safe.
# ══════════════════════════════════════════════════════════════
async def init_db():
    global db_pool
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is required")
    db_pool = AsyncConnectionPool(
        DATABASE_URL, min_size=1, max_size=5, open=False,
        kwargs={"autocommit": True, "prepare_threshold": None},
    )
    await db_pool.open()
    print("DB pool opened", flush=True)
    async with db_pool.connection() as conn:
        # Safe: only creates the table if it does not already exist.
        # Never drops — all saved state is preserved across restarts.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_progress (
                id                   INTEGER PRIMARY KEY,
                target_chat_id       BIGINT,
                season               INTEGER  DEFAULT 1,
                episode              INTEGER  DEFAULT 1,
                total_episode        INTEGER  DEFAULT 1,
                video_count          INTEGER  DEFAULT 0,
                selected_qualities   TEXT     DEFAULT '480p,720p,1080p',
                base_caption         TEXT,
                auto_caption_enabled BOOLEAN  DEFAULT TRUE,
                thumbnail_file_id    TEXT     DEFAULT NULL,
                CONSTRAINT single_row CHECK (id = 1)
            )
        """)
        # Insert default row only if it does not exist yet.
        # On conflict keep everything — never overwrite saved progress.
        await conn.execute("""
            INSERT INTO bot_progress (id, base_caption, auto_caption_enabled)
            VALUES (1, %s, TRUE)
            ON CONFLICT (id) DO NOTHING
        """, (progress["base_caption"],))
    print("DB ready", flush=True)
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
                    print(
                        f"Loaded S{progress['season']}E{progress['episode']} "
                        f"vc={progress['video_count']} "
                        f"AC={progress['auto_caption_enabled']} "
                        f"Cover={'Y' if progress['thumbnail_file_id'] else 'N'}",
                        flush=True,
                    )
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
                        target_chat_id=%s, season=%s, episode=%s, total_episode=%s,
                        video_count=%s, selected_qualities=%s, base_caption=%s,
                        auto_caption_enabled=%s, thumbnail_file_id=%s
                    WHERE id=1
                """, (
                    progress["target_chat_id"], progress["season"], progress["episode"],
                    progress["total_episode"], progress["video_count"],
                    ",".join(progress["selected_qualities"]), progress["base_caption"],
                    progress["auto_caption_enabled"], progress["thumbnail_file_id"],
                ))
            return
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                raise

# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════
def is_authorized(user_id: int) -> bool:
    return True if not AUTHORIZED_USERS else user_id in AUTHORIZED_USERS


async def delete_last_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    if chat_id in last_bot_messages:
        try:
            await context.bot.delete_message(chat_id, last_bot_messages[chat_id])
        except Exception:
            pass
        del last_bot_messages[chat_id]


def build_caption(quality: str, season: int = None, episode: int = None) -> str:
    s   = season  if season  is not None else progress["season"]
    ep  = episode if episode is not None else progress["episode"]
    tot = progress["total_episode"]
    return (
        progress["base_caption"]
        .replace("{season}",        f"{s:02}")
        .replace("{episode}",       f"{ep:02}")
        .replace("{total_episode}", f"{tot:02}")
        .replace("{quality}",       quality)
    )


def current_quality() -> str:
    if not progress["selected_qualities"]:
        return "N/A"
    return progress["selected_qualities"][progress["video_count"] % len(progress["selected_qualities"])]


async def advance_counters():
    """Counter mode: advance quality slot; wrap → bump episode."""
    progress["video_count"] += 1
    if progress["video_count"] >= len(progress["selected_qualities"]):
        progress["episode"]       += 1
        progress["total_episode"] += 1
        progress["video_count"]   = 0
    await save_progress()


async def advance_quality_only():
    """
    Recognised mode: episode comes from filename — do NOT touch
    episode/total_episode.  Only move the quality rotation forward.
    When it wraps, reset to 0.
    """
    progress["video_count"] += 1
    if progress["video_count"] >= len(progress["selected_qualities"]):
        progress["video_count"] = 0
    await save_progress()


async def get_cover_input(context: ContextTypes.DEFAULT_TYPE):
    """Re-download thumbnail bytes every call — Telegram rejects reused file_ids as covers."""
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
        print(f"Cover prep failed: {e}", flush=True)
        return None


async def _delete_and_repost(
    context:       ContextTypes.DEFAULT_TYPE,
    chat_id:       int,
    msg_id:        int,
    video_file_id: str,
    caption:       str,
    cover_input,
):
    """
    Delete original then repost with cover + caption.

    FIX: 1-second pause between delete and send_video.
    Without this pause the leech bot can fire the next video
    into the channel in the exact gap between our delete and
    our repost, causing that video to be processed out-of-order.
    The 1-second window is enough for Telegram to settle while
    being unnoticeable to viewers.
    """
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        print(f"Deleted original msg={msg_id}", flush=True)
    except Exception as del_err:
        print(f"Delete failed (msg={msg_id}): {del_err}", flush=True)

    # Brief pause so the leech bot's next upload does not land
    # between our delete and our repost.
    await asyncio.sleep(1)

    await context.bot.send_video(
        chat_id=chat_id,
        video=video_file_id,
        caption=caption,
        parse_mode=ParseMode.HTML,
        thumbnail=cover_input,
        api_kwargs={"cover": cover_input},
    )
    print(f"Reposted with cover + caption (msg was {msg_id})", flush=True)


# ══════════════════════════════════════════════════════════════
#  UI TEXT
# ══════════════════════════════════════════════════════════════
def settings_panel() -> str:
    ch    = progress["target_chat_id"]
    ac    = progress.get("auto_caption_enabled", True)
    thumb = bool(progress.get("thumbnail_file_id"))
    s, ep, tot = progress["season"], progress["episode"], progress["total_episode"]
    quals = ", ".join(progress["selected_qualities"]) if progress["selected_qualities"] else sc("none")
    ch_txt    = f"<code>{ch}</code>"  if ch    else f"<i>{sc('not set')}</i>"
    ac_txt    = f"<b>{sc('on')}</b>"  if ac    else f"<i>{sc('off')}</i>"
    thumb_txt = f"<b>{sc('set')}</b>" if thumb else f"<i>{sc('not set')}</i>"
    q_depth   = _channel_post_queue.qsize()
    queue_txt = f"  \u231b <b>{q_depth}</b> {sc('pending')}" if q_depth else ""
    return (
        f"\u250c {sc('channel')}        {ch_txt}\n"
        f"\u251c {sc('auto-caption')}  {ac_txt}\n"
        f"\u251c {sc('cover')}          {thumb_txt}\n"
        f"\u251c {sc('season / ep')}   <b>S{s:02} \u00b7 E{ep:02} / {tot:02}</b>\n"
        f"\u251c {sc('next quality')}  <b>{current_quality()}</b>\n"
        f"\u2514 {sc('qualities')}     <code>{quals}</code>{queue_txt}"
    )

# ══════════════════════════════════════════════════════════════
#  KEYBOARDS
# ══════════════════════════════════════════════════════════════
def get_menu_markup() -> InlineKeyboardMarkup:
    ac    = progress.get("auto_caption_enabled", True)
    thumb = bool(progress.get("thumbnail_file_id"))
    ac_btn    = f"{chr(0x2705) if ac else chr(0x2610)}  {sc('Auto-Caption')}  {sc('on') if ac else sc('off')}"
    thumb_btn = f"{chr(0x1F5BC)+chr(0xFE0F) if thumb else chr(0x1F304)}  {sc('Cover Image')}  {chr(0x2714) if thumb else ''}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"\U0001f441  {sc('Preview Caption')}",    callback_data="preview")],
        [InlineKeyboardButton(f"\U0001f4cb  {sc('View Raw Caption')}",   callback_data="view_raw_caption")],
        [InlineKeyboardButton(f"\u270f\ufe0f  {sc('Edit Caption')}",       callback_data="set_caption")],
        [
            InlineKeyboardButton(f"\U0001f5c2  {sc('Season')}",          callback_data="set_season"),
            InlineKeyboardButton(f"\U0001f3ac  {sc('Episode')}",         callback_data="set_episode"),
        ],
        [InlineKeyboardButton(f"\U0001f4cb  {sc('Total Episodes')}",     callback_data="set_total_episode")],
        [InlineKeyboardButton(f"\u2699\ufe0f  {sc('Quality Settings')}",   callback_data="quality_menu")],
        [InlineKeyboardButton(f"\U0001f4e1  {sc('Set Target Channel')}", callback_data="set_target_channel")],
        [InlineKeyboardButton(ac_btn,                          callback_data="toggle_auto_caption")],
        [InlineKeyboardButton(thumb_btn,                       callback_data="thumb_menu")],
        [
            InlineKeyboardButton(f"\U0001f504  {sc('Reset Ep.')}",       callback_data="reset"),
            InlineKeyboardButton(f"\U0001f5d1\ufe0f  {sc('Clear DB')}",  callback_data="clear_db"),
        ],
        [InlineKeyboardButton(f"\u2716  {sc('Close')}",              callback_data="cancel")],
    ])


def get_quality_markup() -> InlineKeyboardMarkup:
    rows = []
    for q in ALL_QUALITIES:
        mark = "\u2705 " if q in progress["selected_qualities"] else "     "
        rows.append([InlineKeyboardButton(f"{mark}{q}", callback_data=f"toggle_quality_{q}")])
    rows.append([InlineKeyboardButton(f"\u2b05  {sc('Back')}", callback_data="back_to_main")])
    return InlineKeyboardMarkup(rows)


def get_thumb_markup() -> InlineKeyboardMarkup:
    btns = [[InlineKeyboardButton(f"\U0001f4f8  {sc('Upload / Replace Cover')}", callback_data="set_thumbnail")]]
    if progress.get("thumbnail_file_id"):
        btns.append([
            InlineKeyboardButton(f"\U0001f441  {sc('View')}",         callback_data="view_thumbnail"),
            InlineKeyboardButton(f"\U0001f5d1\ufe0f  {sc('Remove')}", callback_data="remove_thumbnail"),
        ])
    btns.append([InlineKeyboardButton(f"\u2b05  {sc('Back')}", callback_data="back_to_main")])
    return InlineKeyboardMarkup(btns)


def back_btn():
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"\u2b05  {sc('Back to Menu')}", callback_data="back_to_main")]])

def cancel_btn():
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"\u2716  {sc('Cancel')}", callback_data="cancel")]])

def retry_cancel_btn(cb):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"\U0001f504  {sc('Try Again')}", callback_data=cb)],
        [InlineKeyboardButton(f"\u2716  {sc('Cancel')}",   callback_data="cancel")],
    ])

# ══════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_authorized(user_id):
        await update.message.reply_text(f"\u26d4  {sc('You are not authorized.')}")
        return
    try:
        await update.message.delete()
    except Exception:
        pass
    await delete_last_message(context, chat_id)
    prefix = ""
    if WELCOME_MESSAGE_ID and WELCOME_MESSAGE_CHAT_ID:
        try:
            await context.bot.copy_message(
                chat_id=chat_id, from_chat_id=WELCOME_MESSAGE_CHAT_ID,
                message_id=int(WELCOME_MESSAGE_ID),
            )
            prefix = f"\U0001f446  <i>{sc('Main control panel below')}</i>\n\n"
        except Exception as e:
            print(f"Welcome copy failed: {e}", flush=True)
            prefix = f"\u26a0\ufe0f  <i>{sc('Could not load welcome banner.')}</i>\n\n"
    sent = await context.bot.send_message(
        chat_id,
        f"\U0001f3af  <b>{sc('Beat Anime')}  \u00b7  {sc('Control Panel')}</b>\n\n"
        f"{prefix}{settings_panel()}",
        parse_mode=ParseMode.HTML,
        reply_markup=get_menu_markup(),
    )
    last_bot_messages[chat_id] = sent.message_id


# ══════════════════════════════════════════════════════════════
#  /status  — quick one-liner status without opening the menu
# ══════════════════════════════════════════════════════════════
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        return
    try:
        await update.message.delete()
    except Exception:
        pass
    q   = current_quality()
    dep = _channel_post_queue.qsize()
    ac  = progress.get("auto_caption_enabled", True)
    sent = await context.bot.send_message(
        update.effective_chat.id,
        f"\U0001f4ca  <b>{sc('Quick Status')}</b>\n\n"
        f"<blockquote>"
        f"\U0001f5c2  <b>S{progress['season']:02}</b>  "
        f"\U0001f3ac  <b>E{progress['episode']:02} / {progress['total_episode']:02}</b>\n"
        f"\U0001f4e1  {progress['target_chat_id'] or sc('not set')}\n"
        f"\U0001f3af  {sc('next quality')}  <b>{q}</b>\n"
        f"\u2699\ufe0f  {sc('auto-caption')}  <b>{sc('on') if ac else sc('off')}</b>\n"
        f"\u231b  {sc('queue depth')}  <b>{dep}</b>"
        f"</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"\U0001f3af  {sc('Open Menu')}", callback_data="back_to_main"),
        ]]),
    )
    last_bot_messages[update.effective_chat.id] = sent.message_id


# ══════════════════════════════════════════════════════════════
#  BUTTON HANDLER
# ══════════════════════════════════════════════════════════════
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    if not is_authorized(user_id):
        await query.answer(f"{sc('Not authorized.')}", show_alert=True)
        return
    await query.answer()
    chat_id = query.message.chat_id
    data    = query.data
    await delete_last_message(context, chat_id)

    # ── Toggle auto-caption ──────────────────────────────────────
    if data == "toggle_auto_caption":
        progress["auto_caption_enabled"] = not progress.get("auto_caption_enabled", True)
        await save_progress()
        state = sc("enabled") if progress["auto_caption_enabled"] else sc("disabled")
        sent = await query.message.reply_text(
            f"\U0001f4cc  <b>{sc('Auto-Caption')}</b> {sc('is now')} <b>{state}</b>\n\n{settings_panel()}",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    # ── Thumbnail menu ───────────────────────────────────────────
    if data == "thumb_menu":
        thumb  = progress.get("thumbnail_file_id")
        status = f"\u2705  <b>{sc('Cover is set')}</b>" if thumb else f"\U0001f304  <i>{sc('No cover set yet')}</i>"
        sent = await query.message.reply_text(
            f"\U0001f5bc\ufe0f  <b>{sc('Cover / Thumbnail Settings')}</b>\n\n"
            f"{status}\n\n"
            f"<blockquote>"
            f"{sc('The cover is applied to every video posted to the channel.')}\n"
            f"{sc('In auto-mode the bot deletes and re-posts with cover + caption.')}"
            f"</blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=get_thumb_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    if data == "set_thumbnail":
        waiting_for_input[user_id] = "thumbnail"
        sent = await query.message.reply_text(
            f"\U0001f4f8  <b>{sc('Send a Photo')}</b>\n\n"
            f"<blockquote>{sc('This image will become the cover of every video posted to the channel.')}</blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    if data == "view_thumbnail":
        if progress.get("thumbnail_file_id"):
            sent = await context.bot.send_photo(
                chat_id=chat_id, photo=progress["thumbnail_file_id"],
                caption=f"\U0001f5bc\ufe0f  <b>{sc('Current Cover')}</b>",
                parse_mode=ParseMode.HTML, reply_markup=get_thumb_markup(),
            )
        else:
            sent = await query.message.reply_text(
                f"\U0001f304  <i>{sc('No cover set. Upload one first.')}</i>",
                parse_mode=ParseMode.HTML, reply_markup=get_thumb_markup(),
            )
        last_bot_messages[chat_id] = sent.message_id
        return

    if data == "remove_thumbnail":
        progress["thumbnail_file_id"] = None
        await save_progress()
        sent = await query.message.reply_text(
            f"\U0001f5d1\ufe0f  <b>{sc('Cover Removed')}</b>\n\n"
            f"<i>{sc('Videos will be posted without a custom cover.')}</i>",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    # ── View raw caption (HTML tags visible) ─────────────────────
    if data == "view_raw_caption":
        raw  = _html.escape(progress["base_caption"])
        sent = await query.message.reply_text(
            f"\U0001f4cb  <b>{sc('Raw Caption Template')}</b>\n\n"
            f"<blockquote><i>{sc('All HTML tags shown as-is. Copy, edit, send back.')}</i></blockquote>\n\n"
            f"<code>{raw}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"\u270f\ufe0f  {sc('Edit Caption')}", callback_data="set_caption")],
                [InlineKeyboardButton(f"\u2b05  {sc('Back')}",               callback_data="back_to_main")],
            ]),
        )
        last_bot_messages[chat_id] = sent.message_id
        return

    # ── Preview ─────────────────────────────────────────────────
    if data == "preview":
        quality = current_quality()
        preview = build_caption(quality)
        sent = await query.message.reply_text(
            f"\U0001f441  <b>{sc('Caption Preview')}</b>  \u00b7  <i>{sc('next')}: <b>{quality}</b></i>\n\n"
            f"{preview}\n\n"
            f"<blockquote>{settings_panel()}</blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_caption":
        waiting_for_input[user_id] = "caption"
        sent = await query.message.reply_text(
            f"\u270f\ufe0f  <b>{sc('Edit Caption')}</b>\n\n"
            f"<blockquote>"
            f"{sc('Send your new caption now. HTML tags are supported.')}\n\n"
            f"{sc('Available placeholders:')}\n"
            f"  <code>{{season}}</code>    <code>{{episode}}</code>\n"
            f"  <code>{{total_episode}}</code>    <code>{{quality}}</code>"
            f"</blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_season":
        waiting_for_input[user_id] = "season"
        sent = await query.message.reply_text(
            f"\U0001f5c2  <b>{sc('Set Season')}</b>\n\n"
            f"{sc('Current')}: <b>S{progress['season']:02}</b>\n\n"
            f"<i>{sc('Send the new season number, e.g.')} <code>2</code></i>",
            parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_episode":
        waiting_for_input[user_id] = "episode"
        sent = await query.message.reply_text(
            f"\U0001f3ac  <b>{sc('Set Episode')}</b>\n\n"
            f"{sc('Current')}: <b>E{progress['episode']:02}</b>\n\n"
            f"<i>{sc('Send the new episode number, e.g.')} <code>5</code></i>",
            parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_total_episode":
        waiting_for_input[user_id] = "total_episode"
        sent = await query.message.reply_text(
            f"\U0001f4cb  <b>{sc('Set Total Episodes')}</b>\n\n"
            f"{sc('Current')}: <b>{progress['total_episode']:02}</b>\n\n"
            f"<i>{sc('Send the total episode count, e.g.')} <code>12</code></i>",
            parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "set_target_channel":
        waiting_for_input[user_id] = "set_channel"
        sent = await query.message.reply_text(
            f"\U0001f4e1  <b>{sc('Set Target Channel')}</b>\n\n"
            f"<blockquote>"
            f"<b>A</b>  {sc('Forward any message from the channel')}\n"
            f"<b>B</b>  {sc('Send channel ID')}  \u2014  <code>-1001234567890</code>\n"
            f"<b>C</b>  {sc('Send username')}  \u2014  <code>@yourchannel</code>"
            f"</blockquote>\n\n"
            f"<i>{sc('Make sure the bot is admin with Post Messages permission.')}</i>",
            parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "quality_menu":
        quals = ", ".join(progress["selected_qualities"]) if progress["selected_qualities"] else sc("none")
        sent = await query.message.reply_text(
            f"\u2699\ufe0f  <b>{sc('Quality Settings')}</b>\n\n"
            f"<blockquote>{sc('Tap a quality to toggle it on or off.')}\n"
            f"{sc('Selected')}: <b>{quals}</b></blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=get_quality_markup(),
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
        quals = ", ".join(progress["selected_qualities"]) if progress["selected_qualities"] else sc("none")
        body  = (
            f"\u2699\ufe0f  <b>{sc('Quality Settings')}</b>\n\n"
            f"<blockquote>{sc('Tap a quality to toggle it on or off.')}\n"
            f"{sc('Selected')}: <b>{quals}</b></blockquote>"
        )
        try:
            await query.edit_message_text(body, parse_mode=ParseMode.HTML, reply_markup=get_quality_markup())
        except Exception:
            sent = await query.message.reply_text(body, parse_mode=ParseMode.HTML, reply_markup=get_quality_markup())
            last_bot_messages[chat_id] = sent.message_id

    elif data == "back_to_main":
        try:
            await query.message.delete()
        except Exception:
            pass
        sent = await context.bot.send_message(
            chat_id,
            f"\U0001f3af  <b>{sc('Beat Anime')}  \u00b7  {sc('Control Panel')}</b>\n\n{settings_panel()}",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "reset":
        progress["episode"]     = 1
        progress["video_count"] = 0
        await save_progress()
        sent = await query.message.reply_text(
            f"\U0001f504  <b>{sc('Episode Counter Reset')}</b>\n\n"
            f"<i>{sc('Restarting from')} <b>E01  \u00b7  S{progress['season']:02}</b></i>",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
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
            row_count = db_size = sc("unknown")
        sent = await query.message.reply_text(
            f"\u26a0\ufe0f  <b>{sc('Clear Database')}</b>\n\n"
            f"<blockquote>"
            f"\U0001f4e6  {sc('size')}   <b>{db_size}</b>\n"
            f"\U0001f4c4  {sc('rows')}   <b>{row_count}</b>"
            f"</blockquote>\n\n"
            f"{sc('This will reset all counters and remove the target channel.')}\n"
            f"<i>{sc('Caption, qualities and cover are preserved.')}</i>\n\n"
            f"<b>{sc('Are you sure?')}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"\u2705  {sc('Yes  -  Clear & Optimise')}", callback_data="confirm_clear_db")],
                [InlineKeyboardButton(f"\u2716  {sc('No  -  Go Back')}",           callback_data="back_to_main")],
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
                ",".join(progress["selected_qualities"]), progress["base_caption"],
                progress["auto_caption_enabled"], progress["thumbnail_file_id"],
            ))
            await conn.execute("VACUUM FULL bot_progress")
        await load_progress()
        sent = await query.message.reply_text(
            f"\u2705  <b>{sc('Database Cleared & Optimised')}</b>\n\n"
            f"<blockquote>"
            f"\u2022 {sc('All counters reset')}\n"
            f"\u2022 {sc('Duplicate data removed')}\n"
            f"\u2022 {sc('Storage reclaimed')}\n"
            f"\u2022 {sc('Caption, qualities and cover preserved')}"
            f"</blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif data == "cancel":
        waiting_for_input.pop(user_id, None)
        sent = await query.message.reply_text(
            f"\u2716  <i>{sc('Cancelled.')}</i>",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

# ══════════════════════════════════════════════════════════════
#  MESSAGE HANDLER
# ══════════════════════════════════════════════════════════════
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

    if input_type == "thumbnail":
        if update.message.photo:
            progress["thumbnail_file_id"] = update.message.photo[-1].file_id
            await save_progress()
            del waiting_for_input[user_id]
            sent = await context.bot.send_message(
                chat_id,
                f"\u2705  <b>{sc('Cover Saved!')}</b>\n\n"
                f"<i>{sc('Applied to every video posted to the channel.')}</i>",
                parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
            )
            last_bot_messages[chat_id] = sent.message_id
        else:
            sent = await context.bot.send_message(
                chat_id,
                f"\u274c  {sc('Please send a')} <b>{sc('photo')}</b> {sc('(not a file or video).')}",
                parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
            )
            last_bot_messages[chat_id] = sent.message_id
        return

    if input_type == "set_channel" and update.message.forward_origin:
        if hasattr(update.message.forward_origin, "chat"):
            channel    = update.message.forward_origin.chat
            channel_id = channel.id
            try:
                await context.bot.get_chat(channel_id)
                progress["target_chat_id"] = channel_id
                await save_progress()
                del waiting_for_input[user_id]
                sent = await context.bot.send_message(
                    chat_id,
                    f"\u2705  <b>{sc('Target Channel Set')}</b>\n\n"
                    f"<blockquote>\U0001f4db  {channel.title}\n\U0001f194  <code>{channel_id}</code></blockquote>",
                    parse_mode=ParseMode.HTML, reply_markup=back_btn(),
                )
                last_bot_messages[chat_id] = sent.message_id
            except Exception as e:
                sent = await context.bot.send_message(
                    chat_id,
                    f"\u274c  <b>{sc('Cannot Access Channel')}</b>\n\n<code>{e}</code>\n\n"
                    f"<i>{sc('Make sure the bot is admin with Post Messages permission.')}</i>",
                    parse_mode=ParseMode.HTML, reply_markup=retry_cancel_btn("set_target_channel"),
                )
                last_bot_messages[chat_id] = sent.message_id
        else:
            sent = await context.bot.send_message(
                chat_id,
                f"\u274c  {sc('Forward from a')} <b>{sc('channel')}</b>{sc(', not from a user.')}",
                parse_mode=ParseMode.HTML, reply_markup=retry_cancel_btn("set_target_channel"),
            )
            last_bot_messages[chat_id] = sent.message_id
        return

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
                    f"\u2705  <b>{sc('Target Channel Set')}</b>\n\n"
                    f"<blockquote>\U0001f4db  {info.title}\n\U0001f194  <code>{info.id}</code></blockquote>",
                    parse_mode=ParseMode.HTML, reply_markup=back_btn(),
                )
                last_bot_messages[chat_id] = sent.message_id
            else:
                sent = await context.bot.send_message(
                    chat_id,
                    f"\u274c  <b>{sc('Invalid Format')}</b>\n\n{sc('Send channel ID or')} <code>@username</code>.",
                    parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
                )
                last_bot_messages[chat_id] = sent.message_id
        except Exception as e:
            sent = await context.bot.send_message(
                chat_id,
                f"\u274c  <b>{sc('Cannot Access Channel')}</b>\n\n<code>{e}</code>",
                parse_mode=ParseMode.HTML, reply_markup=retry_cancel_btn("set_target_channel"),
            )
            last_bot_messages[chat_id] = sent.message_id
        return

    if not update.message.text:
        return
    text = update.message.text

    if input_type == "caption":
        progress["base_caption"] = text
        await save_progress()
        del waiting_for_input[user_id]
        sent = await context.bot.send_message(
            chat_id,
            f"\u2705  <b>{sc('Caption Updated!')}</b>",
            parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
        )
        last_bot_messages[chat_id] = sent.message_id

    elif input_type in ("season", "episode", "total_episode"):
        if text.isdigit():
            progress[input_type] = int(text)
            await save_progress()
            del waiting_for_input[user_id]
            labels = {"season": "Season", "episode": "Episode", "total_episode": "Total Episodes"}
            sent = await context.bot.send_message(
                chat_id,
                f"\u2705  <b>{sc(labels[input_type])}</b> {sc('set to')} <b>{text}</b>",
                parse_mode=ParseMode.HTML, reply_markup=get_menu_markup(),
            )
            last_bot_messages[chat_id] = sent.message_id
        else:
            sent = await context.bot.send_message(
                chat_id,
                f"\u274c  {sc('Please enter a valid number.')}",
                parse_mode=ParseMode.HTML, reply_markup=cancel_btn(),
            )
            last_bot_messages[chat_id] = sent.message_id

# ══════════════════════════════════════════════════════════════
#  VIDEO HANDLER (private → channel)
# ══════════════════════════════════════════════════════════════
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        await update.message.reply_text(f"\u26d4  {sc('Authentication error.')}")
        return
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text(f"\u26d4  {sc('You are not authorized.')}")
        return

    if not progress["target_chat_id"]:
        await update.message.reply_text(
            f"\u274c  <b>{sc('No Target Channel')}</b>\n\n<i>{sc('Set a target channel from the main menu.')}</i>",
            parse_mode=ParseMode.HTML,
        )
        return
    if not progress["selected_qualities"]:
        await update.message.reply_text(
            f"\u274c  <b>{sc('No Qualities Selected')}</b>\n\n"
            f"<i>{sc('Select at least one quality from Quality Settings.')}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    file_id = update.message.video.file_id
    quality = current_quality()
    caption = build_caption(quality)

    try:
        await context.bot.get_chat(progress["target_chat_id"])
    except Exception as e:
        await update.message.reply_text(
            f"\u274c  <b>{sc('Cannot Reach Channel')}</b>\n\n<code>{e}</code>\n\n"
            f"<i>{sc('Ensure the bot is admin with Post Messages permission.')}</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        cover_input = await get_cover_input(context)
        send_kwargs = dict(
            chat_id=progress["target_chat_id"],
            video=file_id,
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
        if cover_input:
            send_kwargs["thumbnail"]  = cover_input
            send_kwargs["api_kwargs"] = {"cover": cover_input}

        sent_msg = await context.bot.send_video(**send_kwargs)
        print(f"Private send msg_id={sent_msg.message_id} Q={quality}", flush=True)

        idx   = progress["video_count"] + 1
        total = len(progress["selected_qualities"])
        await update.message.reply_text(
            f"\u2705  <b>{sc('Posted to Channel!')}</b>\n\n"
            f"<blockquote>"
            f"\U0001f3ac  {sc('quality')}    <b>{quality}</b>\n"
            f"\U0001f5bc\ufe0f  {sc('cover')}      <b>{'set' if cover_input else 'none'}</b>\n"
            f"\U0001f4ca  {sc('progress')}  <b>{idx}/{total}</b> {sc('for this episode')}"
            f"</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        await advance_counters()

    except Exception as e:
        err = str(e).lower()
        if "not enough rights" in err or "admin" in err:
            await update.message.reply_text(f"\u26d4  {sc('Bot lacks admin rights in the target channel.')}")
        elif "chat not found" in err:
            await update.message.reply_text(f"\u274c  {sc('Channel not found. Please reset the target channel.')}")
        else:
            await update.message.reply_text(f"\u274c  <code>{e}</code>", parse_mode=ParseMode.HTML)
        import traceback; traceback.print_exc()

# ══════════════════════════════════════════════════════════════
#  CHANNEL POST HANDLER — enqueues only, never blocks polling
# ══════════════════════════════════════════════════════════════
async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel_post = update.channel_post
    if not channel_post or not channel_post.video:
        return
    chat_id = channel_post.chat.id
    if chat_id != progress.get("target_chat_id"):
        return
    if not progress.get("auto_caption_enabled", False):
        return

    # Snapshot BOTH fields before queuing.
    # Leech bots put episode info in file_name, not caption.
    caption_text  = channel_post.caption or ""
    filename_text = channel_post.video.file_name or ""

    depth = _channel_post_queue.qsize()
    if depth >= _QUEUE_WARN_DEPTH:
        print(f"WARNING: queue depth={depth} — leech bot sending faster than bot can process", flush=True)

    await _channel_post_queue.put((
        channel_post.message_id,
        channel_post.video.file_id,
        caption_text,
        filename_text,
        chat_id,
        context,
    ))
    print(
        f"Queued msg={channel_post.message_id} "
        f"file='{filename_text[:50]}' depth={depth + 1}",
        flush=True,
    )


# ══════════════════════════════════════════════════════════════
#  ACTUAL PROCESSING — called by worker, strictly one at a time
# ══════════════════════════════════════════════════════════════
async def _process_channel_post(
    msg_id:        int,
    video_file_id: str,
    caption_text:  str,
    filename_text: str,
    chat_id:       int,
    context:       ContextTypes.DEFAULT_TYPE,
):
    global _last_recog_season, _last_recog_episode

    # Always reload from DB — guaranteed to see state written by previous job
    await load_progress()

    # FIX: re-check auto_caption after DB reload.
    # User might have toggled it between enqueue and now.
    if not progress.get("auto_caption_enabled", False):
        print(f"Auto-caption disabled — skipping msg={msg_id}", flush=True)
        return

    if not progress["selected_qualities"]:
        print("No qualities configured — skipping", flush=True)
        return

    # Try caption first, then filename
    parsed_season, parsed_ep = parse_episode_info(caption_text, filename_text)

    if parsed_season is not None and parsed_ep is not None:
        # ── RECOGNISED MODE ─────────────────────────────────────
        # FIX: episode boundary detection.
        # When the episode number changes we reset video_count to 0
        # so the first quality in the rotation is always used for
        # the first file of each new episode.
        ep_changed = (
            parsed_season  != _last_recog_season or
            parsed_ep      != _last_recog_episode
        )
        if ep_changed:
            if _last_recog_episode is not None:
                print(
                    f"Episode boundary: "
                    f"S{_last_recog_season:02}E{_last_recog_episode:02} → "
                    f"S{parsed_season:02}E{parsed_ep:02} — resetting vc to 0",
                    flush=True,
                )
            progress["video_count"] = 0
            await save_progress()
            _last_recog_season  = parsed_season
            _last_recog_episode = parsed_ep

        quality = current_quality()
        caption = build_caption(quality, season=parsed_season, episode=parsed_ep)
        print(
            f"[RECOGNISED] msg={msg_id} "
            f"S{parsed_season:02}E{parsed_ep:02} Q={quality} vc={progress['video_count']}",
            flush=True,
        )

        cover_input = await get_cover_input(context)
        try:
            if cover_input:
                # FIX: _delete_and_repost waits 1 s between delete and send
                # to prevent leech bot from slipping the next video into the gap.
                await _delete_and_repost(
                    context, chat_id, msg_id, video_file_id, caption, cover_input
                )
            else:
                await context.bot.edit_message_caption(
                    chat_id=chat_id, message_id=msg_id,
                    caption=caption, parse_mode=ParseMode.HTML,
                )
                print("Caption edited, no cover (recognised)", flush=True)

            await advance_quality_only()

        except Exception as e:
            print(f"Channel post error msg={msg_id}: {e}", flush=True)
            import traceback; traceback.print_exc()
        return

    # ── COUNTER MODE — no episode info found anywhere ────────────
    quality     = current_quality()
    caption     = build_caption(quality)
    cover_input = await get_cover_input(context)
    print(
        f"[COUNTER] msg={msg_id} "
        f"S{progress['season']:02}E{progress['episode']:02} Q={quality} vc={progress['video_count']}",
        flush=True,
    )
    try:
        if cover_input:
            # FIX: same 1-second pause applies in counter mode too
            await _delete_and_repost(
                context, chat_id, msg_id, video_file_id, caption, cover_input
            )
        else:
            await context.bot.edit_message_caption(
                chat_id=chat_id, message_id=msg_id,
                caption=caption, parse_mode=ParseMode.HTML,
            )
            print("Caption edited, no cover (counter)", flush=True)

        await advance_counters()

    except Exception as e:
        print(f"Channel post error msg={msg_id}: {e}", flush=True)
        import traceback; traceback.print_exc()

# ══════════════════════════════════════════════════════════════
#  WEB SERVER & SELF-PING
# ══════════════════════════════════════════════════════════════
async def health_check(request):
    return web.Response(text="Bot is running!")

async def self_ping():
    import aiohttp
    url = os.getenv("RENDER_EXTERNAL_URL", "https://beat-caption-bot.onrender.com")
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{url}/health") as r:
                    print(f"Ping {'ok' if r.status == 200 else r.status}", flush=True)
        except Exception as e:
            print(f"Ping error: {e}", flush=True)

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    print(f"Web server on port {PORT}", flush=True)
    asyncio.create_task(self_ping())

async def post_init(application: Application):
    global _queue_worker_task
    await init_db()
    await start_web_server()
    _queue_worker_task = asyncio.create_task(_channel_post_worker())
    print("Channel-post queue worker started", flush=True)

async def post_shutdown(application: Application):
    global db_pool, _queue_worker_task
    print("Shutting down", flush=True)
    if _queue_worker_task:
        _queue_worker_task.cancel()
        try:
            await _queue_worker_task
        except asyncio.CancelledError:
            pass
    if db_pool:
        try:
            await db_pool.close()
        except Exception as e:
            print(f"DB close error: {e}", flush=True)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Error: {context.error}", flush=True)
    import traceback; traceback.print_exc()
    if update and update.effective_chat and update.effective_chat.type == "private":
        try:
            await context.bot.send_message(
                update.effective_chat.id,
                f"\u274c  {sc('An error occurred. Please try again.')}\n\n"
                f"<code>{str(context.error)[:120]}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
def main():
    print("=" * 50, flush=True)
    print("  BEAT ANIME  -  Caption + Cover Bot", flush=True)
    print("=" * 50, flush=True)
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    application.add_handler(CommandHandler("start",  start))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.VIDEO & ~filters.COMMAND,
        handle_video,
    ))
    application.add_handler(MessageHandler(
        filters.ChatType.PRIVATE &
        (filters.TEXT | filters.PHOTO | filters.FORWARDED) &
        ~filters.VIDEO & ~filters.COMMAND,
        handle_message,
    ))
    application.add_handler(MessageHandler(
        filters.ChatType.CHANNEL & filters.VIDEO,
        handle_channel_post,
    ))
    application.add_error_handler(error_handler)
    print(f"Auth: {AUTHORIZED_USERS or 'All users'}", flush=True)
    print("=" * 50, flush=True)
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    import signal, sys
    def sig_handler(sig, frame):
        print("Shutdown signal", flush=True)
        sys.exit(0)
    signal.signal(signal.SIGINT,  sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped by user", flush=True)
    except Exception as e:
        print(f"Fatal: {e}", flush=True)
        import traceback; traceback.print_exc()
    finally:
        print("Goodbye!", flush=True)
