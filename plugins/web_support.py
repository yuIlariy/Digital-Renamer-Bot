# (c) @RknDeveloperr
# Rkn Developer 
# Don't Remove Credit 😔
# Telegram Channel @RknDeveloper & @Rkn_Botz
# Developer @RknDeveloperr
# Special Thanks To @ReshamOwner
# Update Channel @Digital_Botz & @OthersBees
"""
Apache License 2.0
Copyright (c) 2022 @Digital_Botz
"""

import aiohttp
from aiohttp import web
import time
import psutil
import shutil
import os
import re
import uuid
import json
import hmac
import hashlib
import asyncio
import pytz
from urllib.parse import parse_qsl
from datetime import datetime, timezone, timedelta
from config import Config
from plugins import __version__
from helper.utils import humanbytes
from helper.database import digital_botz
from wzgram.types import Message

try:
    from wzgram.enums import ParseMode
except ImportError:
    ParseMode = None

try:
    from wzgram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid
except ImportError:
    from wzgram.errors import FloodWait
    InputUserDeactivated = UserIsBlocked = PeerIdInvalid = Exception

# Ensure templates & temp broadcast directories exist
os.makedirs('templates', exist_ok=True)
os.makedirs('BroadcastTemp', exist_ok=True)

DigitalRenameBot = web.RouteTableDef()

TG_POST_LINK_RE = re.compile(
    r"^https?://(?:t\.me|telegram\.me|telegram\.dog)/(?:s/)?(?:c/(\d+)|([A-Za-z0-9_]{3,}))/(?:\d+/)?(\d+)(?:\?.*)?$",
    re.IGNORECASE,
)

MAX_BROADCAST_UPLOAD_BYTES = 28 * 1024 * 1024  # 28 MB

# --- NETWORK SPEED TRACKER GLOBALS ---
last_net_io = psutil.net_io_counters()
last_time = time.time()
last_up_speed = 0.0
last_dl_speed = 0.0

# --- DB QUERY TTL CACHE (Reduces MongoDB load on rapid stream ticks) ---
_db_cache = {
    "timestamp": 0.0,
    "ttl": 15.0,
    "real_total_users": 0,
    "real_total_premium_users": 0,
    "raw_total_renames": 0,
    "net_stats": {"sent": 0, "recv": 0}
}

# --- PERSISTENT 7-DAY & 24-HOUR ANALYTICS ROLLUP BUFFER & CACHE ---
_rollup_buffer = {
    "daily": {},   # { "YYYY-MM-DD": {"renames": int, "bytes": int, "peak": int} }
    "hourly": {}   # { "YYYY-MM-DDTHH": {"renames": int, "bytes": int, "peak": int} }
}

_net_rollup_baseline = {
    "db_total": None,
    "psutil_total": None,
    "recorded_direct_bytes": 0
}

_analytics_cache = {
    "timestamp": 0.0,
    "ttl": 15.0,
    "data": None
}

_analytics_24h_cache = {
    "timestamp": 0.0,
    "ttl": 15.0,
    "data": None
}

# --- WEB ADMIN BROADCAST STATE ---
_broadcast_state = {
    "running": False,
    "sent": 0,
    "failed": 0,
    "total": 0,
    "has_media": False,
    "parse_mode": "html",
    "task": None
}


def _utc_keys(now_dt=None):
    dt = now_dt or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H")


def record_rename_event(size_bytes: int = 0):
    """Buffers a completed rename and its transferred bytes into hourly/daily rollup counters."""
    try:
        day_key, hour_key = _utc_keys()
        safe_bytes = max(0, int(size_bytes or 0))

        d_entry = _rollup_buffer["daily"].setdefault(day_key, {"renames": 0, "bytes": 0, "peak": 0})
        d_entry["renames"] += 1
        d_entry["bytes"] += safe_bytes

        h_entry = _rollup_buffer["hourly"].setdefault(hour_key, {"renames": 0, "bytes": 0, "peak": 0})
        h_entry["renames"] += 1
        h_entry["bytes"] += safe_bytes

        if safe_bytes > 0:
            _net_rollup_baseline["recorded_direct_bytes"] += safe_bytes

        # Invalidate caches so the next SSE tick reflects the completed task immediately
        _analytics_cache["timestamp"] = 0.0
        _analytics_24h_cache["timestamp"] = 0.0
        _db_cache["timestamp"] = 0.0
    except Exception:
        pass


# ⚠️ INVISIBLE HOOK: Fallback listener for plugins that report completion via Message.edit
if not hasattr(Message, "_hooked_for_renames"):
    original_edit = getattr(Message, "edit", None)
    original_edit_text = getattr(Message, "edit_text", None)

    async def hooked_edit_wrapper(self, text, *args, **kwargs):
        if isinstance(text, str) and "Uploaded Successfully" in text and not getattr(self, "_already_counted", False):
            self._already_counted = True
            await digital_botz.increment_total_renames()
            record_rename_event()
        if original_edit:
            return await original_edit(self, text, *args, **kwargs)

    async def hooked_edit_text_wrapper(self, text, *args, **kwargs):
        if isinstance(text, str) and "Uploaded Successfully" in text and not getattr(self, "_already_counted", False):
            self._already_counted = True
            await digital_botz.increment_total_renames()
            record_rename_event()
        if original_edit_text:
            return await original_edit_text(self, text, *args, **kwargs)

    if original_edit:
        Message.edit = hooked_edit_wrapper
    if original_edit_text:
        Message.edit_text = hooked_edit_text_wrapper

    Message._hooked_for_renames = True


# ==========================================
# --- TELEGRAM MINI APP AUTH VERIFICATION ---
# ==========================================
def _get_admin_ids():
    raw = getattr(Config, "ADMIN", [])
    if isinstance(raw, int):
        return {raw}
    if isinstance(raw, (list, tuple, set)):
        return {int(x) for x in raw if str(x).isdigit()}
    if isinstance(raw, str):
        return {int(x) for x in raw.replace(",", " ").split() if x.isdigit()}
    return set()


def verify_telegram_webapp_user(init_data: str):
    """Validates Telegram WebApp initData via HMAC-SHA256 and returns the user dict."""
    bot_token = getattr(Config, "BOT_TOKEN", "")
    if not init_data or not bot_token:
        return None

    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None

        auth_date = int(parsed.get("auth_date", 0))
        if auth_date <= 0 or (time.time() - auth_date) > 86400:
            return None

        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(calculated_hash, received_hash):
            return None

        user_raw = parsed.get("user")
        if not user_raw:
            return None

        user_obj = json.loads(user_raw)
        if not int(user_obj.get("id", 0)):
            return None

        return user_obj
    except Exception:
        return None


def verify_telegram_webapp(init_data: str):
    """Validates Telegram WebApp initData and ensures user is in Config.ADMIN."""
    user_obj = verify_telegram_webapp_user(init_data)
    if not user_obj:
        return None
    if int(user_obj.get("id", 0)) not in _get_admin_ids():
        return None
    return user_obj


def _extract_user_from_request(request):
    auth_header = request.headers.get("Authorization", "")
    init_data = ""
    if auth_header.startswith("tma "):
        init_data = auth_header[4:].strip()
    elif "initData" in request.query:
        init_data = request.query.get("initData", "").strip()
    if not init_data:
        return None
    return verify_telegram_webapp_user(init_data)


def _extract_admin_from_request(request):
    user_obj = _extract_user_from_request(request)
    if not user_obj:
        return None
    if int(user_obj.get("id", 0)) not in _get_admin_ids():
        return None
    return user_obj


def _get_bot_client():
    try:
        from plugins import file_rename
        return getattr(Config, "BOT_CLIENT", None) or getattr(file_rename, "main_bot_client", None)
    except Exception:
        return getattr(Config, "BOT_CLIENT", None)


async def _notify_user_and_admin(
    bot_client,
    admin_id: int,
    target_uid: int,
    user_text: str,
    admin_text_builder,
    log_text: str = ""
) -> bool:
    """Sends DM notifications to the target user and the acting admin, plus optional log channel."""
    if not bot_client:
        return False

    user_notified = False
    if user_text:
        try:
            await bot_client.send_message(
                chat_id=int(target_uid),
                text=user_text,
                disable_web_page_preview=True
            )
            user_notified = True
        except Exception:
            user_notified = False

    admin_msg = admin_text_builder(user_notified) if callable(admin_text_builder) else str(admin_text_builder)
    if admin_id and admin_msg:
        try:
            await bot_client.send_message(
                chat_id=int(admin_id),
                text=admin_msg,
                disable_web_page_preview=True
            )
        except Exception:
            pass

    if log_text and getattr(Config, "LOG_CHANNEL", None):
        try:
            await bot_client.send_message(
                chat_id=Config.LOG_CHANNEL,
                text=log_text,
                disable_web_page_preview=True
            )
        except Exception:
            pass

    return user_notified


def format_short_number(num):
    """Formats numbers to 1k, 1.8k, 2.39k formats."""
    if num < 1000:
        return str(num)
    for unit in ['k', 'M', 'B']:
        num /= 1000.0
        if num < 1000:
            formatted = f"{num:.2f}".rstrip('0').rstrip('.')
            return f"{formatted}{unit}"
    return str(num)


def extract_extension(filename: str) -> str:
    """Extracts a brief uppercase file extension (e.g. MKV, MP4, ZIP)."""
    if not filename or not isinstance(filename, str) or "." not in filename:
        return ""
    ext = filename.rsplit(".", 1)[-1].strip().upper()
    if not ext or len(ext) > 8 or "/" in ext or "\\" in ext or " " in ext:
        return ""
    return ext


def mask_filename(filename: str) -> str:
    """Masks filenames for non-owner/non-admin telemetry viewers while preserving extension."""
    if not filename or not isinstance(filename, str):
        return "••••••••"
    ext = extract_extension(filename)
    return f"••••••••.{ext.lower()}" if ext else "••••••••"


def parse_human_bytes(size_str):
    """Parses human-readable byte strings (e.g., '12.50 MB/s') back to float bytes."""
    if not size_str or not isinstance(size_str, str):
        return 0.0
    clean = size_str.replace("/s", "").strip()
    parts = clean.split()
    if len(parts) != 2:
        return 0.0
    try:
        val = float(parts[0])
    except ValueError:
        return 0.0
    unit = parts[1].upper()
    multipliers = {
        "B": 1,
        "KB": 1024, "KIB": 1024,
        "MB": 1024 ** 2, "MIB": 1024 ** 2,
        "GB": 1024 ** 3, "GIB": 1024 ** 3,
        "TB": 1024 ** 4, "TIB": 1024 ** 4,
    }
    return val * multipliers.get(unit, 1)


def format_eta(current_str, total_str, speed_str):
    """Calculates human-readable ETA from formatted progress strings."""
    speed_bps = parse_human_bytes(speed_str)
    if speed_bps <= 0:
        return "Calculating..."
    current_b = parse_human_bytes(current_str)
    total_b = parse_human_bytes(total_str)
    remaining_b = max(0.0, total_b - current_b)
    if remaining_b <= 0:
        return "Finishing..."
    rem_sec = int(remaining_b / speed_bps)
    if rem_sec < 60:
        return f"{rem_sec}s"
    mins, secs = divmod(rem_sec, 60)
    if mins < 60:
        return f"{mins}m {secs:02d}s"
    hrs, mins = divmod(mins, 60)
    return f"{hrs}h {mins:02d}m"


def _sample_active_tasks_count() -> int:
    """Reads live active pipeline task count safely."""
    try:
        from plugins import file_rename
        active_tasks = getattr(file_rename, "active_tasks", {})
        active_progress = getattr(file_rename, "active_progress", {})
        return int(max(len(active_tasks), len(active_progress)))
    except Exception:
        return 0


async def _collect_transferred_bytes_delta() -> int:
    """Computes incremental transferred bytes not already captured via record_rename_event()."""
    delta_bytes = 0
    try:
        net_stats = await digital_botz.get_network_stats()
        if isinstance(net_stats, dict):
            cur_db_total = int(net_stats.get("sent", 0)) + int(net_stats.get("recv", 0))
            if cur_db_total > 0:
                if _net_rollup_baseline["db_total"] is None:
                    _net_rollup_baseline["db_total"] = cur_db_total
                elif cur_db_total >= _net_rollup_baseline["db_total"]:
                    raw_delta = cur_db_total - _net_rollup_baseline["db_total"]
                    _net_rollup_baseline["db_total"] = cur_db_total
                    already_recorded = _net_rollup_baseline["recorded_direct_bytes"]
                    if already_recorded > 0:
                        delta_bytes = max(0, raw_delta - already_recorded)
                        _net_rollup_baseline["recorded_direct_bytes"] = max(0, already_recorded - raw_delta)
                    else:
                        delta_bytes = raw_delta
                else:
                    _net_rollup_baseline["db_total"] = cur_db_total
                return delta_bytes
    except Exception:
        pass

    if _net_rollup_baseline["recorded_direct_bytes"] > 0:
        _net_rollup_baseline["recorded_direct_bytes"] = 0
        return 0

    try:
        io = psutil.net_io_counters()
        cur_psutil_total = int(io.bytes_sent + io.bytes_recv)
        if _net_rollup_baseline["psutil_total"] is None:
            _net_rollup_baseline["psutil_total"] = cur_psutil_total
        else:
            raw_delta = max(0, cur_psutil_total - _net_rollup_baseline["psutil_total"])
            _net_rollup_baseline["psutil_total"] = cur_psutil_total
            if _sample_active_tasks_count() > 0:
                delta_bytes = raw_delta
    except Exception:
        pass

    return delta_bytes


async def flush_analytics_rollups():
    """Flushes buffered hourly and daily analytics rollups to MongoDB."""
    if getattr(digital_botz, "db", None) is None:
        return

    now_dt = datetime.now(timezone.utc)
    day_key, hour_key = _utc_keys(now_dt)

    bytes_delta = await _collect_transferred_bytes_delta()
    if bytes_delta > 0:
        d_entry = _rollup_buffer["daily"].setdefault(day_key, {"renames": 0, "bytes": 0, "peak": 0})
        d_entry["bytes"] += bytes_delta
        h_entry = _rollup_buffer["hourly"].setdefault(hour_key, {"renames": 0, "bytes": 0, "peak": 0})
        h_entry["bytes"] += bytes_delta

    daily_snapshot = _rollup_buffer["daily"]
    hourly_snapshot = _rollup_buffer["hourly"]
    _rollup_buffer["daily"] = {}
    _rollup_buffer["hourly"] = {}

    for d_key, vals in daily_snapshot.items():
        if vals["renames"] == 0 and vals["bytes"] == 0 and vals["peak"] == 0:
            continue
        try:
            await digital_botz.upsert_analytics_rollup(
                collection_name="analytics_daily",
                doc_id=d_key,
                label_field="date",
                renames_delta=vals["renames"],
                bytes_delta=vals["bytes"],
                peak_tasks=vals["peak"],
                updated_at=now_dt
            )
        except Exception:
            cur = _rollup_buffer["daily"].setdefault(d_key, {"renames": 0, "bytes": 0, "peak": 0})
            cur["renames"] += vals["renames"]
            cur["bytes"] += vals["bytes"]
            cur["peak"] = max(cur["peak"], vals["peak"])

    for h_key, vals in hourly_snapshot.items():
        if vals["renames"] == 0 and vals["bytes"] == 0 and vals["peak"] == 0:
            continue
        try:
            await digital_botz.upsert_analytics_rollup(
                collection_name="analytics_hourly",
                doc_id=h_key,
                label_field="hour",
                renames_delta=vals["renames"],
                bytes_delta=vals["bytes"],
                peak_tasks=vals["peak"],
                updated_at=now_dt
            )
        except Exception:
            cur = _rollup_buffer["hourly"].setdefault(h_key, {"renames": 0, "bytes": 0, "peak": 0})
            cur["renames"] += vals["renames"]
            cur["bytes"] += vals["bytes"]
            cur["peak"] = max(cur["peak"], vals["peak"])


async def _analytics_rollup_loop():
    """Lightweight background task sampling peak tasks every 5s and flushing MongoDB rollups every 30s."""
    await _collect_transferred_bytes_delta()
    last_flush = time.time()
    last_prune = 0.0

    while True:
        try:
            await asyncio.sleep(5.0)
            active_now = _sample_active_tasks_count()
            day_key, hour_key = _utc_keys()

            d_entry = _rollup_buffer["daily"].setdefault(day_key, {"renames": 0, "bytes": 0, "peak": 0})
            if active_now > d_entry["peak"]:
                d_entry["peak"] = active_now

            h_entry = _rollup_buffer["hourly"].setdefault(hour_key, {"renames": 0, "bytes": 0, "peak": 0})
            if active_now > h_entry["peak"]:
                h_entry["peak"] = active_now

            now = time.time()
            if now - last_flush >= 30.0:
                await flush_analytics_rollups()
                last_flush = now
                _analytics_cache["timestamp"] = 0.0
                _analytics_24h_cache["timestamp"] = 0.0

            if now - last_prune >= 3600.0 and getattr(digital_botz, "db", None) is not None:
                try:
                    cutoff = (datetime.now(timezone.utc) - timedelta(days=8)).strftime("%Y-%m-%dT%H")
                    await digital_botz.prune_hourly_analytics(cutoff)
                except Exception:
                    pass
                last_prune = now

        except asyncio.CancelledError:
            await flush_analytics_rollups()
            break
        except Exception:
            await asyncio.sleep(5.0)


async def get_7d_analytics():
    """Returns 7-day daily rollup series (renames_count, gb_transferred, peak_active_tasks) with 15s cache."""
    now_ts = time.time()
    if _analytics_cache["data"] is not None and (now_ts - _analytics_cache["timestamp"]) < _analytics_cache["ttl"]:
        return _analytics_cache["data"]

    now_dt = datetime.now(timezone.utc)
    day_keys = [(now_dt - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(6, -1, -1)]
    docs_by_day = {}

    if getattr(digital_botz, "db", None) is not None:
        try:
            docs_by_day = await digital_botz.get_daily_analytics_docs(day_keys)
        except Exception:
            pass

    days_list = []
    total_renames_7d = 0
    total_bytes_7d = 0
    peak_tasks_7d = 0

    for idx, d_key in enumerate(day_keys):
        db_doc = docs_by_day.get(d_key, {})
        pending = _rollup_buffer["daily"].get(d_key, {"renames": 0, "bytes": 0, "peak": 0})

        renames_count = int(db_doc.get("renames_count", 0)) + int(pending.get("renames", 0))
        bytes_transferred = int(db_doc.get("bytes_transferred", 0)) + int(pending.get("bytes", 0))
        gb_transferred = round(bytes_transferred / (1024 ** 3), 2)
        peak_active_tasks = max(int(db_doc.get("peak_active_tasks", 0)), int(pending.get("peak", 0)))

        dt_obj = datetime.strptime(d_key, "%Y-%m-%d")
        short_label = "Today" if idx == 6 else dt_obj.strftime("%b %d")

        total_renames_7d += renames_count
        total_bytes_7d += bytes_transferred
        if peak_active_tasks > peak_tasks_7d:
            peak_tasks_7d = peak_active_tasks

        days_list.append({
            "date": d_key,
            "label": short_label,
            "renames_count": renames_count,
            "gb_transferred": gb_transferred,
            "bytes_transferred": bytes_transferred,
            "peak_active_tasks": peak_active_tasks
        })

    result = {
        "days": days_list,
        "summary": {
            "total_renames": total_renames_7d,
            "total_renames_short": format_short_number(total_renames_7d),
            "total_gb": round(total_bytes_7d / (1024 ** 3), 2),
            "peak_active_tasks": peak_tasks_7d
        }
    }

    _analytics_cache["data"] = result
    _analytics_cache["timestamp"] = now_ts
    return result


async def get_24h_analytics():
    """Returns 24-hour UTC hourly rollup series with 15s cache."""
    now_ts = time.time()
    if _analytics_24h_cache["data"] is not None and (now_ts - _analytics_24h_cache["timestamp"]) < _analytics_24h_cache["ttl"]:
        return _analytics_24h_cache["data"]

    now_dt = datetime.now(timezone.utc)
    hour_keys = [(now_dt - timedelta(hours=offset)).strftime("%Y-%m-%dT%H") for offset in range(23, -1, -1)]
    docs_by_hour = {}

    if getattr(digital_botz, "db", None) is not None:
        try:
            docs_by_hour = await digital_botz.get_hourly_analytics_docs(hour_keys)
        except Exception:
            pass

    hours_list = []
    total_renames_24h = 0
    total_bytes_24h = 0
    peak_tasks_24h = 0

    for h_key in hour_keys:
        db_doc = docs_by_hour.get(h_key, {})
        pending = _rollup_buffer["hourly"].get(h_key, {"renames": 0, "bytes": 0, "peak": 0})

        renames_count = int(db_doc.get("renames_count", 0)) + int(pending.get("renames", 0))
        bytes_transferred = int(db_doc.get("bytes_transferred", 0)) + int(pending.get("bytes", 0))
        gb_transferred = round(bytes_transferred / (1024 ** 3), 2)
        peak_active_tasks = max(int(db_doc.get("peak_active_tasks", 0)), int(pending.get("peak", 0)))

        hr_label = f"{h_key[-2:]}:00"
        display_date = f"{h_key[:10]} {hr_label} UTC"

        total_renames_24h += renames_count
        total_bytes_24h += bytes_transferred
        if peak_active_tasks > peak_tasks_24h:
            peak_tasks_24h = peak_active_tasks

        hours_list.append({
            "date": display_date,
            "hour": hr_label,
            "label": hr_label,
            "renames_count": renames_count,
            "gb_transferred": gb_transferred,
            "bytes_transferred": bytes_transferred,
            "peak_active_tasks": peak_active_tasks
        })

    result = {
        "days": hours_list,
        "summary": {
            "total_renames": total_renames_24h,
            "total_renames_short": format_short_number(total_renames_24h),
            "total_gb": round(total_bytes_24h / (1024 ** 3), 2),
            "peak_active_tasks": peak_tasks_24h
        }
    }

    _analytics_24h_cache["data"] = result
    _analytics_24h_cache["timestamp"] = now_ts
    return result


async def _build_user_plan_payload(user_id: int) -> dict:
    """Fetches daily usage, limits, and subscription status for a Telegram user."""
    premium_enabled = bool(getattr(Config, "PREMIUM_MODE", False))
    upload_limit_mode = bool(getattr(Config, "UPLOAD_LIMIT_MODE", False))

    if upload_limit_mode:
        try:
            await digital_botz.reset_uploadlimit_access(user_id)
        except Exception:
            pass

    is_premium = False
    if premium_enabled:
        try:
            is_premium = bool(await digital_botz.has_premium_access(user_id))
        except Exception:
            is_premium = False

    prem_data = await digital_botz.get_user(user_id) or {}
    user_data = await digital_botz.get_user_data(user_id) or {}

    is_lifetime = bool(prem_data.get("is_lifetime", False))
    raw_type = user_data.get("usertype", "Free") or "Free"

    if is_lifetime:
        plan_name = "Lifetime ♾️"
        expiry_formatted = "Lifetime ♾️"
        remaining_time = "Unlimited"
    elif is_premium:
        plan_name = f"{raw_type} Plan" if raw_type != "Free" else "Premium Plan"
        expiry = prem_data.get("expiry_time")
        if isinstance(expiry, datetime):
            tz = pytz.timezone("Africa/Nairobi")
            exp_eat = expiry.astimezone(tz) if expiry.tzinfo else pytz.utc.localize(expiry).astimezone(tz)
            expiry_formatted = exp_eat.strftime("%d %b %Y, %H:%M EAT")
            rem_sec = max(0, int((expiry.replace(tzinfo=None) - datetime.now()).total_seconds()))
            rem_days, rem_s = divmod(rem_sec, 86400)
            rem_hrs, rem_s = divmod(rem_s, 3600)
            rem_mins, _ = divmod(rem_s, 60)
            remaining_time = f"{rem_days}d {rem_hrs}h left" if rem_days > 0 else f"{rem_hrs}h {rem_mins}m left"
        else:
            expiry_formatted = "Active"
            remaining_time = "Active"
    else:
        plan_name = "Free Plan"
        expiry_formatted = "No Expiry"
        remaining_time = "Resets 00:00 EAT"

    used_bytes = int(user_data.get("used_limit", 0) or 0)
    limit_bytes = int(user_data.get("uploadlimit", getattr(Config, "FREE_UPLOAD_LIMIT", 0)) or 0)

    if is_lifetime or not upload_limit_mode:
        daily_limit_human = "Unlimited"
        remaining_human = "Unlimited"
        usage_pct = 0
    else:
        rem_bytes = max(0, limit_bytes - used_bytes)
        daily_limit_human = humanbytes(limit_bytes)
        remaining_human = humanbytes(rem_bytes)
        usage_pct = round(min(100.0, (used_bytes / limit_bytes) * 100), 1) if limit_bytes > 0 else 0

    has_string = bool(getattr(Config, "STRING_SESSION", None))
    max_file_size_human = "4 GB+" if (is_premium and has_string) else "2 GB"

    return {
        "plan_name": plan_name,
        "is_premium": is_premium or is_lifetime,
        "expiry_formatted": expiry_formatted,
        "remaining_time": remaining_time,
        "daily_used_human": humanbytes(used_bytes),
        "remaining_human": remaining_human,
        "daily_limit_human": daily_limit_human,
        "max_file_size_human": max_file_size_human,
        "usage_pct": usage_pct
    }


async def get_cached_db_metrics():
    """Fetches DB metrics concurrently with a 15-second TTL cache."""
    now = time.time()
    if now - _db_cache["timestamp"] >= _db_cache["ttl"] or _db_cache["timestamp"] == 0.0:
        premium_enabled = getattr(Config, 'PREMIUM_MODE', False)

        coros = [
            digital_botz.total_users_count(),
            digital_botz.get_total_renames(),
            digital_botz.get_network_stats(),
        ]
        if premium_enabled:
            coros.append(digital_botz.total_premium_users_count())

        results = await asyncio.gather(*coros, return_exceptions=True)

        if not isinstance(results[0], Exception) and results[0] is not None:
            _db_cache["real_total_users"] = results[0]
        if not isinstance(results[1], Exception) and results[1] is not None:
            _db_cache["raw_total_renames"] = results[1]
        if not isinstance(results[2], Exception) and isinstance(results[2], dict):
            _db_cache["net_stats"] = results[2]
        if premium_enabled and len(results) > 3 and not isinstance(results[3], Exception) and results[3] is not None:
            _db_cache["real_total_premium_users"] = results[3]

        _db_cache["timestamp"] = now

    return (
        _db_cache["real_total_users"],
        _db_cache["real_total_premium_users"],
        _db_cache["raw_total_renames"],
        _db_cache["net_stats"]
    )


def _build_worker_lookup():
    """Maps id(worker_client) -> readable worker label."""
    workers = getattr(Config, "WORKER_CLIENTS", [])
    lookup = {}
    for idx, w in enumerate(workers):
        w_name = getattr(w, "name", None)
        if not w_name:
            me = getattr(w, "me", None)
            w_name = f"@{me.username}" if getattr(me, "username", None) else getattr(me, "first_name", f"Worker-{idx + 1}")
        lookup[id(w)] = f"#{idx + 1} {w_name}"
    return lookup


async def get_status(pending_count=0, is_admin=False):
    """Fetches and formats system, fleet, and bot statistics."""
    global last_net_io, last_time, last_up_speed, last_dl_speed

    try:
        from plugins import file_rename
        if not getattr(file_rename, "_maintenance_loaded", False) and hasattr(file_rename, "load_persisted_maintenance_state"):
            await file_rename.load_persisted_maintenance_state()
        worker_loads = getattr(file_rename, "worker_loads", {})
        active_tasks = getattr(file_rename, "active_tasks", {})
        active_progress = getattr(file_rename, "active_progress", {})
        queue_paused = bool(getattr(file_rename, "queue_paused", False))
        maintenance_msg = getattr(file_rename, "maintenance_message", "") or ""
    except ImportError:
        worker_loads = {}
        active_tasks = {}
        active_progress = {}
        queue_paused = False
        maintenance_msg = ""

    real_total_users, real_total_premium_users, raw_total_renames, net_stats = await get_cached_db_metrics()

    if is_admin:
        total_users = real_total_users
        total_premium_users = real_total_premium_users if getattr(Config, 'PREMIUM_MODE', False) else "Disabled ✅"
    else:
        total_users = real_total_users + 1009
        total_premium_users = (real_total_premium_users + 50) if getattr(Config, 'PREMIUM_MODE', False) else "Disabled ✅"

    # --- UPTIME CALCULATIONS ---
    now = time.time()
    bot_sec = max(0, now - Config.BOT_UPTIME)
    b_days, b_rem = divmod(bot_sec, 86400)
    b_hrs, b_rem = divmod(b_rem, 3600)
    b_mins, b_secs = divmod(b_rem, 60)
    bot_uptime = f"{int(b_days)}d {int(b_hrs)}h {int(b_mins)}m" if b_days > 0 else f"{int(b_hrs):02d}h {int(b_mins):02d}m {int(b_secs):02d}s"

    sys_sec = max(0, now - psutil.boot_time())
    s_days, s_rem = divmod(sys_sec, 86400)
    s_hrs, s_rem = divmod(s_rem, 3600)
    s_mins, s_secs = divmod(s_rem, 60)
    system_uptime = f"{int(s_days)}d {int(s_hrs)}h {int(s_mins)}m" if s_days > 0 else f"{int(s_hrs):02d}h {int(s_mins):02d}m {int(s_secs):02d}s"

    total, used, free = shutil.disk_usage(".")

    # --- OVERALL BANDWIDTH SPEED TRACKER ---
    current_net_io = psutil.net_io_counters()
    current_time = time.time()
    time_delta = current_time - last_time
    if time_delta >= 1.0:
        last_up_speed = max(0.0, (current_net_io.bytes_sent - last_net_io.bytes_sent) / time_delta)
        last_dl_speed = max(0.0, (current_net_io.bytes_recv - last_net_io.bytes_recv) / time_delta)
        last_net_io = current_net_io
        last_time = current_time

    formatted_total_renames = format_short_number(raw_total_renames)
    data_sent = humanbytes(net_stats.get('sent', 0))
    data_recv = humanbytes(net_stats.get('recv', 0))

    # --- AGGREGATE ACTIVE SPEED PER WORKER ID ---
    worker_speeds = {}
    for prog in list(active_progress.values()):
        wid = prog.get("worker_id")
        if wid is not None:
            bps = prog.get("speed_bps")
            if bps is None:
                bps = parse_human_bytes(prog.get("speed", "0 B/s"))
            worker_speeds[wid] = worker_speeds.get(wid, 0.0) + float(bps)

    # --- FLEET NODE & ACTIVE TASKS MONITORING ---
    workers = getattr(Config, "WORKER_CLIENTS", [])
    total_workers = len(workers)
    healthy_workers = 0
    active_workers = 0
    worker_nodes = []

    for idx, w in enumerate(workers):
        unhealthy_until = float(getattr(w, "unhealthy_until", 0) or 0)
        is_healthy = bool(getattr(w, "is_healthy", True))
        is_disabled = bool(getattr(w, "disabled", False))

        if not is_healthy and unhealthy_until > 0 and now >= unhealthy_until:
            w.is_healthy = True
            w.unhealthy_until = 0
            is_healthy = True
            unhealthy_until = 0

        cooldown_sec = max(0, int(unhealthy_until - now)) if not is_healthy else 0
        is_connected = bool(getattr(w, "is_connected", True))
        load = int(worker_loads.get(w, 0))

        if is_healthy and not is_disabled:
            healthy_workers += 1
        if load > 0:
            active_workers += 1

        w_name = getattr(w, "name", None)
        if not w_name:
            me = getattr(w, "me", None)
            w_name = f"@{me.username}" if getattr(me, "username", None) else getattr(me, "first_name", f"Worker-{idx + 1}")

        node_bps = worker_speeds.get(id(w), 0.0)
        node_speed_str = f"{humanbytes(node_bps)}/s" if node_bps > 0 else "0 B/s"

        worker_nodes.append({
            "id": idx + 1,
            "worker_id": str(id(w)),
            "name": str(w_name),
            "healthy": is_healthy and not is_disabled,
            "disabled": is_disabled,
            "connected": is_connected,
            "load": load,
            "cooldown_sec": cooldown_sec,
            "floodwait_count": int(getattr(w, "floodwait_count", 0)),
            "floodwait_total_sec": int(getattr(w, "floodwait_total_sec", 0)),
            "speed": node_speed_str,
            "speed_bps": round(node_bps, 2),
            "status": "Disabled" if is_disabled else ("Rate-Limited" if not is_healthy else ("Busy" if load > 0 else "Idle"))
        })

    free_workers = max(0, healthy_workers - active_workers)
    current_active_tasks = len(active_tasks)

    active_user_ids = {
        p.get("user_id") for p in list(active_progress.values())
        if p.get("user_id") is not None
    }
    active_batches = len(active_user_ids) if active_user_ids else current_active_tasks

    up_speed_str = f"{humanbytes(last_up_speed)}/s" if last_up_speed > 0 else "0 B/s"
    dl_speed_str = f"{humanbytes(last_dl_speed)}/s" if last_dl_speed > 0 else "0 B/s"

    payload = {
        "bot_status": "Paused (Draining)" if queue_paused else "Operational",
        "queue_paused": queue_paused,
        "is_admin": is_admin,
        "bot_version": __version__,
        "total_users": total_users,
        "premium_users": total_premium_users,
        "bot_uptime": bot_uptime,
        "system_uptime": system_uptime,
        "cpu_usage": round(psutil.cpu_percent(), 1),
        "ram_usage": round(psutil.virtual_memory().percent, 1),
        "disk_usage": round(psutil.disk_usage('.').percent, 1),
        "total_disk": humanbytes(total),
        "used_disk": humanbytes(used),
        "free_disk": humanbytes(free),
        "data_sent": data_sent,
        "data_recv": data_recv,
        "up_speed": up_speed_str,
        "dl_speed": dl_speed_str,
        "up_speed_bps": round(last_up_speed, 2),
        "dl_speed_bps": round(last_dl_speed, 2),
        "total_renames": formatted_total_renames,
        "total_workers": total_workers,
        "healthy_workers": healthy_workers,
        "active_workers": active_workers,
        "free_workers": free_workers,
        "worker_nodes": worker_nodes,
        "active_tasks": current_active_tasks,
        "active_batches": active_batches,
        "pending_tasks": pending_count,
        "timestamp": int(time.time()),
        "github_link": "https://github.com/DigitalBotz/Digital-Rename-Bot",
        "telegram_link": "https://t.me/OtherBs"
    }

    if is_admin or queue_paused:
        payload["maintenance_message"] = maintenance_msg

    if is_admin:
        payload["broadcast"] = {
            "running": _broadcast_state["running"],
            "sent": _broadcast_state["sent"],
            "failed": _broadcast_state["failed"],
            "total": _broadcast_state["total"],
            "has_media": _broadcast_state.get("has_media", False),
            "parse_mode": _broadcast_state.get("parse_mode", "html")
        }

    return payload


async def get_queue_data(viewer_user_id=None, is_admin=False):
    """Exposes live split data for Downloads, Uploads, and Pending Tasks with viewer privacy masking."""
    try:
        from helper.database import Task
        from plugins.file_rename import active_progress
    except ImportError:
        return {"success": False, "error": "Cannot load required modules", "downloading": [], "uploading": [], "pending": []}

    def _apply_privacy(item: dict, owner_id):
        is_owner = (viewer_user_id is not None and owner_id is not None and int(owner_id) == int(viewer_user_id))
        raw_name = item.get("filename") or "Unknown File"
        item["extension"] = extract_extension(raw_name)
        item["is_owner"] = is_owner

        if is_admin or is_owner:
            item["filename"] = raw_name
            item["is_masked"] = False
            if is_admin and owner_id is not None:
                item["user_id"] = str(owner_id)
            else:
                item.pop("user_id", None)
        else:
            item["filename"] = mask_filename(raw_name)
            item["is_masked"] = True
            item.pop("user_id", None)
            item.pop("task_id", None)
        return item

    try:
        active_db_tasks = await Task.find_all().to_list()
        worker_lookup = _build_worker_lookup()

        downloading = []
        uploading = []
        pending = []
        seen_ids = set()

        for task in active_db_tasks:
            tid = str(task.id)
            seen_ids.add(tid)
            owner_id = getattr(task, "user_id", None)

            if tid in active_progress:
                prog = dict(active_progress[tid])
                fname = (prog.get("filename") or getattr(task, "new_name", None) or "Unknown File").strip()
                prog["filename"] = fname
                prog["task_id"] = tid
                prog["worker_name"] = worker_lookup.get(prog.get("worker_id"), "Primary Bot")
                prog["eta"] = format_eta(
                    prog.get("current", "0 B"),
                    prog.get("total", "0 B"),
                    prog.get("speed", "0 B/s")
                )
                _apply_privacy(prog, prog.get("user_id") or owner_id)
                if prog.get("status") == "Downloading":
                    downloading.append(prog)
                else:
                    uploading.append(prog)
            else:
                fname = (getattr(task, "new_name", None) or "").strip()
                if not fname or fname == ".":
                    continue
                if getattr(task, "id", None) and hasattr(task.id, "generation_time"):
                    age_sec = (datetime.now(timezone.utc) - task.id.generation_time).total_seconds()
                    if age_sec > 1800:
                        continue

                raw_size = getattr(task, "file_size", 0)
                fsize = humanbytes(raw_size) if raw_size else ""
                item = {
                    "filename": fname,
                    "task_id": tid,
                    "size": fsize,
                    "status": "Pending"
                }
                _apply_privacy(item, owner_id)
                pending.append(item)

        for tid, prog in list(active_progress.items()):
            if tid not in seen_ids:
                item = dict(prog)
                fname = (item.get("filename") or "Unknown File").strip()
                item["filename"] = fname
                item["task_id"] = tid
                item["worker_name"] = worker_lookup.get(item.get("worker_id"), "Primary Bot")
                item["eta"] = format_eta(
                    item.get("current", "0 B"),
                    item.get("total", "0 B"),
                    item.get("speed", "0 B/s")
                )
                _apply_privacy(item, item.get("user_id"))
                if item.get("status") == "Downloading":
                    downloading.append(item)
                else:
                    uploading.append(item)

        return {
            "success": True,
            "downloading": downloading,
            "uploading": uploading,
            "pending": pending
        }
    except Exception as e:
        return {"success": False, "error": str(e), "downloading": [], "uploading": [], "pending": []}


# ==========================================
# --- FORMATTED & MEDIA BROADCAST HELPERS ---
# ==========================================
def _resolve_parse_mode(mode_str: str):
    if ParseMode is None:
        return None
    m = (mode_str or "html").strip().lower()
    if m == "html":
        return ParseMode.HTML
    if m in ("markdown", "md"):
        return ParseMode.MARKDOWN
    if m in ("plain", "none", "disabled", "text"):
        return ParseMode.DISABLED
    return ParseMode.HTML


def _detect_media_kind(media_type: str, filename: str = "", media_url: str = "", content_type: str = "") -> str:
    mt = (media_type or "auto").strip().lower()
    if mt in ("photo", "video", "animation", "audio", "document"):
        return mt
    ct = (content_type or "").lower()
    if "gif" in ct:
        return "animation"
    if ct.startswith("image/"):
        return "photo"
    if ct.startswith("video/"):
        return "video"
    if ct.startswith("audio/"):
        return "audio"

    probe = (filename or media_url or "").split("?")[0].lower()
    ext = probe.rsplit(".", 1)[-1] if "." in probe else ""
    if ext in ("jpg", "jpeg", "png", "webp"):
        return "photo"
    if ext == "gif":
        return "animation"
    if ext in ("mp4", "mkv", "mov", "webm", "avi", "m4v"):
        return "video"
    if ext in ("mp3", "m4a", "ogg", "oga", "wav", "flac", "aac", "opus"):
        return "audio"
    if filename:
        return "document"
    return "photo"


def _extract_msg_media_info(msg):
    """Extracts (media_kind, file_id) from any Pyrogram/wzgram Message containing media."""
    if not msg:
        return None, None
    if getattr(msg, "photo", None):
        return "photo", msg.photo.file_id
    if getattr(msg, "video", None):
        return "video", msg.video.file_id
    if getattr(msg, "animation", None):
        return "animation", msg.animation.file_id
    if getattr(msg, "audio", None):
        return "audio", msg.audio.file_id
    if getattr(msg, "document", None):
        return "document", msg.document.file_id
    if getattr(msg, "voice", None):
        return "voice", msg.voice.file_id
    if getattr(msg, "sticker", None):
        return "sticker", msg.sticker.file_id
    return None, None


def _extract_cached_file_id(sent_msg, media_kind: str):
    if not sent_msg:
        return None
    if media_kind == "photo" and getattr(sent_msg, "photo", None):
        return sent_msg.photo.file_id
    if media_kind == "video" and getattr(sent_msg, "video", None):
        return sent_msg.video.file_id
    if media_kind == "animation" and getattr(sent_msg, "animation", None):
        return sent_msg.animation.file_id
    if media_kind == "audio" and getattr(sent_msg, "audio", None):
        return sent_msg.audio.file_id
    if getattr(sent_msg, "document", None):
        return sent_msg.document.file_id
    _, fid = _extract_msg_media_info(sent_msg)
    return fid


async def _download_remote_media_to_temp(url: str):
    """Downloads an external HTTP(S) media URL into BroadcastTemp/ when Telegram cannot scrape it directly."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; DigitalRenameBot/2.0)"}
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status >= 400:
                raise ValueError(f"Remote media URL returned HTTP {resp.status}")

            ct = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" in ct:
                raise ValueError("URL points to an HTML webpage instead of a direct media file.")

            url_path = url.split("?")[0]
            ext = os.path.splitext(url_path)[1]
            if not ext:
                if "jpeg" in ct or "jpg" in ct:
                    ext = ".jpg"
                elif "png" in ct:
                    ext = ".png"
                elif "webp" in ct:
                    ext = ".webp"
                elif "gif" in ct:
                    ext = ".gif"
                elif "mp4" in ct or "video/" in ct:
                    ext = ".mp4"
                elif "mpeg" in ct or "mp3" in ct or "audio/" in ct:
                    ext = ".mp3"
                else:
                    ext = ".bin"

            os.makedirs("BroadcastTemp", exist_ok=True)
            temp_path = os.path.join("BroadcastTemp", f"url_{uuid.uuid4().hex}{ext}")
            written = 0
            try:
                with open(temp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(65536):
                        written += len(chunk)
                        if written > MAX_BROADCAST_UPLOAD_BYTES:
                            raise ValueError("Remote media exceeds 28 MB limit.")
                        f.write(chunk)
            except Exception:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
                raise

            if written == 0:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
                raise ValueError("Remote media URL returned an empty file.")

            return temp_path, ct


async def _prepare_broadcast_media(
    bot_client,
    media_path: str = None,
    media_url: str = "",
    media_type: str = "auto",
    media_name: str = ""
) -> dict:
    """
    Normalizes uploaded files, Telegram post links (https://t.me/.../123),
    direct HTTP(S) URLs, or Telegram file_ids into a unified media descriptor.
    """
    spec = {
        "source": None,
        "kind": "photo",
        "copy_ref": None,
        "temp_files": []
    }

    if media_path and os.path.exists(media_path):
        spec["source"] = media_path
        spec["kind"] = _detect_media_kind(media_type, filename=media_name or media_path)
        spec["temp_files"].append(media_path)
        return spec

    clean_url = (media_url or "").strip()
    if not clean_url:
        return spec

    # Handle Telegram post links like https://t.me/ChannelName/228 or https://t.me/c/123456789/228
    tg_match = TG_POST_LINK_RE.match(clean_url)
    if tg_match:
        private_cid, public_uname, msg_id_str = tg_match.groups()
        src_chat = int(f"-100{private_cid}") if private_cid else public_uname
        src_msg_id = int(msg_id_str)
        spec["copy_ref"] = (src_chat, src_msg_id)

        try:
            src_msg = await bot_client.get_messages(src_chat, src_msg_id)
            if not src_msg or getattr(src_msg, "empty", False):
                raise ValueError("Telegram post not found or bot lacks access to that channel.")

            detected_kind, file_id = _extract_msg_media_info(src_msg)
            if not file_id:
                raise ValueError("The linked Telegram post does not contain any media.")

            spec["source"] = file_id
            spec["kind"] = detected_kind if media_type == "auto" else _detect_media_kind(media_type)
        except ValueError:
            raise
        except Exception:
            # Even if get_messages cannot read file_id directly, copy_message can still copy public posts
            spec["source"] = None
            spec["kind"] = _detect_media_kind(media_type)

        return spec

    # Direct external HTTP(S) URL or raw Telegram file_id
    spec["source"] = clean_url
    spec["kind"] = _detect_media_kind(media_type, filename=media_name, media_url=clean_url)
    return spec


async def _send_broadcast_to_chat(
    bot_client,
    chat_id: int,
    text_msg: str,
    parse_mode=None,
    media_spec: dict = None,
    media_type_override: str = "auto",
    media_name: str = "",
    disable_preview: bool = False
):
    """Sends a formatted text or rich-media broadcast message to a single Telegram chat."""
    clean_text = text_msg or ""
    has_media = bool(media_spec and (media_spec.get("source") or media_spec.get("copy_ref")))

    if not has_media:
        return await bot_client.send_message(
            chat_id,
            clean_text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_preview
        )

    use_caption = clean_text if (clean_text and len(clean_text) <= 1024) else None
    send_followup_text = bool(clean_text and len(clean_text) > 1024)
    media_source = media_spec.get("source")
    media_kind = media_spec.get("kind", "photo")
    copy_ref = media_spec.get("copy_ref")

    async def _dispatch_media(src, kind: str):
        if kind == "photo":
            return await bot_client.send_photo(
                chat_id,
                photo=src,
                caption=use_caption,
                parse_mode=parse_mode
            )
        elif kind == "video":
            return await bot_client.send_video(
                chat_id,
                video=src,
                caption=use_caption,
                parse_mode=parse_mode
            )
        elif kind == "animation":
            return await bot_client.send_animation(
                chat_id,
                animation=src,
                caption=use_caption,
                parse_mode=parse_mode
            )
        elif kind == "audio":
            kwargs = {"audio": src, "caption": use_caption, "parse_mode": parse_mode}
            if media_name:
                kwargs["file_name"] = media_name
            return await bot_client.send_audio(chat_id, **kwargs)
        else:
            kwargs = {"document": src, "caption": use_caption, "parse_mode": parse_mode}
            if media_name:
                kwargs["file_name"] = media_name
            return await bot_client.send_document(chat_id, **kwargs)

    async def _dispatch_copy():
        src_chat, src_msg_id = copy_ref
        copy_kwargs = {
            "chat_id": chat_id,
            "from_chat_id": src_chat,
            "message_id": src_msg_id,
        }
        if use_caption is not None:
            copy_kwargs["caption"] = use_caption
            copy_kwargs["parse_mode"] = parse_mode
        return await bot_client.copy_message(**copy_kwargs)

    sent = None
    if copy_ref and not media_source:
        sent = await _dispatch_copy()
    else:
        try:
            sent = await _dispatch_media(media_source, media_kind)
        except (FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
            raise
        except Exception:
            if copy_ref:
                sent = await _dispatch_copy()
                media_spec["source"] = None
            elif isinstance(media_source, str) and media_source.startswith(("http://", "https://")):
                # Fallback: download remote HTTP(S) media locally when Telegram cannot scrape the URL directly
                dl_path, dl_ct = await _download_remote_media_to_temp(media_source)
                media_spec.setdefault("temp_files", []).append(dl_path)
                detected_kind = _detect_media_kind(media_type_override, filename=dl_path, content_type=dl_ct)
                media_spec["source"] = dl_path
                media_spec["kind"] = detected_kind
                try:
                    sent = await _dispatch_media(dl_path, detected_kind)
                except Exception:
                    media_spec["kind"] = "document"
                    sent = await _dispatch_media(dl_path, "document")
            elif media_kind != "document" and isinstance(media_source, str) and os.path.exists(media_source):
                media_spec["kind"] = "document"
                sent = await _dispatch_media(media_source, "document")
            else:
                raise

    # Cache Telegram file_id on first successful send for fast bulk delivery
    if sent and not copy_ref:
        cached_fid = _extract_cached_file_id(sent, media_spec.get("kind", media_kind))
        if cached_fid:
            media_spec["source"] = cached_fid

    if send_followup_text:
        await bot_client.send_message(
            chat_id,
            clean_text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_preview
        )

    return sent


async def _run_web_broadcast(
    text_msg: str,
    parse_mode_str: str = "html",
    media_path: str = None,
    media_url: str = "",
    media_type: str = "auto",
    media_name: str = "",
    disable_preview: bool = False
):
    """Background broadcast loop triggered from the Telegram Mini App Admin Console."""
    _broadcast_state["running"] = True
    _broadcast_state["sent"] = 0
    _broadcast_state["failed"] = 0
    _broadcast_state["total"] = 0
    _broadcast_state["has_media"] = bool(media_path or media_url)
    _broadcast_state["parse_mode"] = parse_mode_str or "html"

    resolved_parse_mode = _resolve_parse_mode(parse_mode_str)
    staged_log_msg = None
    bot_client = None
    media_spec = {"temp_files": [media_path] if media_path else []}

    try:
        bot_client = _get_bot_client()
        if bot_client is None:
            return

        media_spec = await _prepare_broadcast_media(
            bot_client=bot_client,
            media_path=media_path,
            media_url=media_url,
            media_type=media_type,
            media_name=media_name
        )

        # Stage local media once in LOG_CHANNEL to reuse Telegram file_id across all recipients
        src = media_spec.get("source")
        if src and isinstance(src, str) and os.path.exists(src) and getattr(Config, "LOG_CHANNEL", None):
            try:
                staged_log_msg = await _send_broadcast_to_chat(
                    bot_client,
                    Config.LOG_CHANNEL,
                    text_msg="",
                    parse_mode=None,
                    media_spec=media_spec,
                    media_type_override=media_type,
                    media_name=media_name
                )
            except Exception:
                pass

        all_users = await digital_botz.get_all_users()
        _broadcast_state["total"] = len(all_users)

        for user in all_users:
            if not _broadcast_state["running"]:
                break
            uid = user.get("_id", user.get("id"))
            if not uid:
                continue
            try:
                await _send_broadcast_to_chat(
                    bot_client,
                    int(uid),
                    text_msg=text_msg,
                    parse_mode=resolved_parse_mode,
                    media_spec=media_spec,
                    media_type_override=media_type,
                    media_name=media_name,
                    disable_preview=disable_preview
                )
                _broadcast_state["sent"] += 1
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
                try:
                    await _send_broadcast_to_chat(
                        bot_client,
                        int(uid),
                        text_msg=text_msg,
                        parse_mode=resolved_parse_mode,
                        media_spec=media_spec,
                        media_type_override=media_type,
                        media_name=media_name,
                        disable_preview=disable_preview
                    )
                    _broadcast_state["sent"] += 1
                except Exception:
                    _broadcast_state["failed"] += 1
            except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
                _broadcast_state["failed"] += 1
                try:
                    await digital_botz.delete_user(int(uid))
                except Exception:
                    pass
            except Exception:
                _broadcast_state["failed"] += 1

            await asyncio.sleep(0.05)
    except asyncio.CancelledError:
        pass
    finally:
        if staged_log_msg and bot_client and getattr(Config, "LOG_CHANNEL", None):
            try:
                await bot_client.delete_messages(Config.LOG_CHANNEL, staged_log_msg.id)
            except Exception:
                pass
        for t_path in media_spec.get("temp_files", []):
            if t_path and os.path.exists(t_path):
                try:
                    os.remove(t_path)
                except Exception:
                    pass
        _broadcast_state["running"] = False
        _broadcast_state["task"] = None


def _render_html_template(template_path: str, status_data: dict, missing_msg: str) -> web.Response:
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        return web.Response(text=missing_msg, content_type='text/plain')

    for key, value in status_data.items():
        if not isinstance(value, (list, dict)):
            html_content = html_content.replace(f'{{{{{key}}}}}', str(value))

    return web.Response(text=html_content, content_type='text/html')


@DigitalRenameBot.get("/", allow_head=True)
async def root_route_handler(request):
    queue_data = await get_queue_data()
    status_data = await get_status(pending_count=len(queue_data.get("pending", [])))
    return _render_html_template(
        'templates/welcome.html',
        status_data,
        "API is Operational. (templates/welcome.html is missing)"
    )


@DigitalRenameBot.get("/admin", allow_head=True)
async def admin_route_handler(request):
    """Dedicated Admin Console page opened from the dashboard's Admin Console button."""
    queue_data = await get_queue_data()
    status_data = await get_status(pending_count=len(queue_data.get("pending", [])))
    return _render_html_template(
        'templates/admin.html',
        status_data,
        "Admin Console template missing. (templates/admin.html is missing)"
    )


@DigitalRenameBot.get("/api/status")
async def api_status_handler(request):
    user_obj = _extract_user_from_request(request)
    is_admin = bool(user_obj and int(user_obj.get("id", 0)) in _get_admin_ids())
    viewer_id = int(user_obj["id"]) if user_obj else None

    queue_data = await get_queue_data(viewer_user_id=viewer_id, is_admin=is_admin)
    status_data = await get_status(pending_count=len(queue_data.get("pending", [])), is_admin=is_admin)
    return web.json_response(status_data)


@DigitalRenameBot.get("/api/queue")
async def api_queue_handler(request):
    user_obj = _extract_user_from_request(request)
    is_admin = bool(user_obj and int(user_obj.get("id", 0)) in _get_admin_ids())
    viewer_id = int(user_obj["id"]) if user_obj else None

    queue_data = await get_queue_data(viewer_user_id=viewer_id, is_admin=is_admin)
    return web.json_response(queue_data)


@DigitalRenameBot.get("/api/user/plan")
async def api_user_plan_handler(request):
    user_obj = _extract_user_from_request(request)
    if not user_obj:
        return web.json_response({"authenticated": False})

    user_id = int(user_obj["id"])
    plan_payload = await _build_user_plan_payload(user_id)
    return web.json_response({
        "authenticated": True,
        "first_name": user_obj.get("first_name", ""),
        **plan_payload
    })


@DigitalRenameBot.get("/api/analytics/7d")
async def api_analytics_7d_handler(request):
    analytics_data = await get_7d_analytics()
    return web.json_response(analytics_data)


@DigitalRenameBot.get("/api/analytics/24h")
async def api_analytics_24h_handler(request):
    analytics_data = await get_24h_analytics()
    return web.json_response(analytics_data)


@DigitalRenameBot.post("/api/admin/verify")
async def api_admin_verify_handler(request):
    """Verifies Telegram Mini App initData and unlocks admin UI if user is in Config.ADMIN."""
    admin_user = _extract_admin_from_request(request)
    if not admin_user:
        return web.json_response({"authorized": False}, status=403)
    return web.json_response({
        "authorized": True,
        "user": {
            "id": admin_user.get("id"),
            "first_name": admin_user.get("first_name", "Admin"),
            "username": admin_user.get("username", "")
        }
    })


@DigitalRenameBot.get("/api/admin/user")
async def api_admin_user_lookup_handler(request):
    """Admin user lookup by Telegram ID or @username."""
    admin_user = _extract_admin_from_request(request)
    if not admin_user:
        return web.json_response({"success": False, "error": "Unauthorized Telegram session"}, status=403)

    query = request.query.get("q", "").strip()
    if not query:
        return web.json_response({"success": False, "error": "Missing user query"}, status=400)

    user_doc = await digital_botz.find_user_by_query(query)
    if not user_doc:
        return web.json_response({"success": False, "error": "User not found in database"}, status=404)

    uid = int(user_doc["_id"])
    plan_info = await _build_user_plan_payload(uid)
    ban_status = user_doc.get("ban_status", {})

    try:
        from helper.database import Task
        from plugins.file_rename import active_progress
        active_count = sum(1 for p in active_progress.values() if int(p.get("user_id") or 0) == uid)
        queued_count = await Task.find(Task.user_id == uid).count()
    except Exception:
        active_count = 0
        queued_count = 0

    return web.json_response({
        "success": True,
        "user": {
            "user_id": uid,
            "first_name": user_doc.get("first_name") or f"User {uid}",
            "username": user_doc.get("username") or "",
            "is_banned": bool(ban_status.get("is_banned", False)),
            "plan": plan_info,
            "active_tasks": active_count,
            "queued_tasks": queued_count
        }
    })


@DigitalRenameBot.post("/api/admin/broadcast")
async def api_admin_broadcast_handler(request):
    """Handles formatted text & media (file upload, Telegram post link, URL, or file_id) broadcasts."""
    admin_user = _extract_admin_from_request(request)
    if not admin_user:
        return web.json_response({"success": False, "error": "Unauthorized Telegram session"}, status=403)

    msg = ""
    parse_mode_str = "html"
    media_type = "auto"
    media_url = ""
    media_path = None
    media_name = ""
    test_only = False
    disable_preview = False

    content_type = (request.content_type or "").lower()

    try:
        if "multipart/" in content_type:
            reader = await request.multipart()
            while True:
                part = await reader.next()
                if part is None:
                    break
                field_name = part.name or ""
                if field_name == "media" and getattr(part, "filename", None):
                    raw_fname = os.path.basename(part.filename).strip()
                    if raw_fname:
                        media_name = raw_fname
                        os.makedirs("BroadcastTemp", exist_ok=True)
                        temp_path = os.path.join("BroadcastTemp", f"{uuid.uuid4().hex}_{raw_fname}")
                        written = 0
                        with open(temp_path, "wb") as f:
                            while True:
                                chunk = await part.read_chunk(65536)
                                if not chunk:
                                    break
                                written += len(chunk)
                                if written > MAX_BROADCAST_UPLOAD_BYTES:
                                    raise ValueError("Uploaded media exceeds 28 MB limit.")
                                f.write(chunk)
                        if written > 0:
                            media_path = temp_path
                        else:
                            try:
                                os.remove(temp_path)
                            except Exception:
                                pass
                else:
                    val = await part.text()
                    if field_name == "message":
                        msg = val.strip()
                    elif field_name == "parse_mode":
                        parse_mode_str = val.strip() or "html"
                    elif field_name == "media_type":
                        media_type = val.strip() or "auto"
                    elif field_name == "media_url":
                        media_url = val.strip()
                    elif field_name == "test_only":
                        test_only = val.strip().lower() in ("1", "true", "yes", "on")
                    elif field_name == "disable_preview":
                        disable_preview = val.strip().lower() in ("1", "true", "yes", "on")
        else:
            body = await request.json()
            msg = str(body.get("message", "")).strip()
            parse_mode_str = str(body.get("parse_mode", "html")).strip() or "html"
            media_type = str(body.get("media_type", "auto")).strip() or "auto"
            media_url = str(body.get("media_url", "")).strip()
            test_only = bool(body.get("test_only", False))
            disable_preview = bool(body.get("disable_preview", False))
    except Exception as e:
        if media_path and os.path.exists(media_path):
            try:
                os.remove(media_path)
            except Exception:
                pass
        return web.json_response({"success": False, "error": str(e)}, status=400)

    has_media = bool(media_path or media_url)
    if not msg and not has_media:
        return web.json_response({"success": False, "error": "Provide a broadcast message or attach media."}, status=400)

    # Admin DM test preview before sending to all users
    if test_only:
        media_spec = {"temp_files": [media_path] if media_path else []}
        try:
            bot_client = _get_bot_client()
            if bot_client is None:
                return web.json_response({"success": False, "error": "Bot client is not initialized yet."}, status=503)

            resolved_pm = _resolve_parse_mode(parse_mode_str)
            media_spec = await _prepare_broadcast_media(
                bot_client=bot_client,
                media_path=media_path,
                media_url=media_url,
                media_type=media_type,
                media_name=media_name
            )

            await _send_broadcast_to_chat(
                bot_client,
                int(admin_user["id"]),
                text_msg=msg,
                parse_mode=resolved_pm,
                media_spec=media_spec,
                media_type_override=media_type,
                media_name=media_name,
                disable_preview=disable_preview
            )
            return web.json_response({"success": True, "message": "Test preview sent to your Telegram DM!"})
        except Exception as e:
            return web.json_response({"success": False, "error": f"Formatting/Media error: {e}"}, status=400)
        finally:
            for t_path in media_spec.get("temp_files", []):
                if t_path and os.path.exists(t_path):
                    try:
                        os.remove(t_path)
                    except Exception:
                        pass

    if _broadcast_state["running"]:
        if media_path and os.path.exists(media_path):
            try:
                os.remove(media_path)
            except Exception:
                pass
        return web.json_response({"success": False, "error": "A broadcast is already running"}, status=409)

    _broadcast_state["task"] = asyncio.create_task(
        _run_web_broadcast(
            text_msg=msg,
            parse_mode_str=parse_mode_str,
            media_path=media_path,
            media_url=media_url,
            media_type=media_type,
            media_name=media_name,
            disable_preview=disable_preview
        )
    )
    return web.json_response({"success": True, "message": "Global broadcast started."})


@DigitalRenameBot.post("/api/admin/action")
async def api_admin_action_handler(request):
    """Executes authenticated Telegram Mini App admin commands."""
    admin_user = _extract_admin_from_request(request)
    if not admin_user:
        return web.json_response({"success": False, "error": "Unauthorized Telegram session"}, status=403)

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"success": False, "error": "Invalid JSON payload"}, status=400)

    action = body.get("action")

    if action == "purge_queue":
        try:
            from helper.database import Task
            from plugins.file_rename import active_tasks, active_progress

            running_ids = set(active_tasks.keys())
            all_tasks = await Task.find_all().to_list()
            deleted_count = 0

            for t in all_tasks:
                if str(t.id) not in running_ids:
                    await t.delete()
                    deleted_count += 1

            for pid in list(active_progress.keys()):
                if pid not in running_ids:
                    del active_progress[pid]

            return web.json_response({
                "success": True,
                "message": f"Purged {deleted_count} stuck/orphaned task(s) from database."
            })
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    elif action == "heal_workers":
        try:
            workers = getattr(Config, "WORKER_CLIENTS", [])
            healed = 0
            reconnected = 0

            for w in workers:
                if not getattr(w, "is_healthy", True) or getattr(w, "unhealthy_until", 0) > 0 or getattr(w, "disabled", False):
                    healed += 1
                w.is_healthy = True
                w.unhealthy_until = 0
                w.disabled = False
                if not getattr(w, "is_connected", True):
                    try:
                        await w.connect()
                        reconnected += 1
                    except Exception:
                        pass

            return web.json_response({
                "success": True,
                "message": f"Healed {healed} node(s) & reconnected {reconnected} session(s)."
            })
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    elif action == "toggle_pause":
        try:
            from plugins import file_rename
            new_paused = not bool(getattr(file_rename, "queue_paused", False))
            persisted = await digital_botz.set_maintenance_state(queue_paused=new_paused)
            file_rename.queue_paused = bool(persisted.get("queue_paused", new_paused))
            state_label = "PAUSED (draining active tasks)" if file_rename.queue_paused else "RESUMED (accepting files)"
            return web.json_response({
                "success": True,
                "queue_paused": file_rename.queue_paused,
                "message": f"Queue intake is now {state_label}."
            })
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    elif action == "set_maintenance_message":
        try:
            from plugins import file_rename
            new_msg = str(body.get("message", "")).strip()
            persisted = await digital_botz.set_maintenance_state(maintenance_message=new_msg)
            file_rename.maintenance_message = str(persisted.get("maintenance_message", new_msg))
            return web.json_response({
                "success": True,
                "maintenance_message": file_rename.maintenance_message,
                "message": "Maintenance notice saved & persisted." if file_rename.maintenance_message else "Maintenance notice cleared."
            })
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    elif action == "user_manage":
        try:
            uid = int(body.get("user_id", 0))
            op = body.get("op")
            if not uid:
                return web.json_response({"success": False, "error": "Invalid user ID"}, status=400)

            admin_id = int(admin_user.get("id", 0))
            admin_first = admin_user.get("first_name", "Admin")
            admin_mention = f"[{admin_first}](tg://user?id={admin_id})" if admin_id else "Admin"

            bot_client = _get_bot_client()
            user_mention = f"[User {uid}](tg://user?id={uid})"
            if bot_client:
                try:
                    tg_target = await bot_client.get_users(uid)
                    user_mention = tg_target.mention
                except Exception:
                    pass

            tz_eat = pytz.timezone("Africa/Nairobi")
            now_eat = datetime.now(tz_eat)
            current_time_str = now_eat.strftime("%d-%m-%Y\n⏱️ ᴊᴏɪɴɪɴɢ ᴛɪᴍᴇ : %I:%M:%S %p")

            if op == "grant_premium":
                days = int(body.get("days", 30))
                plan_type = str(body.get("plan_type", "UltraPro")).strip() or "UltraPro"
                if plan_type not in ("Pro", "UltraPro"):
                    plan_type = "UltraPro"
                limit_bytes = 107374182400 if plan_type == "Pro" else 1073741824000

                if days <= 0:
                    expiry_time = None
                    is_lifetime = True
                    time_label = "Lifetime ♾️"
                else:
                    expiry_time = datetime.now() + timedelta(days=days)
                    is_lifetime = False
                    time_label = f"{days} day{'s' if days != 1 else ''}"

                user_data = {"id": uid, "expiry_time": expiry_time, "is_lifetime": is_lifetime}
                if hasattr(digital_botz, "addpremium"):
                    await digital_botz.addpremium(uid, user_data, limit_bytes, plan_type)
                else:
                    await digital_botz.add_premium(uid, user_data, limit=limit_bytes, type=plan_type)

                db_u = await digital_botz.get_user_data(uid) or {}
                applied_limit = int(db_u.get("uploadlimit", limit_bytes) or limit_bytes)
                applied_type = db_u.get("usertype", plan_type) or plan_type

                prem_doc = await digital_botz.get_user(uid) or {}
                expiry_val = prem_doc.get("expiry_time", expiry_time)
                if prem_doc.get("is_lifetime", is_lifetime):
                    expiry_str = "Lifetime ♾️"
                elif isinstance(expiry_val, datetime):
                    exp_local = expiry_val.astimezone(tz_eat) if expiry_val.tzinfo else pytz.utc.localize(expiry_val).astimezone(tz_eat)
                    expiry_str = exp_local.strftime("%d-%m-%Y\n⏱️ ᴇxᴘɪʀʏ ᴛɪᴍᴇ : %I:%M:%S %p")
                else:
                    expiry_str = str(expiry_val)

                user_dm = (
                    f"👋 ʜᴇʏ {user_mention},\n"
                    f"ᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴘᴜʀᴄʜᴀꜱɪɴɢ ᴘʀᴇᴍɪᴜᴍ.\n"
                    f"ᴇɴᴊᴏʏ !! ✨🎉\n\n"
                    f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : <code>{time_label}</code>\n"
                    f"ᴘʟᴀɴ :- `{applied_type}`\n"
                    f"ᴅᴀɪʟʏ ᴜᴘʟᴏᴀᴅ ʟɪᴍɪᴛ :- `{humanbytes(applied_limit)}`\n"
                    f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {current_time_str}\n\n"
                    f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str}"
                )

                def _admin_dm(notified: bool):
                    notify_note = "✅ User notified via DM." if notified else "⚠️ User DM failed (blocked/unreachable)."
                    return (
                        f"ᴘʀᴇᴍɪᴜᴍ ᴀᴅᴅᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ✅ (Mini App)\n\n"
                        f"👤 ᴜꜱᴇʀ : {user_mention}\n"
                        f"⚡ ᴜꜱᴇʀ ɪᴅ : <code>{uid}</code>\n"
                        f"ᴘʟᴀɴ :- `{applied_type}`\n"
                        f"ᴅᴀɪʟʏ ᴜᴘʟᴏᴀᴅ ʟɪᴍɪᴛ :- `{humanbytes(applied_limit)}`\n"
                        f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : <code>{time_label}</code>\n\n"
                        f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {current_time_str}\n\n"
                        f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_str}\n\n"
                        f"{notify_note}"
                    )

                notified = await _notify_user_and_admin(
                    bot_client,
                    admin_id,
                    uid,
                    user_dm,
                    _admin_dm,
                    log_text=f"#AdminAction #GrantPremium\nAdmin: {admin_mention} (`{admin_id}`)\nUser: {user_mention} (`{uid}`)\nPlan: `{applied_type}` ({time_label})"
                )
                _db_cache["timestamp"] = 0.0
                return web.json_response({
                    "success": True,
                    "message": f"Granted {applied_type} ({time_label}) to {uid}. {'User & admin notified.' if notified else 'Admin notified (user DM failed).'}"
                })

            elif op == "revoke_premium":
                await digital_botz.remove_premium(uid)
                user_dm = (
                    f"**ʜᴇʏ {user_mention},\n\n"
                    f"✨ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ ʜᴀs ʙᴇᴇɴ ʀᴇᴍᴏᴠᴇᴅ ғʀᴏᴍ ᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴ\n\n"
                    f"ᴄʜᴇᴄᴋ ʏᴏᴜʀ ᴘʟᴀɴ ʜᴇʀᴇ /myplan**"
                )

                def _admin_dm(notified: bool):
                    notify_note = "✅ User notified via DM." if notified else "⚠️ User DM failed (blocked/unreachable)."
                    return (
                        f"ʜᴇʏ {user_mention} (`{uid}`), ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴ sᴜᴄᴄᴇssғᴜʟʟʏ ʀᴇᴍᴏᴠᴇᴅ via Mini App.\n\n"
                        f"{notify_note}"
                    )

                notified = await _notify_user_and_admin(
                    bot_client,
                    admin_id,
                    uid,
                    user_dm,
                    _admin_dm,
                    log_text=f"#AdminAction #RevokePremium\nAdmin: {admin_mention} (`{admin_id}`)\nUser: {user_mention} (`{uid}`)"
                )
                _db_cache["timestamp"] = 0.0
                return web.json_response({
                    "success": True,
                    "message": f"Revoked premium for {uid}. {'User & admin notified.' if notified else 'Admin notified (user DM failed).'}"
                })

            elif op == "ban":
                ban_duration = int(body.get("duration", 3650))
                ban_reason = str(body.get("reason", "Banned via Web Admin Console")).strip() or "Banned via Web Admin Console"
                await digital_botz.ban_user(uid, ban_duration, ban_reason)

                user_dm = (
                    f"🚫 You are banned from using this bot for **{ban_duration}** day(s) for the reason: __{ban_reason}__\n\n"
                    f"**Message from the admin**"
                )

                def _admin_dm(notified: bool):
                    notify_note = "User notified successfully!" if notified else "User notification failed (user may have blocked the bot)."
                    return (
                        f"🚫 Banned user {user_mention} (`{uid}`) for **{ban_duration}** day(s) via Mini App.\n"
                        f"**Reason:** __{ban_reason}__\n\n"
                        f"{notify_note}"
                    )

                notified = await _notify_user_and_admin(
                    bot_client,
                    admin_id,
                    uid,
                    user_dm,
                    _admin_dm,
                    log_text=f"#AdminAction #Ban\nAdmin: {admin_mention} (`{admin_id}`)\nUser: {user_mention} (`{uid}`)\nDuration: {ban_duration}d\nReason: {ban_reason}"
                )
                return web.json_response({
                    "success": True,
                    "message": f"User {uid} banned. {'User & admin notified.' if notified else 'Admin notified (user DM failed).'}"
                })

            elif op == "unban":
                await digital_botz.remove_ban(uid)
                user_dm = "✅ Your ban was lifted! You can use the bot again."

                def _admin_dm(notified: bool):
                    notify_note = "User notified successfully!" if notified else "User notification failed!"
                    return f"🔓 Unbanned user {user_mention} (`{uid}`) via Mini App.\n\n{notify_note}"

                notified = await _notify_user_and_admin(
                    bot_client,
                    admin_id,
                    uid,
                    user_dm,
                    _admin_dm,
                    log_text=f"#AdminAction #Unban\nAdmin: {admin_mention} (`{admin_id}`)\nUser: {user_mention} (`{uid}`)"
                )
                return web.json_response({
                    "success": True,
                    "message": f"User {uid} unbanned. {'User & admin notified.' if notified else 'Admin notified (user DM failed).'}"
                })

            elif op == "clear_tasks":
                from helper.database import Task
                from plugins.file_rename import active_tasks, active_progress
                user_tasks = await Task.find(Task.user_id == uid).to_list()
                cleared = 0
                for t in user_tasks:
                    tid = str(t.id)
                    if tid in active_tasks:
                        active_tasks[tid].cancel()
                    active_progress.pop(tid, None)
                    await t.delete()
                    cleared += 1

                user_dm = f"⚠️ Your **{cleared}** active/queued task(s) were cleared by an administrator."

                def _admin_dm(notified: bool):
                    notify_note = "User notified via DM." if notified else "User DM failed."
                    return f"🧹 Cleared **{cleared}** task(s) for {user_mention} (`{uid}`) via Mini App.\n{notify_note}"

                notified = await _notify_user_and_admin(
                    bot_client,
                    admin_id,
                    uid,
                    user_dm if cleared > 0 else "",
                    _admin_dm
                )
                return web.json_response({
                    "success": True,
                    "message": f"Cleared {cleared} task(s) for user {uid}. {'User & admin notified.' if notified else 'Admin notified.'}"
                })

            return web.json_response({"success": False, "error": "Unknown user operation"}, status=400)
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    elif action == "cancel_task":
        try:
            task_id = str(body.get("task_id", "")).strip()
            if not task_id:
                return web.json_response({"success": False, "error": "Missing task_id"}, status=400)

            from plugins.file_rename import active_tasks, active_progress
            cancelled = False
            if task_id in active_tasks:
                active_tasks[task_id].cancel()
                cancelled = True
            active_progress.pop(task_id, None)
            try:
                await digital_botz.delete_task(task_id)
                cancelled = True
            except Exception:
                pass

            return web.json_response({
                "success": True,
                "message": f"Task {task_id[-6:]} cancelled." if cancelled else "Task already completed."
            })
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    elif action == "worker_control":
        try:
            target_wid = str(body.get("worker_id", "")).strip()
            op = body.get("op")
            workers = getattr(Config, "WORKER_CLIENTS", [])
            target = next((w for w in workers if str(id(w)) == target_wid), None)
            if not target:
                return web.json_response({"success": False, "error": "Worker node not found"}, status=404)

            if op == "heal":
                target.is_healthy = True
                target.unhealthy_until = 0
                target.disabled = False
                if not getattr(target, "is_connected", True):
                    try:
                        await target.connect()
                    except Exception:
                        pass
                return web.json_response({"success": True, "message": "Worker node healed & enabled."})
            elif op == "disable":
                target.disabled = True
                return web.json_response({"success": True, "message": "Worker node disabled from rotation."})
            elif op == "enable":
                target.disabled = False
                target.is_healthy = True
                target.unhealthy_until = 0
                return web.json_response({"success": True, "message": "Worker node enabled."})

            return web.json_response({"success": False, "error": "Unknown worker op"}, status=400)
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    elif action == "broadcast_start":
        msg = str(body.get("message", "")).strip()
        parse_mode_str = str(body.get("parse_mode", "html")).strip() or "html"
        media_url = str(body.get("media_url", "")).strip()
        media_type = str(body.get("media_type", "auto")).strip() or "auto"
        disable_preview = bool(body.get("disable_preview", False))

        if not msg and not media_url:
            return web.json_response({"success": False, "error": "Broadcast message or media cannot be empty"}, status=400)
        if _broadcast_state["running"]:
            return web.json_response({"success": False, "error": "A broadcast is already running"}, status=409)
        _broadcast_state["task"] = asyncio.create_task(
            _run_web_broadcast(
                text_msg=msg,
                parse_mode_str=parse_mode_str,
                media_url=media_url,
                media_type=media_type,
                disable_preview=disable_preview
            )
        )
        return web.json_response({"success": True, "message": "Global broadcast started."})

    elif action == "broadcast_cancel":
        _broadcast_state["running"] = False
        if _broadcast_state["task"]:
            _broadcast_state["task"].cancel()
        return web.json_response({"success": True, "message": "Broadcast stopped."})

    return web.json_response({"success": False, "error": "Unknown admin action"}, status=400)


@DigitalRenameBot.get("/api/stream")
async def api_stream_handler(request):
    """Unified Server-Sent Events (SSE) endpoint pushing live status, queue & analytics telemetry."""
    user_obj = _extract_user_from_request(request)
    is_admin = bool(user_obj and int(user_obj.get("id", 0)) in _get_admin_ids())
    viewer_id = int(user_obj["id"]) if user_obj else None

    response = web.StreamResponse(
        status=200,
        reason='OK',
        headers={
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        }
    )
    await response.prepare(request)

    try:
        while True:
            queue_data = await get_queue_data(viewer_user_id=viewer_id, is_admin=is_admin)
            status_data = await get_status(pending_count=len(queue_data.get("pending", [])), is_admin=is_admin)
            analytics_7d = await get_7d_analytics()
            analytics_24h = await get_24h_analytics()
            payload = {
                "status": status_data,
                "queue": queue_data,
                "analytics_7d": analytics_7d,
                "analytics_24h": analytics_24h
            }
            chunk = f"data: {json.dumps(payload)}\n\n".encode("utf-8")
            await response.write(chunk)
            await response.drain()
            await asyncio.sleep(1.0)
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
        pass
    except Exception:
        pass

    return response


async def _on_startup_analytics(app):
    try:
        from plugins import file_rename
        if hasattr(file_rename, "load_persisted_maintenance_state"):
            await file_rename.load_persisted_maintenance_state()
        else:
            state = await digital_botz.get_maintenance_state()
            file_rename.queue_paused = bool(state.get("queue_paused", False))
            file_rename.maintenance_message = str(state.get("maintenance_message", "") or "")
    except Exception:
        pass
    app["analytics_rollup_task"] = asyncio.create_task(_analytics_rollup_loop())


async def _on_cleanup_analytics(app):
    task = app.get("analytics_rollup_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    await flush_analytics_rollups()


async def web_server():
    web_app = web.Application(client_max_size=30000000)
    web_app.add_routes(DigitalRenameBot)
    web_app.on_startup.append(_on_startup_analytics)
    web_app.on_cleanup.append(_on_cleanup_analytics)
    return web_app
