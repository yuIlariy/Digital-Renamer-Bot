# (c) @RknDeveloperr
# Rkn Developer 
# Don't Remove Credit 😔
# Telegram Channel @RknDeveloper & @Rkn_Botz
# Developer @RknDeveloperr
# Special Thanks To @ReshamOwner
# Update Channel @Digital_Botz & @DigitalBotz_Support
"""
Apache License 2.0
Copyright (c) 2022 @Digital_Botz
"""

from aiohttp import web
import time
import psutil
import shutil
import os
from collections import deque
from config import Config
from plugins import __version__
from helper.utils import humanbytes
from helper.database import digital_botz
from wzgram.types import Message

# Ensure templates directory exists
os.makedirs('templates', exist_ok=True)

DigitalRenameBot = web.RouteTableDef()

# --- NETWORK SPEED TRACKER GLOBALS ---
last_net_io = psutil.net_io_counters()
last_time = time.time()
last_up_speed = 0
last_dl_speed = 0

# ⚠️ INVISIBLE HOOK: Intercept Pyrogram's edit message instead of the database!
# This quietly listens for "Uploaded Successfully" and adds +1 to the database counter.
if not hasattr(Message, "_hooked_for_renames"):
    original_edit = getattr(Message, "edit", None)
    original_edit_text = getattr(Message, "edit_text", None)

    async def hooked_edit_wrapper(self, text, *args, **kwargs):
        if isinstance(text, str) and "Uploaded Successfully" in text:
            await digital_botz.increment_total_renames()
        if original_edit:
            return await original_edit(self, text, *args, **kwargs)

    async def hooked_edit_text_wrapper(self, text, *args, **kwargs):
        if isinstance(text, str) and "Uploaded Successfully" in text:
            await digital_botz.increment_total_renames()
        if original_edit_text:
            return await original_edit_text(self, text, *args, **kwargs)

    if original_edit:
        Message.edit = hooked_edit_wrapper
    if original_edit_text:
        Message.edit_text = hooked_edit_text_wrapper
    
    Message._hooked_for_renames = True

def format_short_number(num):
    if num < 1000:
        return str(num)
    for unit in ['k', 'M', 'B']:
        num /= 1000.0
        if num < 1000:
            formatted = f"{num:.2f}".rstrip('0').rstrip('.')
            return f"{formatted}{unit}"
    return str(num)

async def get_status():
    global last_net_io, last_time, last_up_speed, last_dl_speed
    
    try:
        from plugins.file_rename import worker_loads, active_tasks
    except ImportError:
        worker_loads = {}
        active_tasks = {}
    
    real_total_users = await digital_botz.total_users_count()
    total_users = real_total_users + 1009
    
    if getattr(Config, 'PREMIUM_MODE', False):
        real_total_premium_users = await digital_botz.total_premium_users_count()
        total_premium_users = real_total_premium_users + 50
    else:
        total_premium_users = "Disabled ✅"
    
    bot_sec = time.time() - Config.BOT_UPTIME
    b_days, b_rem = divmod(bot_sec, 86400)
    b_hrs, b_rem = divmod(b_rem, 3600)
    b_mins, b_secs = divmod(b_rem, 60)
    bot_uptime = f"{int(b_days)}d {int(b_hrs)}h {int(b_mins)}m" if b_days > 0 else f"{int(b_hrs):02d}h{int(b_mins):02d}m{int(b_secs):02d}s"

    sys_sec = time.time() - psutil.boot_time()
    s_days, s_rem = divmod(sys_sec, 86400)
    s_hrs, s_rem = divmod(s_rem, 3600)
    s_mins, s_secs = divmod(s_rem, 60)
    system_uptime = f"{int(s_days)}d {int(s_hrs)}h {int(s_mins)}m" if s_days > 0 else f"{int(s_hrs):02d}h{int(s_mins):02d}m{int(s_secs):02d}s"

    total, used, free = shutil.disk_usage(".")
    
    current_net_io = psutil.net_io_counters()
    current_time = time.time()
    
    if (current_time - last_time) >= 1.0: 
        last_up_speed = (current_net_io.bytes_sent - last_net_io.bytes_sent) / (current_time - last_time)
        last_dl_speed = (current_net_io.bytes_recv - last_net_io.bytes_recv) / (current_time - last_time)
        last_net_io = current_net_io
        last_time = current_time

    raw_total_renames = await digital_botz.get_total_renames()
    formatted_total_renames = format_short_number(raw_total_renames)

    net_stats = await digital_botz.get_network_stats()
    data_sent = humanbytes(net_stats.get('sent', 0))
    data_recv = humanbytes(net_stats.get('recv', 0))

    workers = getattr(Config, "WORKER_CLIENTS", [])
    total_workers = len(workers)
    healthy_workers = sum(1 for w in workers if getattr(w, "is_healthy", True))
    active_workers = sum(1 for load in worker_loads.values() if load > 0)
    free_workers = max(0, healthy_workers - active_workers)
    
    current_active_tasks = len(active_tasks)
    
    up_speed_str = f"{humanbytes(last_up_speed)}/s" if last_up_speed > 0 else "0 B/s"
    dl_speed_str = f"{humanbytes(last_dl_speed)}/s" if last_dl_speed > 0 else "0 B/s"
    
    return {
        "bot_status": "Operational",
        "bot_version": __version__,
        "total_users": total_users,
        "premium_users": total_premium_users,
        "bot_uptime": bot_uptime,
        "system_uptime": system_uptime,
        "cpu_usage": psutil.cpu_percent(),
        "ram_usage": psutil.virtual_memory().percent,
        "disk_usage": psutil.disk_usage('/').percent,
        "total_disk": humanbytes(total),
        "used_disk": humanbytes(used),
        "free_disk": humanbytes(free),
        "data_sent": data_sent,
        "data_recv": data_recv,
        "up_speed": up_speed_str,
        "dl_speed": dl_speed_str,
        "total_renames": formatted_total_renames,
        "total_workers": total_workers,
        "healthy_workers": healthy_workers, 
        "active_workers": active_workers,
        "free_workers": free_workers,
        "active_tasks": current_active_tasks,
        "timestamp": int(time.time()),
        "github_link": "https://github.com/DigitalBotz/Digital-Rename-Bot",
        "telegram_link": "https://t.me/OtherBs"
    }

@DigitalRenameBot.get("/", allow_head=True)
async def root_route_handler(request):
    status_data = await get_status()
    
    try:
        with open('templates/welcome.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        return web.Response(text="API is Operational. (templates/welcome.html is missing)", content_type='text/plain')
    
    for key, value in status_data.items():
        html_content = html_content.replace(f'{{{{{key}}}}}', str(value))
    
    return web.Response(text=html_content, content_type='text/html')

@DigitalRenameBot.get("/api/status")
async def api_status_handler(request):
    status_data = await get_status()
    return web.json_response(status_data)

# ==========================================
# --- LIVE QUEUE WITH PROGRESS TRACKING ---
# ==========================================
@DigitalRenameBot.get("/api/queue")
async def api_queue_handler(request):
    """Exposes live split data for Downloads, Uploads, and Pending Tasks."""
    try:
        from helper.database import digital_botz, Task
        from plugins.file_rename import active_progress
    except ImportError:
        return web.json_response({"error": "Cannot load required modules"})
        
    try:
        active_db_tasks = await Task.find_all().to_list()
        
        downloading = []
        uploading = []
        pending = []
        
        for task in active_db_tasks:
            tid = str(task.id)
            if tid in active_progress:
                prog = active_progress[tid]
                if prog["status"] == "Downloading":
                    downloading.append(prog)
                else:
                    uploading.append(prog)
            else:
                pending.append({"filename": task.new_name, "status": "Pending"})
                
        return web.json_response({
            "success": True,
            "downloading": downloading,
            "uploading": uploading,
            "pending": pending
        })
    except Exception as e:
        return web.json_response({"success": False, "error": str(e)})


async def web_server():
    web_app = web.Application(client_max_size=30000000)
    web_app.add_routes(DigitalRenameBot)
    return web_app
