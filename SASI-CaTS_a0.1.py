# ==============================================================================
# SASI-CaTS (Software Applet & Search Interface - Caching auto-Transcoding Server)
# Version: a0.1
#
# A Python-based web server that transcodes media on-the-fly for vintage computers.
# ==============================================================================
#
# --- Basic Usage ---
#
# 1. Start Server:
#    python SASI-CaTS_a0.1.py /path/to/your/media
#
# 2. Connect Client:
#    Open browser and go to http://<server_ip>:8000
#
# --- Startup Flags ---
#
# --fresh    : Deletes the entire cache on startup.
# --cpu      : Forces iStream to use a CPU-only transcode workflow.
# --AppleS   : Uses a transcode workflow optimized for Apple Silicon Macs.
#
# --- Python Virtual Environment (Recommended) ---
#
# For managed Python systems (Debian/APT; macOS/Homebrew), a venv is advised.
#
#   # Create the environment
#   python3 -m venv sasi_env
#
#   # Activate it (necessary for each new terminal session)
#   source sasi_env/bin/activate
#
#   # Install required libraries
#   pip install requests Pillow websocket-client
#
# --- Client Recommendations ---
#
# * Browser: Classilla (OS 9), TenFourFox (OSX PPC), MyPal (Win XP).
# * Player: QuickTime (OS 9), VLC (OSX/Windows).
#
# --- Applet Overview ---
#
# * Homepage: Main hub with a FrogFind! search bar and applet navigation.
#
# * Files: Paginated browser for your media library.
#
# * iMagery: Grid-based image gallery with an automatic slideshow feature.
#
# * iStream: Transcodes videos to a format compatible with vintage QuickTime
#     (Target: ~346x260, 19fps, MPEG4/ADPCM in .mov). Can batch process
#     entire directories for serial viewing.
#
# * iTube: Downloads YouTube videos to the server for iStream transcoding.
#
# * iGem: A chat client for the Google Gemini API. To enable, you must
#     insert your own API key into the `GEMINI_API_KEY` variable in the
#     script's configuration section.
#
# * iComfy: An API client for a local ComfyUI instance. To use a ComfyUI
#     instance on a different machine, change the `COMFYUI_IP` variable in
#     the configuration section to your ComfyUI's IP address and port.
#
# ==============================================================================

import http.server
import socketserver
import subprocess
import argparse
import os
import random
import socket
import atexit
import shutil
import sys
import threading
import time
import json
import math
import requests
import re
import string
import uuid
import websocket # Requires websocket-client library
from urllib.parse import urlparse, parse_qs, quote, unquote_plus, urlencode
from collections import deque
from PIL import Image

# --- ############################################################### ---
# ---                       CONFIGURATION                             ---
# --- ############################################################### ---

# Server settings
PORT = 8000
COMFYUI_IP = "ComfyUI_IP" # Default is localhost. Set to "IP:PORT" for a remote instance.

# Pagination settings
FILES_PER_PAGE = 31
IMAGERY_FILES_PER_PAGE = 51
ISTREAM_ITEMS_PER_PAGE = 15

# iStream main transcoding settings
MAX_PIXELS = 346 * 260
TARGET_FRAMERATE = 19

# Supported file extensions
VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm']
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.gif', '.png', '.bmp', '.webp']
TRANSCODE_IMAGE_EXT = ['.heic', '.avif', '.tiff']

# iStream preview-specific transcoding settings
MAX_PIXELS_PREVIEW = 266 * 200
TARGET_FRAMERATE_PREVIEW = 16
VIDEO_BITRATE_PREVIEW = "300k"

# iMagery slideshow settings
SLIDESHOW_MAX_PIXELS = 589824
SLIDESHOW_TRANSCODE_BATCH_SIZE = 20
SLIDESHOW_TRANSCODE_TRIGGER = 5

# Thumbnail settings
IMAGERY_THUMBNAIL_WIDTH = 200

# iGem (Gemini Chat) settings
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY_HERE" # Replace with your key
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"


# --- ############################################################### ---
# ---                         GLOBAL STATE                            ---
# --- ############################################################### ---

# Filesystem Paths
ROOT_MEDIA_FOLDER = ""
AVAILABLE_DRIVES = []
CACHE_DIR = ""
ITUBE_DIR = ""
ICOMFY_DIR = ""
CACHE_INDEX_FILE = ""

# Cache and Session Management
CACHE_DATA = {}
ISTREAM_SESSIONS = {}
ACTIVE_ISTREAM_SESSION_ID = None
SLIDESHOW_SESSIONS = {}
GEMINI_SESSIONS = {}
ITUBE_JOBS = {}
ICOMFY_SESSIONS = {}

# Threading and Concurrency Control
STOP_EVENT = threading.Event()
CACHE_LOCK = threading.Lock()
ISTREAM_LOCK = threading.Lock()
SLIDESHOW_LOCK = threading.Lock()
THUMB_CRAWLER_LOCK = threading.Lock()
ICOMFY_LOCK = threading.Lock()

# Worker Threads
ISTREAM_WORKER = None
THUMB_CRAWLER_WORKER = None
ICOMFY_WORKER = None
THUMB_CRAWLER_PAUSE_EVENT = threading.Event()

# Global Flags & Data
USE_THOUSANDS_COLORS = True
CPU_MODE = False
APPLE_SILICON_MODE = False
COMFYUI_URL = "127.0.0.1:8188" # Default value, can be overridden by config
COMFY_CLIENT_ID = str(uuid.uuid4())
COMFY_DATA = { 'models': [], 'vaes': [], 'loras': [], 'samplers': [], 'schedulers': [] }
SOFTKEY_COLORS = ['#57a849', '#ef882a', '#de3d2b', '#8456a0', '#1880c2']

# --- ############################################################### ---
# ---                 CACHE MANAGEMENT SYSTEM                       ---
# --- ############################################################### ---

class CacheManager:
    """Handles loading, saving, and querying the persistent cache index."""
    @staticmethod
    def load_cache():
        global CACHE_DATA
        with CACHE_LOCK:
            if os.path.exists(CACHE_INDEX_FILE):
                with open(CACHE_INDEX_FILE, 'r') as f:
                    try:
                        CACHE_DATA = json.load(f)
                    except json.JSONDecodeError:
                        print("!! WARNING: Cache index is corrupted. Starting fresh.")
                        CACHE_DATA = {}
            else:
                CACHE_DATA = {'istream': {}, 'previews': {}, 'thumbnails': {}, 'imagery_thumbs': {}, 'slideshow': {}, 'icomfy': {}}

    @staticmethod
    def save_cache():
        with CACHE_LOCK:
            if os.path.exists(CACHE_INDEX_FILE):
                try:
                    shutil.copy2(CACHE_INDEX_FILE, CACHE_INDEX_FILE + '.bak')
                except Exception as e:
                    print(f"!! Could not create cache backup: {e}")

            with open(CACHE_INDEX_FILE, 'w') as f:
                json.dump(CACHE_DATA, f, indent=4)

    @staticmethod
    def get_item(cache_type, original_path):
        with CACHE_LOCK:
            path = CACHE_DATA.get(cache_type, {}).get(original_path)
            if path and os.path.exists(path):
                return path
            return None

    @staticmethod
    def add_item(cache_type, original_path, cached_path):
        with CACHE_LOCK:
            if cache_type not in CACHE_DATA:
                CACHE_DATA[cache_type] = {}
            CACHE_DATA[cache_type][original_path] = cached_path

    @staticmethod
    def perform_fresh_cleanup():
        print("-> Performing --fresh cleanup...")
        if os.path.exists(CACHE_INDEX_FILE):
            with open(CACHE_INDEX_FILE, 'r') as f:
                try:
                    data_to_clean = json.load(f)
                    all_files = []
                    for cache_type in data_to_clean.values():
                        all_files.extend(cache_type.values())
                    
                    for file_path in all_files:
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except OSError as e:
                                print(f"!! Could not remove file {file_path}: {e}")
                    
                    print(f"--> Cleaned up {len(all_files)} cached files.")
                    os.remove(CACHE_INDEX_FILE)
                    if os.path.exists(CACHE_INDEX_FILE + '.bak'):
                        os.remove(CACHE_INDEX_FILE + '.bak')
                except Exception as e:
                    print(f"!! Error during cleanup: {e}")
        else:
            print("--> No cache index found. Nothing to clean.")
        
        if os.path.exists(CACHE_DIR):
            try:
                shutil.rmtree(CACHE_DIR)
                print(f"--> Removed cache directory: {CACHE_DIR}")
            except OSError:
                 print(f"--> Cache directory may not be empty, leaving as is.")


def get_cache_path(original_path, cache_subdir, new_extension):
    """Generates a structured, persistent path for a cached file using a hash for the filename."""
    filename_hash = str(hash(original_path))
    new_filename = f"{filename_hash}{new_extension}"
    
    sub_dir_1 = new_filename[:2]
    sub_dir_2 = new_filename[2:4]
    
    final_path = os.path.join(CACHE_DIR, cache_subdir, sub_dir_1, sub_dir_2, new_filename)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    return final_path

# --- ############################################################### ---
# ---                  HTML & CSS TEMPLATES                         ---
# --- ############################################################### ---
CSS_STYLES = """
<style>
    * { box-sizing: border-box; }
    body { font-family: 'Lucida Grande', 'Verdana', 'sans-serif'; background-color: #A5A5A5; color: #000; margin: 0; padding: 0; }
    .main-container { max-width: 700px; margin: 20px auto; background-color: #EAEAEA; border: 2px solid #555; padding: 10px 20px 20px 20px; box-shadow: 5px 5px 10px #333; }
    h1, h2, h3 { color: #000; }
    h1 { text-align: center; border-bottom: 2px solid #999; padding-bottom: 10px; margin-top: 10px; font-size: 16px; font-weight: bold; }
    h2 { font-size: 13px; text-align: center; margin-top: -5px; word-wrap: break-word; }
    h3 { font-size: 12px; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #ccc; padding-bottom: 3px; }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; }
    td { padding: 8px; border-bottom: 1px solid #ccc; vertical-align: middle; }
    .td-filename { word-wrap: break-word; word-break: break-all; }
    tr:nth-child(even) { background-color: #DFDFDF; }
    a { color: #00479d; text-decoration: none; }
    a:hover { text-decoration: underline; color: #d62d20; }
    .softkey { display: inline-block; background-color: #D8D8D8; border: 2px outset #B0B0B0; padding: 8px 15px; text-decoration: none; color: #000; margin: 5px; font-weight: bold; }
    .softkey-disabled { background-color: #BDBDBD; color: #666; border-style: solid; border-color: #999; }
    .softkey-small { padding: 2px 6px; font-size: 10px; margin-left: 10px; }
    .softkey-slideshow { padding: 4px 10px; font-size: 12px; }
    .softkey-group-item { padding: 5px 10px; font-size: 10px; line-height: 1.1; text-align: center; vertical-align: top; margin: 2px; }
    .softkey-container { text-align: center; white-space: nowrap; }
    .softkey:active { border-style: inset; }
    .pagination-container { text-align: center; margin-top: 10px; }
    .page-info { font-weight: bold; margin: 0 10px; display: inline-block; vertical-align: middle; }
    .header-bar { overflow: hidden; padding-bottom: 10px; }
    .home-content { text-align: center; }
    .file-icon { margin-right: 10px; border: 1px solid #888; }
    .status-text { font-size: 11px; color: #555; font-style: italic; }
    .status-box { padding: 10px; background-color: #f9ee4a; border: 1px solid #ef882a; margin-bottom: 10px; text-align: center; font-weight: bold; }
    .action-link { font-weight: bold; }
    .progress-bar-bg { border: 1px solid #333; background-color: #ccc; padding: 1px; margin-bottom: 2px; }
    .progress-bar-fg { background-color: #00479d; color: #fff; text-align: center; font-size: 10px; font-weight: bold; white-space: nowrap; }
    .gallery-table td { width: 33.3%; text-align: center; padding: 5px; vertical-align: top; border: none; }
    .gallery-thumb { width: 100%; height: auto; border: 2px solid #888; margin-bottom: 5px; }
    .slideshow-container { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background-color: #000; text-align: center; }
    .slideshow-img-container { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; padding-bottom: 60px; }
    .slideshow-img { max-width: 95%; max-height: 95%; width: auto; height: auto; }
    .slideshow-controls { position: absolute; bottom: 0; left: 0; width: 100%; padding: 10px; background-color: rgba(0,0,0,0.5); }
    .controls-container { border: 1px solid #ccc; padding: 5px; margin-bottom: 10px; background-color: #DFDFDF; }
    .icomfy-viewer { text-align: center; margin-bottom: 15px; }
    .icomfy-thumb { max-width: 200px; border: 2px solid #555; cursor: pointer; }
    .icomfy-controls-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; align-items: start;}
    .icomfy-control-group label { display: block; font-weight: bold; margin-bottom: 3px; font-size: 11px;}
    .icomfy-control-group input, .icomfy-control-group select, .icomfy-control-group textarea { width: 100%; font-size: 11px; padding: 2px; }
    .icomfy-control-group textarea { height: 60px; }
    .generate-button { font-size: 16px; padding: 12px 20px; width: 100%; }
    .stop-button { font-size: 16px; padding: 12px 20px; width: 100%; }
    .blinking { animation: blinker 1.5s linear infinite; }
    @keyframes blinker { 50% { opacity: 0.3; } }
    @keyframes blink-caret { 50% { background-color: transparent; } }
    .igem-body { background-color: #000; }
    .igem-terminal { background-color: #000; border-color: #00FF00; color: #00FF00; font-family: 'Monaco', 'Courier New', monospace; }
    .igem-terminal h1, .igem-terminal h2 { color: #00FF00; border-color: #008800; }
    .igem-terminal a { color: #33FF33; }
    .igem-terminal a:hover { color: #99FF99; }
    .igem-terminal .softkey { background-color: #003300; border: 1px outset #008800; color: #00FF00; }
    .igem-terminal hr { border-color: #008800; }
    .igem-terminal-history { padding: 5px; }
    .igem-terminal-history p { margin: 5px 0; line-height: 1.4; }
    .igem-terminal-history b { color: #FFFFFF; }
    .igem-terminal textarea { width:98%; background-color: #000; color: #00FF00; border: 1px solid #008800; font-family: inherit; }
    .igem-terminal textarea:focus { outline: none; border-color: #33FF33; background-color: #001100; animation: blink-caret 1s step-end infinite; }
    .igem-terminal input[type=submit] { background-color: #003300; border: 1px outset #008800; color: #00FF00; padding: 5px 10px; font-weight: bold; }
    .softkey.softkey-c1, input.softkey-c1 { background-color: #57a849; color: #fff !important; text-shadow: 1px 1px 1px #333; }
    .softkey.softkey-c2, input.softkey-c2 { background-color: #ef882a; color: #fff !important; text-shadow: 1px 1px 1px #333; }
    .softkey.softkey-c3, input.softkey-c3 { background-color: #de3d2b; color: #fff !important; text-shadow: 1px 1px 1px #333; }
    .softkey.softkey-c4, input.softkey-c4 { background-color: #8456a0; color: #fff !important; text-shadow: 1px 1px 1px #333; }
    .softkey.softkey-c5, input.softkey-c5 { background-color: #1880c2; color: #fff !important; text-shadow: 1px 1px 1px #333; }
    a.dir-link { color: #005A9C; font-weight: bold; }
    a.file-link { color: #2F4F4F; }
    a.dir-link .filename-wrapper { color: #005A9C; }
</style>
"""

def get_html_header(title, subtitle=None, refresh_interval=None, is_igem=False, color_cycler=None):
    """Generates the standard HTML header for every page."""
    refresh_tag = f'<meta http-equiv="refresh" content="{refresh_interval}">' if refresh_interval else ''
    container_class = "main-container igem-terminal" if is_igem else "main-container"
    body_class = 'class="igem-body"' if is_igem else ''
    subtitle_html = f"<h2>{subtitle}</h2>" if subtitle else ""
    
    home_button_class = "softkey"
    if color_cycler:
        home_button_class += f" {color_cycler.get_class()}"

    return f"""
    <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
    <html><head><title>{title}</title>{CSS_STYLES}{refresh_tag}</head>
    <body {body_class}><div class="{container_class}">
    <div class="header-bar"><a href="/" class="{home_button_class}" style="float: left;">Home</a></div>
    <h1>{title}</h1>{subtitle_html}
    """

HTML_FOOTER = "</div></body></html>"


# --- ############################################################### ---
# ---                CORE LOGIC & WORKER THREADS                      ---
# --- ############################################################### ---

class ColorCycler:
    """Provides cycling color classes for UI elements on a per-page basis."""
    def __init__(self):
        self.index = 0
        self.color_count = len(SOFTKEY_COLORS)

    def get_class(self):
        """Returns the next color class in the cycle."""
        class_name = f"softkey-c{(self.index % self.color_count) + 1}"
        self.index += 1
        return class_name

def get_available_drives():
    """Returns a list of available drive roots (e.g., 'C:\\' or '/')."""
    drives = []
    if sys.platform == "win32":
        for letter in string.ascii_uppercase:
            drive = f"{letter}:\\"
            if os.path.exists(drive):
                drives.append(drive)
    else: # macOS, Linux
        drives.append("/")
    return drives

def get_lan_ip():
    """Attempts to find the server's local network IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't have to be reachable
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        try:
            IP = socket.gethostbyname(socket.gethostname())
        except Exception:
            IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def get_sorted_and_filtered_items(path, sort_by, sort_order, allowed_extensions=None):
    """Lists, filters, and sorts directory contents."""
    try:
        all_items = os.listdir(path)
    except FileNotFoundError:
        return []

    dirs_with_meta = []
    files_with_meta = []
    for item in all_items:
        item_path = os.path.join(path, item)
        try:
            stat_info = os.stat(item_path)
            if os.path.isdir(item_path):
                dirs_with_meta.append({'name': item, 'mtime': stat_info.st_mtime, 'size': stat_info.st_size})
            else:
                ext = os.path.splitext(item)[1].lower()
                if allowed_extensions and ext not in allowed_extensions:
                    continue
                files_with_meta.append({'name': item, 'mtime': stat_info.st_mtime, 'size': stat_info.st_size, 'ext': ext})
        except OSError:
            # Skip files that can't be accessed
            continue

    reverse_order = (sort_order == 'desc')
    
    # Sort directories
    if sort_by == 'date':
        dirs_with_meta.sort(key=lambda x: x['mtime'], reverse=reverse_order)
    elif sort_by == 'size':
        dirs_with_meta.sort(key=lambda x: x['size'], reverse=reverse_order)
    else: # Default to name
        dirs_with_meta.sort(key=lambda x: x['name'].lower(), reverse=reverse_order)
    
    # Sort files
    if sort_by == 'kind':
        def kind_sort_key(file_dict):
            ext = file_dict['ext']
            if ext == '.gif': return 0
            if ext in VIDEO_EXTENSIONS: return 1
            if ext in IMAGE_EXTENSIONS or ext in TRANSCODE_IMAGE_EXT: return 2
            return 3
        files_with_meta.sort(key=kind_sort_key)
    elif sort_by == 'date':
        files_with_meta.sort(key=lambda x: x['mtime'], reverse=reverse_order)
    elif sort_by == 'size':
        files_with_meta.sort(key=lambda x: x['size'], reverse=reverse_order)
    else: # Default to name
        files_with_meta.sort(key=lambda x: x['name'].lower(), reverse=reverse_order)
    
    sorted_dirs = [d['name'] for d in dirs_with_meta]
    sorted_files = [f['name'] for f in files_with_meta]

    # Always list directories first
    return sorted_dirs + sorted_files

def get_video_duration(input_file):
    """Gets the duration of a video file in seconds using ffprobe."""
    ffprobe_cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input_file]
    try:
        result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except (ValueError, subprocess.CalledProcessError):
        return 0.0

def get_new_dimensions(input_file, max_pixels=MAX_PIXELS):
    """Calculates new video dimensions that fit within max_pixels while preserving aspect ratio."""
    ffprobe_cmd = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height', '-of', 'json', input_file]
    try:
        result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        w, h = int(data['streams'][0]['width']), int(data['streams'][0]['height'])
        
        if w * h <= max_pixels:
            return w - (w % 2), h - (h % 2) # Ensure even dimensions for compatibility
            
        ar = w / h
        nh = int(math.sqrt(max_pixels / ar) / 2) * 2
        nw = int(nh * ar / 2) * 2
        return nw, nh
    except Exception:
        return -2, 260 # Return error code

def parse_ffmpeg_progress(line, total_duration):
    """Parses ffmpeg's stderr progress output to calculate a percentage."""
    if total_duration == 0: return 0
    time_match = re.search(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})", line)
    if time_match:
        h, m, s, ms = map(int, time_match.groups())
        current_seconds = h * 3600 + m * 60 + s + ms / 100
        return int((current_seconds / total_duration) * 100)
    return None

def parse_yt_dlp_progress(line):
    """Parses yt-dlp's stdout progress output to calculate a percentage."""
    progress_match = re.search(r"\[download\]\s+([\d\.]+%)", line)
    if progress_match:
        return float(progress_match.group(1).replace('%', ''))
    return None

def get_recursive_images(root_path):
    """Recursively finds all valid image files in a directory."""
    image_files = []
    valid_extensions = IMAGE_EXTENSIONS + TRANSCODE_IMAGE_EXT
    for dirpath, _, filenames in os.walk(root_path):
        for filename in sorted(filenames):
            if os.path.splitext(filename)[1].lower() in valid_extensions:
                image_files.append(os.path.join(dirpath, filename))
    return image_files

def scale_image_to_megapixel(img):
    """Scales a PIL Image object down to fit within SLIDESHOW_MAX_PIXELS."""
    w, h = img.size
    if w * h > SLIDESHOW_MAX_PIXELS:
        ar = w / h
        new_h = int(math.sqrt(SLIDESHOW_MAX_PIXELS / ar))
        new_w = int(new_h * ar)
        return img.resize((new_w, new_h), Image.Resampling.LANCZOS)
    return img

def slideshow_transcode_worker(session_id):
    """Background worker to transcode images for a slideshow session."""
    with SLIDESHOW_LOCK:
        if session_id not in SLIDESHOW_SESSIONS or SLIDESHOW_SESSIONS[session_id].get('is_transcoding', False):
            return
        session = SLIDESHOW_SESSIONS[session_id]
        session['is_transcoding'] = True
        playlist = session['playlist']
        start_cursor = session['transcode_cursor']
        end_cursor = min(start_cursor + SLIDESHOW_TRANSCODE_BATCH_SIZE, len(playlist))

    for i in range(start_cursor, end_cursor):
        original_path = playlist[i]
        
        # Skip if already cached or if it's a GIF (which doesn't need transcoding)
        if CacheManager.get_item('slideshow', original_path): continue
        if original_path.lower().endswith('.gif'): continue
        
        output_path = get_cache_path(original_path, 'slideshow', '.jpg')

        try:
            with Image.open(original_path) as img:
                img = scale_image_to_megapixel(img)
                if img.mode not in ('RGB', 'L'): img = img.convert('RGB')
                img.save(output_path, "JPEG", quality=85)
                CacheManager.add_item('slideshow', original_path, output_path)
        except Exception as e:
            print(f"!! FAILED to transcode slideshow image {os.path.basename(original_path)}: {e}")

    with SLIDESHOW_LOCK:
        if session_id in SLIDESHOW_SESSIONS:
            SLIDESHOW_SESSIONS[session_id]['transcode_cursor'] = end_cursor
            SLIDESHOW_SESSIONS[session_id]['is_transcoding'] = False

def istream_transcode_worker():
    """Main background worker for iStream. Transcodes one video at a time from the active queue."""
    while not STOP_EVENT.is_set():
        # Pause this worker if other high-priority tasks are running
        THUMB_CRAWLER_PAUSE_EVENT.set()
        ICOMFY_LOCK.acquire() 
        ICOMFY_LOCK.release()

        task_info = None
        current_session_id = None
        
        # Safely get the next task from the active session's queue
        with ISTREAM_LOCK:
            global ACTIVE_ISTREAM_SESSION_ID
            current_session_id = ACTIVE_ISTREAM_SESSION_ID
            
            if current_session_id and current_session_id in ISTREAM_SESSIONS:
                session = ISTREAM_SESSIONS[current_session_id]
                if session.get('is_started', False):
                    session['current_task'] = None
                    # Prioritize user-requested files over the regular batch queue
                    if session['priority_queue']: task_info = session['priority_queue'].popleft()
                    elif session['queue']: task_info = session['queue'].popleft()
                    
                    if task_info:
                        session['current_task'] = task_info
                        session['progress'][task_info['original_path']] = 0

        if task_info:
            source_file_path = task_info['original_path']
            final_path = task_info['output_path']
            
            with ISTREAM_LOCK:
                if ISTREAM_SESSIONS[current_session_id].get('is_aborted', False):
                    print(f"-> Session {current_session_id} aborted. Discarding task: {os.path.basename(source_file_path)}")
                    continue

            intermediate_path = os.path.join(CACHE_DIR, f"temp_istream_{str(hash(source_file_path))}.mp4")

            try:
                if CPU_MODE:
                    # Single-pass CPU transcoding
                    print(f"-> [iStream CPU Transcode] [File: {os.path.basename(source_file_path)}]...")
                    duration = get_video_duration(source_file_path)
                    new_w, new_h = get_new_dimensions(source_file_path)
                    
                    vf_filters = []
                    if USE_THOUSANDS_COLORS: vf_filters.extend(['format=rgb565', 'format=yuv420p'])
                    vf_filters.append(f'scale={new_w}:{new_h}')

                    cpu_cmd = ['ffmpeg', '-hide_banner', '-progress', 'pipe:2', '-i', source_file_path, 
                               '-map_metadata', '-1', '-vf', ",".join(vf_filters), '-r', str(TARGET_FRAMERATE), 
                               '-c:v', 'mpeg4', '-q:v', '5', '-pix_fmt', 'yuv420p', 
                               '-c:a', 'adpcm_ima_qt', '-ar', '44100', '-ac', '1', 
                               '-movflags', '+faststart', '-y', final_path]
                    
                    process = subprocess.Popen(cpu_cmd, stderr=subprocess.PIPE, universal_newlines=True)

                    for line in process.stderr:
                        progress = parse_ffmpeg_progress(line, duration)
                        if progress is not None:
                            with ISTREAM_LOCK:
                                if current_session_id in ISTREAM_SESSIONS:
                                    ISTREAM_SESSIONS[current_session_id]['progress'][source_file_path] = progress
                    
                    process.wait()
                    if process.returncode != 0: raise subprocess.CalledProcessError(process.returncode, cpu_cmd)

                else: # GPU-accelerated two-pass transcoding
                    duration = get_video_duration(source_file_path)
                    new_w, new_h = get_new_dimensions(source_file_path)

                    vf_filters = []
                    if USE_THOUSANDS_COLORS: vf_filters.extend(['format=rgb565', 'format=yuv420p'])
                    vf_filters.append(f'scale={new_w}:{new_h}')

                    if APPLE_SILICON_MODE:
                        # Pass 1: GPU accelerated transcode to a modern format (H.264)
                        print(f"-> [iStream AppleS Pass 1] [File: {os.path.basename(source_file_path)}]...")
                        gpu_cmd = ['ffmpeg', '-hide_banner', '-progress', 'pipe:2', '-hwaccel', 'videotoolbox', '-i', source_file_path, 
                                   '-c:v', 'h264_videotoolbox', '-b:v', '4000k', 
                                   '-vf', ",".join(vf_filters), '-r', str(TARGET_FRAMERATE), 
                                   '-c:a', 'aac', '-b:a', '128k', '-y', intermediate_path]
                    else: # Default NVIDIA CUDA
                        print(f"-> [iStream NVIDIA Pass 1] [File: {os.path.basename(source_file_path)}]...")
                        gpu_cmd = ['ffmpeg', '-hide_banner', '-progress', 'pipe:2', '-hwaccel', 'cuda', '-i', source_file_path, 
                                   '-c:v', 'h264_nvenc', '-preset', 'p5', '-vf', ",".join(vf_filters), 
                                   '-r', str(TARGET_FRAMERATE), '-c:a', 'aac', '-b:a', '128k', '-y', intermediate_path]
                    
                    process_gpu = subprocess.Popen(gpu_cmd, stderr=subprocess.PIPE, universal_newlines=True)
                    
                    for line in process_gpu.stderr:
                        progress = parse_ffmpeg_progress(line, duration)
                        if progress is not None:
                            with ISTREAM_LOCK:
                                if current_session_id in ISTREAM_SESSIONS:
                                    ISTREAM_SESSIONS[current_session_id]['progress'][source_file_path] = int(progress * 0.5)
                    
                    process_gpu.wait()
                    if process_gpu.returncode != 0: raise subprocess.CalledProcessError(process_gpu.returncode, gpu_cmd)
                    
                    with ISTREAM_LOCK:
                        if ISTREAM_SESSIONS[current_session_id].get('is_aborted', False):
                            raise InterruptedError("Session aborted during Pass 1")

                    # Pass 2: CPU transcode of the intermediate file to the final vintage format
                    print(f"-> [iStream Pass 2] [File: {os.path.basename(source_file_path)}]...")
                    cpu_cmd = ['ffmpeg', '-hide_banner', '-progress', 'pipe:2', '-i', intermediate_path, '-map_metadata', '-1', '-c:v', 'mpeg4', '-q:v', '5', '-pix_fmt', 'yuv420p', '-c:a', 'adpcm_ima_qt', '-ar', '44100', '-ac', '1', '-movflags', '+faststart', '-y', final_path]
                    process_cpu = subprocess.Popen(cpu_cmd, stderr=subprocess.PIPE, universal_newlines=True)
                    
                    for line in process_cpu.stderr:
                        progress = parse_ffmpeg_progress(line, duration)
                        if progress is not None:
                            with ISTREAM_LOCK:
                                 if current_session_id in ISTREAM_SESSIONS:
                                    ISTREAM_SESSIONS[current_session_id]['progress'][source_file_path] = 50 + int(progress * 0.5)

                    process_cpu.wait()
                    if process_cpu.returncode != 0: raise subprocess.CalledProcessError(process_cpu.returncode, cpu_cmd)
                
                # Common finalization for all successful transcodes
                with ISTREAM_LOCK:
                    if current_session_id in ISTREAM_SESSIONS:
                        session = ISTREAM_SESSIONS[current_session_id]
                        session['completed'].add(source_file_path)
                        if source_file_path in session.get('progress', {}):
                            del session['progress'][source_file_path]
                
                CacheManager.add_item('istream', source_file_path, final_path)
                print(f"-> [iStream Finished] [File: {os.path.basename(source_file_path)}]")

            except InterruptedError:
                 print(f"-> Cleanly stopped task for aborted session {current_session_id}.")
            except Exception as e:
                print(f"!! FAILED to transcode [iStream]: {os.path.basename(source_file_path)}. Error: {e}")
                with ISTREAM_LOCK:
                    if current_session_id in ISTREAM_SESSIONS:
                        ISTREAM_SESSIONS[current_session_id]['failed'].add(source_file_path)
            finally:
                if os.path.exists(intermediate_path):
                    try: os.remove(intermediate_path)
                    except: pass
        else:
            # If no tasks are found, unpause the thumbnail crawler and wait
            THUMB_CRAWLER_PAUSE_EVENT.clear()
            time.sleep(1)

def itube_worker(video_url, video_id, title, user_agent, browser):
    """Worker thread to handle a single yt-dlp download job."""
    ITUBE_JOBS[video_id]['status'] = 'Downloading'
    ITUBE_JOBS[video_id]['progress'] = 0

    safe_filename = "".join([c for c in title if c.isalnum() or c in (' ', '-')]).rstrip()
    source_download_path = os.path.join(ITUBE_DIR, f"{safe_filename}.mp4")
    
    yt_cmd = ['yt-dlp', '--no-playlist', '--progress', '--cookies-from-browser', browser, '--user-agent', user_agent, '-f', 'bestvideo[height<=360]+bestaudio/best[height<=360]', '--merge-output-format', 'mp4', '--output', source_download_path, video_url]

    try:
        print(f"-> [iTube Download] Starting: {title}")
        process_dl = subprocess.Popen(yt_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        for line in process_dl.stdout:
            progress = parse_yt_dlp_progress(line)
            if progress is not None: ITUBE_JOBS[video_id]['progress'] = int(progress)
        process_dl.wait()
        if process_dl.returncode != 0: raise subprocess.CalledProcessError(process_dl.returncode, yt_cmd)
        
        print(f"-> [iTube Download] Finished: {title}")
        ITUBE_JOBS[video_id]['status'] = 'Downloaded'
        ITUBE_JOBS[video_id]['output_path'] = source_download_path

    except subprocess.CalledProcessError as e:
        print(f"!! FAILED iTube download for {video_id}. Error: {e}")
        ITUBE_JOBS[video_id]['status'] = 'Failed'
    except Exception as e:
        print(f"!! FAILED iTube download for {video_id}. Error: {e}")
        ITUBE_JOBS[video_id]['status'] = 'Failed'

def thumbnail_crawler_worker():
    """Background worker that crawls the media folder for new files and generates thumbnails."""
    print("-> Thumbnail crawler worker started.")
    time.sleep(5) 
    
    while not STOP_EVENT.is_set():
        THUMB_CRAWLER_PAUSE_EVENT.wait()
        
        with THUMB_CRAWLER_LOCK:
            try:
                for dirpath, _, filenames in os.walk(ROOT_MEDIA_FOLDER):
                    if STOP_EVENT.is_set(): break
                    # Skip common system/recycle bin folders
                    if "$RECYCLE.BIN" in dirpath or ".Trash-" in dirpath:
                        continue

                    for filename in filenames:
                        if STOP_EVENT.is_set(): break
                        THUMB_CRAWLER_PAUSE_EVENT.wait()
                        
                        full_path = os.path.join(dirpath, filename)
                        ext = os.path.splitext(filename)[1].lower()

                        if ext in VIDEO_EXTENSIONS or ext in IMAGE_EXTENSIONS or ext in TRANSCODE_IMAGE_EXT:
                            # Generate general-purpose thumbnail if it doesn't exist
                            if not CacheManager.get_item('thumbnails', full_path):
                                _generate_thumbnail(full_path, 'thumbnails', (IMAGERY_THUMBNAIL_WIDTH, 9999))
                            
                            # Generate specific iMagery thumbnail if it doesn't exist
                            if ext in IMAGE_EXTENSIONS or ext in TRANSCODE_IMAGE_EXT:
                                if not CacheManager.get_item('imagery_thumbs', full_path):
                                     _generate_thumbnail(full_path, 'imagery_thumbs', (IMAGERY_THUMBNAIL_WIDTH, 9999))

                        time.sleep(0.1) # Small delay to be system-friendly
            except Exception as e:
                 print(f"!! Thumbnail crawler encountered an error: {e}")

        print("-> Thumbnail crawler finished a full pass. Will sleep for 15 minutes.")
        time.sleep(900)

def icomfy_worker():
    """Background worker to handle ComfyUI API interactions."""
    while not STOP_EVENT.is_set():
        session_id_to_process = None
        with ICOMFY_LOCK:
            for sid, session in ICOMFY_SESSIONS.items():
                if session.get('status') == 'queued':
                    session_id_to_process = sid
                    session['status'] = 'generating'
                    break
        
        if session_id_to_process:
            with ICOMFY_LOCK:
                THUMB_CRAWLER_PAUSE_EVENT.set() # Pause crawler during generation
            
            print(f"-> [iComfy] Starting generation for session {session_id_to_process}...")
            ws = None
            try:
                session = ICOMFY_SESSIONS[session_id_to_process]
                prompt_payload = _build_comfy_prompt(session['settings'])
                
                prompt_response = _queue_comfy_prompt(prompt_payload)
                prompt_id = prompt_response.get('prompt_id')
                if not prompt_id:
                    raise ValueError(f"Failed to queue prompt. API response: {prompt_response}")

                print(f"-> [iComfy] Prompt queued with ID: {prompt_id}. Waiting for execution...")
                
                ws = websocket.create_connection(f"ws://{COMFYUI_URL}/ws?clientId={COMFY_CLIENT_ID}", timeout=60)
                
                while True:
                    try:
                        out = ws.recv()
                        if isinstance(out, str):
                            message = json.loads(out)
                            if message.get('type') == 'executing' and message.get('data', {}).get('prompt_id') == prompt_id:
                                if message['data'].get('node') is None:
                                    print("-> [iComfy] Execution complete signal received.")
                                    break # Execution is complete
                    except websocket.WebSocketTimeoutException:
                        print("-> [iComfy] WebSocket timeout, sending ping to keep alive...")
                        ws.ping()
                    except ConnectionAbortedError as e:
                        print(f"!! [iComfy] WebSocket connection aborted: {e}")
                        raise
                
                print(f"-> [iComfy] Generation complete for prompt ID {prompt_id}. Fetching history...")
                history = _get_comfy_history(prompt_id)
                history_data = history.get(prompt_id)
                if not history_data:
                    raise ValueError(f"Could not find history for prompt ID {prompt_id}. Full history: {history}")

                image_saved = False
                for node_id in history_data.get('outputs', {}):
                    node_output = history_data['outputs'][node_id]
                    if 'images' in node_output:
                        for image_info in node_output['images']:
                            image_data = _get_comfy_image(image_info.get('filename'), image_info.get('subfolder'), image_info.get('type'))
                            
                            timestamp = int(time.time())
                            image_filename = f"icomfy_{timestamp}_{random.randint(1000,9999)}.png"
                            image_path = os.path.join(ICOMFY_DIR, image_filename)
                            with open(image_path, "wb") as f:
                                f.write(image_data)
                            print(f"-> [iComfy] Image saved to {image_path}.")
                            
                            _generate_thumbnail(image_path, 'imagery_thumbs', (IMAGERY_THUMBNAIL_WIDTH, 9999))
                            
                            with ICOMFY_LOCK:
                                session['history'].append(image_path)
                                session['current_image_index'] = len(session['history']) - 1
                            image_saved = True
                
                if not image_saved:
                    print("!! [iComfy] WARNING: Workflow completed but no image was found in the output history.")

                with ICOMFY_LOCK:
                    if session['settings']['autogeneration']:
                        print("-> [iComfy] Autogeneration enabled, re-queuing.")
                        session['status'] = 'queued'
                    else:
                        print("-> [iComfy] Generation finished. Setting status to idle.")
                        session['status'] = 'idle'

            except Exception as e:
                print(f"!! [iComfy] FAILED generation for session {session_id_to_process}: {e}")
                import traceback
                traceback.print_exc()
                with ICOMFY_LOCK:
                    if session_id_to_process in ICOMFY_SESSIONS:
                        ICOMFY_SESSIONS[session_id_to_process]['status'] = 'failed'
            finally:
                if ws and ws.connected: 
                    ws.close()
                with ICOMFY_LOCK:
                    is_queue_empty = all(s.get('status') != 'queued' for s in ICOMFY_SESSIONS.values())
                    if is_queue_empty:
                        print("-> [iComfy] All jobs complete, resuming thumbnail crawler.")
                        THUMB_CRAWLER_PAUSE_EVENT.clear()
        else:
            time.sleep(1)

# --- ############################################################### ---
# ---                   HTTP SERVER HANDLER                         ---
# --- ############################################################### ---
class VintageHttpHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.color_cycler = ColorCycler()
        super().__init__(*args, **kwargs)

    def _send_html_response(self, html_content):
        """Sends a 200 OK response with the provided HTML content."""
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))

    def _serve_qtl_file(self, stream_url, download_filename):
        """Sends a QuickTime Media Link (.qtl) file to the client."""
        self.send_response(200)
        self.send_header('Content-Type', 'application/x-quicktime-media-link')
        self.send_header('Content-Disposition', f'attachment; filename="{download_filename}"')
        self.end_headers()
        qtl_content = f'<?xml version="1.0"?>\n<?quicktime type="application/x-quicktime-media-link"?>\n<embed src="{stream_url}" autoplay="true" />'
        self.wfile.write(qtl_content.encode('utf-8'))
        
    def _build_query_string(self, base_params, overrides={}):
        """Builds a URL query string from dictionaries, safely filtering empty values."""
        params = base_params.copy()
        params.update(overrides)
        filtered_params = {k: v for k, v in params.items() if v is not None and v != ''}
        return urlencode(filtered_params)

    def _get_drive_selector_html(self, current_path, action_url, query_params):
        """Generates the HTML for the drive selection dropdown menu."""
        current_drive = os.path.splitdrive(current_path)[0] + "\\" if sys.platform == "win32" else "/"
        html = f'<form action="{action_url}" method="get" style="display: inline;">'
        if 'sort_by' in query_params: html += f'<input type="hidden" name="sort_by" value="{query_params["sort_by"]}">'
        if 'sort_order' in query_params: html += f'<input type="hidden" name="sort_order" value="{query_params["sort_order"]}">'
        
        html += '<select name="path">'
        for drive in AVAILABLE_DRIVES:
            selected = 'selected' if drive == current_drive else ''
            html += f'<option value="{drive}" {selected}>{drive}</option>'
        html += '</select>'
        html += '<input type="submit" value="Mount" class="softkey-group-item">'
        html += '</form>'
        return html
    
    def _pagination_html(self, base_url, query_params, current_page, total_items, per_page):
        """Generates pagination controls (Previous/Next buttons and page info)."""
        total_pages = math.ceil(total_items / per_page)
        if total_pages <= 1:
            return ""

        softkeys_def = []
        if current_page > 1:
            softkeys_def.append((f"{base_url}?{self._build_query_string(query_params, {'page': current_page - 1})}", "Previous", ""))
        
        if current_page < total_pages:
            softkeys_def.append((f"{base_url}?{self._build_query_string(query_params, {'page': current_page + 1})}", "Next", ""))

        html = '<div class="pagination-container">'
        html += self._generate_colored_softkeys(softkeys_def)
        html += f'<span class="page-info">Page {current_page} of {total_pages}</span>'
        html += '</div>'
        return html

    def _generate_colored_softkeys(self, softkey_definitions):
        """Generates a series of styled softkey buttons with cycling colors."""
        html = ""
        for href, text, extra_classes in softkey_definitions:
            color_class = self.color_cycler.get_class()
            full_classes = f"softkey {color_class} {extra_classes or ''}"
            html += f'<a href="{href}" class="{full_classes.strip()}">{text}</a>'
        return html

    def do_GET(self):
        """Handles all GET requests by routing them to the appropriate applet handler."""
        self.color_cycler = ColorCycler() # Reset color cycle for each new page
        try:
            path, _, query_string = self.path.partition('?')
            query = parse_qs(query_string)

            # Map URL paths to their corresponding handler functions
            route_map = {
                '/': self.serve_homepage,
                '/frogfind_redirect': self.serve_frogfind_redirect,
                '/files': self.serve_files_list,
                '/imagery': self.serve_imagery,
                '/slideshow_start': self.serve_slideshow_start,
                '/slideshow': self.serve_slideshow,
                '/view_slideshow_image': self.serve_slideshow_image,
                '/thumbnail': self.serve_thumbnail,
                '/view_media': self.serve_media_handler,
                '/download': self.serve_downloadable_file,
                '/istream_start': self.serve_istream_start,
                '/istream_begin': self.serve_istream_begin,
                '/istream_reset': self.serve_istream_reset,
                '/istream_prioritize': self.serve_istream_prioritize,
                '/istream_ui': self.serve_istream_ui,
                '/istream_play': self.serve_istream_play,
                '/itube': self.serve_itube_main,
                '/itube_queue': self.serve_itube_queue,
                '/gemini': self.serve_gemini_chat,
                '/icomfy': self.serve_icomfy_main,
                '/icomfy_stop': self.serve_icomfy_stop,
                '/icomfy_image': self.serve_icomfy_image,
            }
            
            handler_func = route_map.get(path)
            if handler_func:
                handler_func(query)
            elif path.startswith('/cache/') or path.startswith('/icomfy_media/'):
                self.serve_cache_file(path)
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            print(f"!! SERVER ERROR in do_GET for path {self.path}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            self.send_error(500, f"Server Error: {e}")

    def do_POST(self):
        """Handles all POST requests for form submissions."""
        self.color_cycler = ColorCycler()
        try:
            path, _, _ = self.path.partition('?')
            if path == '/gemini_prompt': self.handle_gemini_prompt()
            elif path == '/itube_download': self.handle_itube_download()
            elif path == '/icomfy_generate': self.handle_icomfy_generate()
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            print(f"!! SERVER ERROR in do_POST for path {self.path}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            self.send_error(500, f"Server Error: {e}")


    def serve_homepage(self, query=None):
        """Serves the main landing page with links to all applets."""
        softkeys_def = [
            ('/files', 'Files', ''),
            ('/imagery', 'iMagery', ''),
            ('/istream_ui', 'iStream', ''),
            ('/itube', 'iTube', ''),
            ('/gemini', 'iGem', ''),
            ('/icomfy', 'iComfy', '')
        ]
        softkeys_html = self._generate_colored_softkeys(softkeys_def)
        
        frogfind_button_class = self.color_cycler.get_class()
        leap_button_class = self.color_cycler.get_class()

        html = get_html_header("SASI-CaTS Server", color_cycler=self.color_cycler) + f"""
            <div class="home-content">
                <form action="/frogfind_redirect" method="get" target="_blank" style="margin: 20px 0; display: flex; justify-content: center; align-items: center;">
                    <a href="/frogfind_redirect?url=http://frogfind.com" target="_blank" class="softkey {frogfind_button_class}" style="margin-right: 10px;">FrogFind!</a>
                    <input type="text" name="q" size="30">
                    <input type="submit" value="Leap!" class="softkey {leap_button_class}" style="margin-left: 5px; font-size: 12px; padding: 8px 12px;">
                </form>
                <hr>
                <div class="softkey-container">
                    <div style="display: inline-block;">
                        {softkeys_html}
                    </div>
                </div>
            </div>
        """ + HTML_FOOTER
        self._send_html_response(html)

    def serve_frogfind_redirect(self, query):
        """Redirects to the FrogFind search engine with the user's query."""
        search_query = query.get('q', [None])[0]
        direct_url = query.get('url', [None])[0]
        if search_query: target_url = f"http://frogfind.com/?q={quote(search_query)}"
        elif direct_url: target_url = direct_url
        else: self.send_error(400, "Bad Request"); return

        html = f'<html><head><title>Redirecting...</title><meta http-equiv="refresh" content="0;url={target_url}"></head><body>Redirecting...</body></html>'
        self._send_html_response(html)

    def serve_files_list(self, query):
        """Serves the 'Files' applet, a paginated list of files and directories."""
        current_path = query.get('path', [ROOT_MEDIA_FOLDER])[0]
        page = int(query.get('page', [1])[0])
        sort_by = query.get('sort_by', ['name'])[0]
        sort_order = query.get('sort_order', ['asc'])[0]

        safe_path = os.path.abspath(current_path)
        items = get_sorted_and_filtered_items(safe_path, sort_by, sort_order)
        if not items and not os.path.isdir(safe_path):
            self.send_error(404, "Directory Not Found"); return

        start_index = (page - 1) * FILES_PER_PAGE
        end_index = start_index + FILES_PER_PAGE
        page_items = items[start_index:end_index]
        
        html = get_html_header(f"Files", subtitle=safe_path, color_cycler=self.color_cycler)
        
        query_params = {'path': safe_path, 'sort_by': sort_by, 'sort_order': sort_order}

        if safe_path not in AVAILABLE_DRIVES and safe_path != ITUBE_DIR:
            parent_dir = os.path.dirname(safe_path)
            html += f'<p><a href="/files?path={quote(parent_dir)}">.. Parent Directory</a></p>'
        
        html += '<div class="controls-container">'
        html += self._get_drive_selector_html(safe_path, '/files', query_params)
        
        html += '<div style="margin-top: 5px;">Sort by: '
        html += f'<a href="/files?{self._build_query_string(query_params, {"sort_by": "name", "sort_order": "asc"})}">Name Asc</a> | '
        html += f'<a href="/files?{self._build_query_string(query_params, {"sort_by": "name", "sort_order": "desc"})}">Name Des</a> | '
        html += f'<a href="/files?{self._build_query_string(query_params, {"sort_by": "date", "sort_order": "asc"})}">Date Asc</a> | '
        html += f'<a href="/files?{self._build_query_string(query_params, {"sort_by": "date", "sort_order": "desc"})}">Date Des</a> | '
        html += f'<a href="/files?{self._build_query_string(query_params, {"sort_by": "size", "sort_order": "asc"})}">Size Asc</a> | '
        html += f'<a href="/files?{self._build_query_string(query_params, {"sort_by": "size", "sort_order": "desc"})}">Size Des</a> | '
        html += f'<a href="/files?{self._build_query_string(query_params, {"sort_by": "kind"})}">Kind</a>'
        html += '</div></div>'

        softkeys_def = [
            ('/istream_start?' + self._build_query_string({'folder': safe_path, 'sort_by': sort_by, 'sort_order': sort_order, 'type': 'batch'}), 'Start iStream Here', 'softkey-group-item'),
            ('/imagery?' + self._build_query_string(query_params), 'Start iMagery Here', 'softkey-group-item')
        ]
        html += f'<p>{self._generate_colored_softkeys(softkeys_def)}</p><hr><table>'
        
        for item in page_items:
            item_path = os.path.join(safe_path, item)
            encoded_path = quote(item_path)
            if os.path.isdir(item_path):
                html += f'<tr><td class="td-filename"><a href="/files?path={encoded_path}" class="dir-link">{item}</a></td><td></td></tr>'
            else:
                ext = os.path.splitext(item)[1].lower()
                if ext in VIDEO_EXTENSIONS:
                    istream_single_qs = self._build_query_string({'file': item_path, 'type': 'single'})
                    html += f'<tr><td class="td-filename"><a class="file-link" href="/istream_start?{istream_single_qs}">{item}</a> <a href="/view_media?path={encoded_path}&mode=preview" class="softkey softkey-small">[Prev.]</a></td>' \
                            f'<td style="width: 95px; text-align: right;"><img src="/thumbnail?path={encoded_path}&type=thumbnails" width="80" height="45" class="file-icon"></td></tr>'
                elif ext in IMAGE_EXTENSIONS or ext in TRANSCODE_IMAGE_EXT:
                    html += f'<tr><td class="td-filename"><a class="file-link" href="/view_media?path={encoded_path}">{item}</a></td>' \
                            f'<td style="width: 95px; text-align: right;"><img src="/thumbnail?path={encoded_path}&type=thumbnails" width="80" height="45" class="file-icon"></td></tr>'
                else:
                    html += f'<tr><td class="td-filename"><a class="file-link" href="/download?path={encoded_path}">{item}</a></td><td></td></tr>'
        html += "</table><hr>"

        html += self._pagination_html('/files', query_params, page, len(items), FILES_PER_PAGE)
        
        html += HTML_FOOTER
        self._send_html_response(html)

    def serve_imagery(self, query):
        """Serves the 'iMagery' applet, a grid-based photo gallery."""
        current_path = query.get('path', [ROOT_MEDIA_FOLDER])[0]
        page = int(query.get('page', [1])[0])
        sort_by = query.get('sort_by', ['name'])[0]
        sort_order = query.get('sort_order', ['asc'])[0]

        safe_path = os.path.abspath(current_path)
        allowed_ext = IMAGE_EXTENSIONS + TRANSCODE_IMAGE_EXT
        
        items = get_sorted_and_filtered_items(safe_path, sort_by, sort_order, allowed_ext)
        if not items and not os.path.isdir(safe_path):
            self.send_error(404, "Directory Not Found"); return

        start_index = (page - 1) * IMAGERY_FILES_PER_PAGE
        end_index = start_index + IMAGERY_FILES_PER_PAGE
        page_items = items[start_index:end_index]
        
        html = get_html_header(f"iMagery", subtitle=safe_path, color_cycler=self.color_cycler)
        query_params = {'path': safe_path, 'sort_by': sort_by, 'sort_order': sort_order}

        if safe_path not in AVAILABLE_DRIVES:
            parent_dir = os.path.dirname(safe_path)
            html += f'<p><a href="/imagery?path={quote(parent_dir)}">.. Parent Directory</a></p>'

        html += '<div class="controls-container">'
        html += self._get_drive_selector_html(safe_path, '/imagery', query_params)
        
        html += '<div style="margin-top: 5px;">Sort by: '
        html += f'<a href="/imagery?{self._build_query_string(query_params, {"sort_by": "name", "sort_order": "asc"})}">Name Asc</a> | '
        html += f'<a href="/imagery?{self._build_query_string(query_params, {"sort_by": "name", "sort_order": "desc"})}">Name Des</a> | '
        html += f'<a href="/imagery?{self._build_query_string(query_params, {"sort_by": "date", "sort_order": "asc"})}">Date Asc</a> | '
        html += f'<a href="/imagery?{self._build_query_string(query_params, {"sort_by": "date", "sort_order": "desc"})}">Date Des</a> | '
        html += f'<a href="/imagery?{self._build_query_string(query_params, {"sort_by": "size", "sort_order": "asc"})}">Size Asc</a> | '
        html += f'<a href="/imagery?{self._build_query_string(query_params, {"sort_by": "size", "sort_order": "desc"})}">Size Des</a>'
        html += '</div></div>'

        image_files = [item for item in items if not os.path.isdir(os.path.join(safe_path, item))]

        softkeys_def = [(f"/files?{self._build_query_string(query_params)}", 'File Browser', 'softkey-group-item')]
        if image_files:
            softkeys_def.extend([
                (f"/slideshow_start?{self._build_query_string(query_params, {'mode': 'seq'})}", 'Slideshow', 'softkey-group-item'),
                (f"/slideshow_start?{self._build_query_string(query_params, {'mode': 'rand'})}", 'Random Show', 'softkey-group-item')
            ])
        softkeys_def.extend([
            (f"/slideshow_start?{self._build_query_string(query_params, {'mode': 'seq', 'recursive': 'true'})}", 'Recursive Show', 'softkey-group-item'),
            (f"/slideshow_start?{self._build_query_string(query_params, {'mode': 'rand', 'recursive': 'true'})}", 'Recursive Random', 'softkey-group-item')
        ])
        html += f'<p>{self._generate_colored_softkeys(softkeys_def)}</p><hr><table>'
        
        for i, item in enumerate(page_items):
            if i % 3 == 0: html += "<tr>"
            item_path = os.path.join(safe_path, item)
            encoded_path = quote(item_path)
            html += "<td>"
            if os.path.isdir(item_path):
                html += f'<a href="/imagery?path={encoded_path}" class="dir-link"><div class="filename-wrapper">{item}</div></a>'
            else:
                thumb_qs = self._build_query_string({'path': item_path, 'type': 'imagery_thumbs'})
                try:
                    start_idx = image_files.index(item)
                    slideshow_qs = self._build_query_string(query_params, {"mode": "seq", "start_index": start_idx})
                    html += f'<a href="/slideshow_start?{slideshow_qs}"><img src="/thumbnail?{thumb_qs}" class="gallery-thumb"></a>'
                except ValueError:
                    html += f'<a href="/view_media?path={encoded_path}"><img src="/thumbnail?{thumb_qs}" class="gallery-thumb"></a>'
            html += "</td>"
            if (i + 1) % 3 == 0 or i == len(page_items) - 1: html += "</tr>"
        html += "</table><hr>"
        
        html += self._pagination_html('/imagery', query_params, page, len(items), IMAGERY_FILES_PER_PAGE)
        
        html += HTML_FOOTER
        self._send_html_response(html)
        
    def serve_slideshow_start(self, query):
        """Initializes a slideshow session and redirects the user to the viewer."""
        source = query.get('source', ['directory'])[0]
        mode = query.get('mode', ['seq'])[0]
        recursive = query.get('recursive', ['false'])[0].lower() == 'true'
        start_index = int(query.get('start_index', [0])[0])
        sort_by = query.get('sort_by', ['name'])[0]
        sort_order = query.get('sort_order', ['asc'])[0]

        all_images = []
        slideshow_source_id = ""
        slideshow_source_type = ""

        if source == 'icomfy':
            session_id = query.get('session_id', [None])[0]
            if not session_id or session_id not in ICOMFY_SESSIONS:
                self.send_error(404, "iComfy session not found"); return
            with ICOMFY_LOCK:
                all_images = ICOMFY_SESSIONS[session_id]['history']
            slideshow_source_id = session_id
            slideshow_source_type = 'icomfy'
        else: # Default directory behavior
            path = query.get('path', [''])[0]
            if not path or not os.path.isdir(path): self.send_error(404, "Directory not found"); return
            
            if recursive:
                all_images = get_recursive_images(path)
            else:
                allowed_ext = IMAGE_EXTENSIONS + TRANSCODE_IMAGE_EXT
                sorted_items = get_sorted_and_filtered_items(path, sort_by, sort_order, allowed_ext)
                all_images = [os.path.join(path, item) for item in sorted_items if not os.path.isdir(os.path.join(path, item))]
            slideshow_source_id = path
            slideshow_source_type = 'directory'

        if not all_images and source not in ['icomfy']:
            self._send_html_response(get_html_header("Slideshow Error", color_cycler=self.color_cycler) + "<p>No images found.</p>" + HTML_FOOTER)
            return

        session_hash_key = slideshow_source_id + mode + str(recursive) + sort_by + sort_order + str(time.time() if mode=='rand' else '')
        slideshow_session_id = str(hash(session_hash_key))

        with SLIDESHOW_LOCK:
            if slideshow_session_id not in SLIDESHOW_SESSIONS:
                playlist = all_images
                if mode == 'rand': random.shuffle(playlist)
                
                new_session = {
                    'source': slideshow_source_type,
                    'source_id': slideshow_source_id,
                    'playlist': playlist, 
                    'transcode_cursor': 0, 
                    'is_transcoding': False,
                    'is_fetching_more': False
                }
                SLIDESHOW_SESSIONS[slideshow_session_id] = new_session
                if playlist and slideshow_source_type == 'directory':
                    threading.Thread(target=slideshow_transcode_worker, args=(slideshow_session_id,), daemon=True).start()

        self.send_response(302)
        self.send_header('Location', f'/slideshow?session_id={slideshow_session_id}&index={start_index}')
        self.end_headers()

    def serve_slideshow(self, query):
        """Serves the main slideshow viewer page."""
        session_id = query.get('session_id', [''])[0]
        current_index = int(query.get('index', [0])[0])
        mode = query.get('mode', [''])[0]

        with SLIDESHOW_LOCK:
            if session_id not in SLIDESHOW_SESSIONS:
                self.send_error(404, "Slideshow session not found")
                return
            session = SLIDESHOW_SESSIONS[session_id]

            is_icomfy_slideshow = session.get('source') == 'icomfy'

            if is_icomfy_slideshow:
                icomfy_session_id = session.get('source_id')
                if icomfy_session_id in ICOMFY_SESSIONS:
                    with ICOMFY_LOCK:
                        session['playlist'] = ICOMFY_SESSIONS[icomfy_session_id]['history'][:]
            
            playlist, total_images = session['playlist'], len(session['playlist'])

        if not playlist:
            refresh_url = f'/slideshow?session_id={session_id}&index=0&mode=auto'
            refresh_tag = f'<meta http-equiv="refresh" content="5;url={refresh_url}">'
            wait_msg = "Waiting for first image to generate..." if is_icomfy_slideshow else "No images found in this search."
            html = get_html_header("Slideshow", color_cycler=self.color_cycler) + refresh_tag + f"<p>{wait_msg}</p>" + HTML_FOOTER
            self._send_html_response(html)
            return

        current_index = max(0, min(current_index, total_images - 1))
        
        is_directory_slideshow = session.get('source') == 'directory'
        if is_directory_slideshow and session.get('transcode_cursor', 0) - current_index <= SLIDESHOW_TRANSCODE_TRIGGER and session.get('transcode_cursor', 0) < total_images:
            threading.Thread(target=slideshow_transcode_worker, args=(session_id,)).start()

        image_url = f"/view_slideshow_image?session_id={session_id}&index={current_index}"


        prev_index = (current_index - 1 + total_images) % total_images
        next_index = (current_index + 1) % total_images
        
        refresh_tag = ""
        if mode == 'auto':
            is_at_end_of_playlist = current_index >= total_images - 1
            
            if is_at_end_of_playlist and is_icomfy_slideshow:
                icomfy_session_id = session.get('source_id')
                icomfy_session_status = ''
                icomfy_autogeneration_enabled = False
                if icomfy_session_id in ICOMFY_SESSIONS:
                    with ICOMFY_LOCK:
                        icomfy_session = ICOMFY_SESSIONS[icomfy_session_id]
                        icomfy_session_status = icomfy_session['status']
                        icomfy_autogeneration_enabled = icomfy_session.get('settings', {}).get('autogeneration', False)
                
                if icomfy_session_status in ['generating', 'queued'] or icomfy_autogeneration_enabled:
                    refresh_tag = f'<meta http-equiv="refresh" content="5;url=/slideshow?session_id={session_id}&index={current_index}&mode=auto">'
            
            elif not is_at_end_of_playlist:
                refresh_tag = f'<meta http-equiv="refresh" content="5;url=/slideshow?session_id={session_id}&index={next_index}&mode=auto">'
        
        close_button_url = "/" 
        if session.get('source') == 'icomfy':
            close_button_url = f"/icomfy?session_id={session.get('source_id')}"
        elif session.get('source') == 'directory':
            close_button_url = f"/imagery?path={quote(session.get('source_id'))}"
        
        softkeys_def = [
            (f"/slideshow?session_id={session_id}&index={prev_index}&mode={mode}", '&lt;', 'softkey-slideshow'),
            (f"/slideshow?session_id={session_id}&index={current_index}", 'Pause', 'softkey-slideshow'),
            (close_button_url, 'X', 'softkey-slideshow'),
            (f"/slideshow?session_id={session_id}&index={current_index}&mode=auto", 'Play', 'softkey-slideshow'),
            (f"/slideshow?session_id={session_id}&index={next_index}&mode={mode}", '&gt;', 'softkey-slideshow')
        ]
        controls_html = self._generate_colored_softkeys(softkeys_def)
        
        view_media_href = "#"
        if is_directory_slideshow:
            original_image_path = playlist[current_index]
            view_media_href = f"/view_media?path={quote(original_image_path)}"
        elif is_icomfy_slideshow:
             icomfy_session_id = session.get('source_id')
             view_media_href = f"/icomfy?session_id={icomfy_session_id}&nav_to={current_index}"


        html = f"""<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
        <html><head><title>Slideshow</title>{CSS_STYLES}{refresh_tag}</head><body style="overflow: hidden;">
        <div class="slideshow-container">
            <div class="slideshow-img-container"><a href="{view_media_href}"><img class="slideshow-img" src="{image_url}"></a></div>
            <div class="slideshow-controls">{controls_html}</div>
        </div></body></html>"""
        self._send_html_response(html)

    def serve_slideshow_image(self, query):
        """Serves a single image for the slideshow, transcoding it on the fly if needed."""
        session_id = query.get('session_id', [''])[0]
        index = int(query.get('index', [-1])[0])
        if not session_id or index == -1: self.send_error(400, "Bad Request"); return

        with SLIDESHOW_LOCK:
            if session_id not in SLIDESHOW_SESSIONS or index >= len(SLIDESHOW_SESSIONS[session_id]['playlist']):
                self.send_error(404, "Image not found in session"); return
            original_path = SLIDESHOW_SESSIONS[session_id]['playlist'][index]

        cached_path = CacheManager.get_item('slideshow', original_path)
        if not cached_path:
            output_path = get_cache_path(original_path, 'slideshow', '.jpg')
            try:
                img_source = original_path
                
                with Image.open(img_source) as img:
                    img = scale_image_to_megapixel(img)
                    if img.mode not in ('RGB', 'L'): img = img.convert('RGB')
                    img.save(output_path, "JPEG", quality=85)
                    CacheManager.add_item('slideshow', original_path, output_path)
                    cached_path = output_path
            except Exception as e:
                print(f"!! Slideshow image processing failed for {original_path}: {e}")
                self.send_error(500, "Image processing failed"); return
        
        relative_cache_path = os.path.relpath(cached_path, CACHE_DIR).replace('\\', '/')
        self.send_response(302)
        self.send_header('Location', f'/cache/{relative_cache_path}')
        self.end_headers()

    def serve_thumbnail(self, query):
        """Serves a cached thumbnail, generating it if it doesn't exist."""
        path = query.get('path', [''])[0]
        thumb_type = query.get('type', ['thumbnails'])[0] 

        if not path or not os.path.exists(path): self.send_error(404); return
        cached_thumb_path = CacheManager.get_item(thumb_type, path)
        if not cached_thumb_path:
            size = (IMAGERY_THUMBNAIL_WIDTH, 9999)
            cached_thumb_path = _generate_thumbnail(path, thumb_type, size)
        
        if cached_thumb_path and os.path.exists(cached_thumb_path):
            relative_cache_path = os.path.relpath(cached_thumb_path, CACHE_DIR).replace('\\', '/')
            self.send_response(302)
            self.send_header('Location', f'/cache/{relative_cache_path}')
            self.end_headers()
        else:
            self.send_error(500, "Thumbnail generation failed")

    def serve_media_handler(self, query):
        """Serves a raw media file or a transcoded preview."""
        path = query.get('path', [''])[0]
        mode = query.get('mode', ['full'])[0]
        if not path or not os.path.exists(path): self.send_error(404); return
        ext = os.path.splitext(path)[1].lower()

        if ext in IMAGE_EXTENSIONS: 
            content_type = {'gif': 'image/gif', 'png': 'image/png', 'webp': 'image/webp'}.get(ext.strip('.'), 'image/jpeg')
            self.send_response(200); self.send_header('Content-Type', content_type); self.end_headers()
            with open(path, 'rb') as f: self.wfile.write(f.read())
        elif ext in TRANSCODE_IMAGE_EXT:
            cached_path = CacheManager.get_item('slideshow', path)
            if not cached_path:
                 cached_path = get_cache_path(path, 'slideshow', '.jpg')
                 subprocess.run(['ffmpeg', '-i', path, '-frames:v', '1', '-y', cached_path])
                 CacheManager.add_item('slideshow', path, cached_path)
            
            relative_cache_path = os.path.relpath(cached_path, CACHE_DIR).replace('\\', '/')
            self.send_response(302)
            self.send_header('Location', f'/cache/{relative_cache_path}')
            self.end_headers()

        elif ext in VIDEO_EXTENSIONS and mode == 'preview':
            cached_preview_path = CacheManager.get_item('previews', path)
            if not cached_preview_path:
                cached_preview_path = get_cache_path(path, 'previews', '.mov')
                try:
                    new_w, new_h = get_new_dimensions(path, MAX_PIXELS_PREVIEW)
                    subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-i', path, '-vf', f"fps={TARGET_FRAMERATE_PREVIEW},scale={new_w}:{new_h}:flags=lanczos", '-c:v', 'mpeg4', '-b:v', VIDEO_BITRATE_PREVIEW, '-an', '-movflags', '+faststart', '-y', cached_preview_path], check=True)
                    CacheManager.add_item('previews', path, cached_preview_path)
                except Exception as e: self.send_error(500, f"Preview transcoding failed: {e}"); return
            
            relative_cache_path = os.path.relpath(cached_preview_path, CACHE_DIR).replace('\\', '/')
            stream_url = f"http://{self.server.server_address[0]}:{PORT}/cache/{relative_cache_path}"
            self._serve_qtl_file(stream_url, "play_preview.qtl")
        else: self.send_error(415, "Unsupported File Type or Mode")

    def serve_downloadable_file(self, query):
        """Serves a file as an attachment for direct download."""
        path = query.get('path', [''])[0]
        if not path or not os.path.exists(path): self.send_error(404); return
        safe_path = os.path.abspath(path)
        if not any(safe_path.startswith(os.path.abspath(d)) for d in AVAILABLE_DRIVES): self.send_error(403); return
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Disposition', f'attachment; filename="{os.path.basename(safe_path)}"')
        self.send_header('Content-Length', str(os.path.getsize(safe_path)))
        self.end_headers()
        with open(safe_path, 'rb') as f: shutil.copyfileobj(f, self.wfile)

    def serve_istream_start(self, query):
        """Initializes an iStream session from the Files or iTube applet."""
        session_type = query.get('type', ['batch'])[0]
        
        playlist = []
        source_dir = None

        if session_type == 'batch':
            source_dir = query.get('folder', [''])[0]
            sort_by = query.get('sort_by', ['name'])[0]
            sort_order = query.get('sort_order', ['asc'])[0]
            if not source_dir or not os.path.isdir(source_dir):
                self.send_error(404, "Folder not found"); return
            
            sorted_items = get_sorted_and_filtered_items(source_dir, sort_by, sort_order, VIDEO_EXTENSIONS)
            playlist = [os.path.join(source_dir, item) for item in sorted_items if not os.path.isdir(os.path.join(source_dir, item))]
        
        elif session_type == 'single':
            single_file = query.get('file', [''])[0]
            if not single_file or not os.path.exists(single_file):
                self.send_error(404, "File not found"); return
            playlist = [single_file]
            source_dir = os.path.dirname(single_file)
        
        elif session_type == 'itube':
            itube_file = query.get('file', [''])[0]
            if not itube_file or not os.path.exists(itube_file):
                self.send_error(404, "iTube file not found"); return
            playlist = [itube_file]
            source_dir = ITUBE_DIR
            
        else:
            self.send_error(400, "Invalid iStream session type"); return
        
        new_session_id = _create_new_istream_session(session_type, source_dir, playlist)

        self.send_response(302)
        self.send_header('Location', f'/istream_ui?session_id={new_session_id}')
        self.end_headers()

    def serve_istream_begin(self, query):
        """Starts the transcoding process for the active iStream session."""
        session_id = query.get('session_id', [None])[0]
        with ISTREAM_LOCK:
            if session_id and session_id in ISTREAM_SESSIONS:
                ISTREAM_SESSIONS[session_id]['is_started'] = True
                print(f"-> iStream session {session_id} has been started by user.")
        self.send_response(302)
        self.send_header('Location', f'/istream_ui?session_id={session_id}')
        self.end_headers()

    def serve_istream_reset(self, query=None):
        """Aborts the current iStream session and clears its queues."""
        with ISTREAM_LOCK:
            global ACTIVE_ISTREAM_SESSION_ID
            if ACTIVE_ISTREAM_SESSION_ID:
                session = ISTREAM_SESSIONS[ACTIVE_ISTREAM_SESSION_ID]
                if session.get('current_task'):
                    print(f"-> iStream reset queued. Will occur after current file finishes.")
                    session['queue'].clear()
                    session['priority_queue'].clear()
                    session['is_aborted'] = True 
                else:
                    print("-> Performing immediate iStream reset.")
                    _reset_active_istream_session()
        
        self.send_response(302)
        self.send_header('Location', f'/istream_ui')
        self.end_headers()
        
    def serve_istream_prioritize(self, query):
        """Moves a specific video to the front of the iStream transcoding queue."""
        file_to_prio = query.get('file', [None])[0]
        session_id = query.get('session_id', [None])[0]
        
        if not file_to_prio or not session_id:
            self.send_error(400, "Missing parameters for prioritization")
            return
            
        with ISTREAM_LOCK:
            _prioritize_istream_task(session_id, file_to_prio)
            if session_id in ISTREAM_SESSIONS and not ISTREAM_SESSIONS[session_id].get('is_started'):
                ISTREAM_SESSIONS[session_id]['is_started'] = True
                print(f"-> iStream session {session_id} started via prioritization.")

        self.send_response(302)
        self.send_header('Location', f'/istream_ui?session_id={session_id}')
        self.end_headers()
        
    def serve_istream_ui(self, query):
        """Serves the main user interface for the iStream applet, showing queue status."""
        session_id = query.get('session_id', [None])[0]
        if not session_id:
             with ISTREAM_LOCK:
                session_id = ACTIVE_ISTREAM_SESSION_ID

        if not session_id or session_id not in ISTREAM_SESSIONS:
            html = get_html_header("iStream", color_cycler=self.color_cycler) + "<p>No active iStream session. Please start one from the <a href='/files'>Files</a> browser.</p>" + HTML_FOOTER
            self._send_html_response(html)
            return

        with ISTREAM_LOCK:
            session = ISTREAM_SESSIONS.get(session_id, {})
            playlist = session.get('playlist', [])
            completed_set = session.get('completed', set())
            failed_set = session.get('failed', set())
            current_task_info = session.get('current_task')
            progress_dict = session.get('progress', {})
            source_dir = session.get('source_dir', '')
            is_active = (session_id == ACTIVE_ISTREAM_SESSION_ID)
            is_started = session.get('is_started', False)
        
        page = int(query.get('page', [1])[0])
        start_index = (page - 1) * ISTREAM_ITEMS_PER_PAGE
        end_index = start_index + ISTREAM_ITEMS_PER_PAGE
        
        html = get_html_header("iStream", subtitle=source_dir, refresh_interval=5, color_cycler=self.color_cycler)
        
        dir_qs = self._build_query_string({'path': source_dir})
        softkeys_def = [
            ('/istream_reset', 'Reset', 'softkey-group-item'),
            (f'/files?{dir_qs}', 'Return to DIR', 'softkey-group-item')
        ]
        html += f'<div style="text-align: right; margin-bottom: 10px;">{self._generate_colored_softkeys(softkeys_def)}</div>'

        with ISTREAM_LOCK:
            display_queue_tasks = list(session.get('priority_queue', [])) + list(session.get('queue',[]))
            display_queue_paths = [task['original_path'] for task in display_queue_tasks]
        
        total_files = len(playlist)
        pending_count = len(display_queue_paths)
        completed_count = len(completed_set)
        progress_percent = int((completed_count / total_files) * 100) if total_files > 0 else 0

        html += f"""
        <p><b>Overall Progress:</b> {completed_count} of {total_files} files complete. ({pending_count} pending)</p>
        <div class="progress-bar-bg">
            <div class="progress-bar-fg" style="width: {progress_percent}%;">&nbsp;{progress_percent}%&nbsp;</div>
        </div><hr>"""

        html += "<table>"
        found_first_queued_item = False
        for i, path in enumerate(playlist[start_index:end_index]):
            status = "---"; action = "---"; status_details = ""
            
            cached_video = CacheManager.get_item('istream', path)
            
            if cached_video or path in completed_set:
                status = "Completed"
                play_qs = self._build_query_string({'path': path})
                action = f'<a href="/istream_play?{play_qs}" class="action-link">&#9654; Play</a>'
            elif path in failed_set:
                status = "Failed"
            elif current_task_info and current_task_info.get('original_path') == path:
                status = "Transcoding..."
                action = "In Progress"
                individual_progress = progress_dict.get(path, 0)
                status_details = f"""<div class="progress-bar-bg"><div class="progress-bar-fg" style="width: {individual_progress}%;">&nbsp;{individual_progress}%&nbsp;</div></div>"""
            elif path in display_queue_paths:
                status = "Queued"
                if is_active:
                    prio_qs = self._build_query_string({'session_id': session_id, 'file': path})
                    if not is_started and not found_first_queued_item:
                         start_qs = self._build_query_string({'session_id': session_id})
                         action = f'<a href="/istream_begin?{start_qs}" class="action-link">Start</a>'
                         found_first_queued_item = True
                    else:
                        btn_text = "Prioritize & Start" if not is_started else "Prioritize"
                        action = f'<a href="/istream_prioritize?{prio_qs}" class="action-link">{btn_text}</a>'
            
            thumb_qs = self._build_query_string({'path': path, 'type': 'thumbnails'})
            html += f'<tr><td style="width:95px;"><img src="/thumbnail?{thumb_qs}" width="80" height="45" align="left" class="file-icon"></td>' \
                    f'<td class="td-filename">{os.path.basename(path)}<br><span class="status-text">{status}</span>{status_details}</td><td style="width:110px; text-align:right;">{action}</td></tr>'

        html += "</table><hr>"

        query_params = {'session_id': session_id}
        html += self._pagination_html('/istream_ui', query_params, page, len(playlist), ISTREAM_ITEMS_PER_PAGE)
        
        html += HTML_FOOTER
        self._send_html_response(html)
        
    def serve_istream_play(self, query):
        """Serves the .qtl file to play a transcoded iStream video."""
        path = query.get('path', [''])[0]
        if not path: self.send_error(400, "Invalid request"); return
        
        cached_video_path = CacheManager.get_item('istream', path)
        if not cached_video_path:
            self.send_error(404, "Transcoded video not found in cache. It may still be in the queue.")
            return

        relative_cache_path = os.path.relpath(cached_video_path, CACHE_DIR).replace('\\', '/')
        stream_url = f"http://{self.server.server_address[0]}:{PORT}/cache/{relative_cache_path}"
        self._serve_qtl_file(stream_url, f"play.qtl")

    def serve_cache_file(self, path):
        """Serves a file directly from the cache directory."""
        if '..' in path:
            self.send_error(400, "Invalid path")
            return

        base_dir = ICOMFY_DIR if path.startswith('/icomfy_media/') else CACHE_DIR
        file_path_segment = path.lstrip('/icomfy_media/').lstrip('/cache/')
        file_path = os.path.abspath(os.path.join(base_dir, file_path_segment))

        if not file_path.startswith(os.path.abspath(base_dir)) or not os.path.exists(file_path):
            self.send_error(404, f"File not found in cache")
            return

        content_type = 'video/quicktime'
        if file_path.lower().endswith(('.jpg', '.jpeg')): content_type = 'image/jpeg'
        elif file_path.lower().endswith('.gif'): content_type = 'image/gif'
        elif file_path.lower().endswith('.png'): content_type = 'image/png'

        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(os.path.getsize(file_path)))
        self.end_headers()
        with open(file_path, 'rb') as f:
            shutil.copyfileobj(f, self.wfile)
            
    def serve_itube_main(self, query=None):
        """Serves the main iTube interface for downloading videos."""
        softkeys_def = [
            ('/itube_queue', 'View Queue', ''),
            (f'/files?path={quote(ITUBE_DIR)}', 'Browse Downloads', '')
        ]
        softkeys_html = self._generate_colored_softkeys(softkeys_def)
        default_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        html = get_html_header("iTube", color_cycler=self.color_cycler) + f"""
        <form action="/itube_download" method="post">
            <table><tr><td><b>URL:</b></td><td><input type="text" name="yt_url" size="40"></td></tr>
            <tr><td><b>Browser:</b></td><td><select name="browser"><option value="firefox">Firefox</option><option value="chrome">Chrome</option><option value="edge">Edge</option></select></td></tr>
            <tr><td><b>User-Agent:</b></td><td><input type="text" name="user_agent" size="40" value="{default_user_agent}"></td></tr></table>
            <input type="submit" value="Download"></form><hr>
        {softkeys_html}
        """ + HTML_FOOTER
        self._send_html_response(html)

    def serve_itube_queue(self, query=None):
        """Displays the current queue and status of iTube downloads."""
        active_jobs = any(job['status'] == 'Downloading' for job in ITUBE_JOBS.values())
        softkeys_def = [('/itube', 'Back to iTube', '')]
        softkeys_html = self._generate_colored_softkeys(softkeys_def)

        html = get_html_header("iTube Download Queue", refresh_interval=5 if active_jobs else None, color_cycler=self.color_cycler)
        html += f'<p>{softkeys_html}</p><hr>'
        html += "<h3>Download Queue</h3><table>"

        if not ITUBE_JOBS:
            html += "<tr><td>No videos have been downloaded yet.</td></tr>"
        else:
            sorted_jobs = sorted(ITUBE_JOBS.items(), key=lambda item: item[1].get('timestamp', 0))
            for i, (video_id, job) in enumerate(sorted_jobs):
                status = job.get('status', 'Unknown')
                progress = job.get('progress', 0)
                action = "---"
                status_details = ""

                if status == 'Downloaded':
                    itube_qs = self._build_query_string({'file': job["output_path"], 'type': 'itube'})
                    action = f'<a href="/istream_start?{itube_qs}" class="softkey">iStream</a>'
                elif status == 'Downloading':
                    action = "In Progress..."
                    status_details = f"""<div class="progress-bar-bg"><div class="progress-bar-fg" style="width: {progress}%;">&nbsp;{progress}%&nbsp;</div></div>"""
                
                html += f"<tr><td>{job['title']}<br><span class='status-text'>{status}</span>{status_details}</td><td>{action}</td></tr>"

        html += "</table>" + HTML_FOOTER
        self._send_html_response(html)

    def handle_itube_download(self):
        """Handles the POST request to start a new yt-dlp download."""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        params = parse_qs(post_data)
        
        yt_url = params.get('yt_url', [''])[0]
        browser = params.get('browser', ['firefox'])[0]
        user_agent = params.get('user_agent', [''])[0]

        if yt_url and browser and user_agent:
            try:
                print(f"-> Fetching info for YouTube URL: {yt_url}")
                info_cmd = ['yt-dlp', '--no-playlist', '--cookies-from-browser', browser, '--user-agent', user_agent, '--get-id', '--get-title', yt_url]
                result = subprocess.run(info_cmd, capture_output=True, text=True, check=True)
                
                lines = result.stdout.strip().split('\n')
                video_id = lines[0]
                title = " ".join(lines[1:])

                if video_id not in ITUBE_JOBS:
                    ITUBE_JOBS[video_id] = {
                        'status': 'Queued', 'title': title, 'url': yt_url,
                        'timestamp': time.time(), 'progress': 0
                    }
                    threading.Thread(target=itube_worker, args=(yt_url, video_id, title, user_agent, browser), daemon=True).start()
                else:
                    print(f"-> Video {video_id} is already in the queue.")

            except subprocess.CalledProcessError as e:
                print(f"!! Failed to get info from yt-dlp. It's likely the video is unavailable or the provided auth details are incorrect.")
            except Exception as e:
                print(f"!! An unexpected error occurred while handling iTube download: {e}")

        self.send_response(302); self.send_header('Location', '/itube_queue'); self.end_headers()

    def serve_gemini_chat(self, query):
        """Serves the iGem chat interface, creating a new session if one doesn't exist."""
        session_id = query.get('session_id', [None])[0]
        if not session_id or session_id not in GEMINI_SESSIONS:
            session_id = ''.join(random.choices("abcdef0123456789", k=16))
            GEMINI_SESSIONS[session_id] = {'history': []}
            self.send_response(302); self.send_header('Location', f'/gemini?session_id={session_id}'); self.end_headers()
            return
        
        softkeys_def = [('/gemini', '[ New Chat ]', '')]
        softkeys_html = self._generate_colored_softkeys(softkeys_def)
        history_html = "".join([f"<p><b>You:</b> {h['user']}<br><b>Gemini:</b> {h['gemini']}</p>" for h in GEMINI_SESSIONS[session_id]['history']])
        html = get_html_header("iGem Terminal", is_igem=True, color_cycler=self.color_cycler)
        html += f"""
            {softkeys_html}<hr>
            <div class="igem-terminal-history">{history_html}</div><hr>
            <form action="/gemini_prompt" method="post">
                <input type="hidden" name="session_id" value="{session_id}">
                <textarea id="prompt-input" name="prompt" rows="4"></textarea><br>
                <input type="submit" value="Send">
            </form>
            <script>
                document.getElementById('prompt-input').addEventListener('keydown', function(event) {{
                    if (event.keyCode === 13 && !event.shiftKey) {{
                        event.preventDefault();
                        this.form.submit();
                    }}
                }});
            </script>
        """
        html += HTML_FOOTER
        self._send_html_response(html)

    def handle_gemini_prompt(self):
        """Handles the POST request from the iGem chat form, sending the prompt to the Gemini API."""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        params = parse_qs(post_data)
        session_id = params.get('session_id', [None])[0]
        user_prompt = params.get('prompt', [""])[0]

        if not session_id or session_id not in GEMINI_SESSIONS or not user_prompt:
            self.send_response(302); self.send_header('Location', '/gemini'); self.end_headers()
            return
        
        # Build the conversation history payload for the API
        history_payload = []
        for turn in GEMINI_SESSIONS[session_id]['history']:
            history_payload.append({'role': 'user', 'parts': [{'text': turn['user']}]})
            history_payload.append({'role': 'model', 'parts': [{'text': turn['gemini']}]})

        payload = {"contents": history_payload + [{'role': 'user', 'parts': [{'text': user_prompt}]}]}

        try:
            response = requests.post(GEMINI_API_URL, json=payload, timeout=60)
            response.raise_for_status()
            gemini_response = response.json()['candidates'][0]['content']['parts'][0]['text'].replace('\n', '<br>')
        except Exception as e:
            gemini_response = f"ERROR: Could not contact Gemini API. {e}"

        GEMINI_SESSIONS[session_id]['history'].append({'user': user_prompt, 'gemini': gemini_response})
        self.send_response(302); self.send_header('Location', f'/gemini?session_id={session_id}'); self.end_headers()

    def serve_icomfy_main(self, query):
        """Serves the iComfy main interface for generating images with ComfyUI."""
        session_id = query.get('session_id', [None])[0]
        nav_to = query.get('nav_to', [None])[0]

        if nav_to is not None and session_id in ICOMFY_SESSIONS:
            with ICOMFY_LOCK:
                try:
                    ICOMFY_SESSIONS[session_id]['current_image_index'] = int(nav_to)
                except (ValueError, IndexError):
                    pass # Ignore if nav_to is invalid
            self.send_response(302)
            self.send_header('Location', f'/icomfy?session_id={session_id}')
            self.end_headers()
            return

        if not session_id or session_id not in ICOMFY_SESSIONS:
            session_id = ''.join(random.choices("abcdef0123456789", k=16))
            ICOMFY_SESSIONS[session_id] = _get_default_icomfy_session()
            self.send_response(302)
            self.send_header('Location', f'/icomfy?session_id={session_id}')
            self.end_headers()
            return
        
        session = ICOMFY_SESSIONS[session_id]
        settings = session['settings']
        refresh_interval = 5 if session['status'] in ['queued', 'generating'] else None
        
        html = get_html_header("iComfy", refresh_interval=refresh_interval, color_cycler=self.color_cycler)
        html += '<form action="/icomfy_generate" method="post">'
        html += f'<input type="hidden" name="session_id" value="{session_id}">'

        # Image Viewer
        html += '<div class="icomfy-viewer">'
        if session['history']:
            img_index = session['current_image_index']
            img_path = session['history'][img_index]
            
            thumb_qs = self._build_query_string({'path': img_path, 'type': 'imagery_thumbs'})
            img_src = f"/thumbnail?{thumb_qs}"

            slideshow_qs = self._build_query_string({
                'source': 'icomfy', 
                'session_id': session_id, 
                'start_index': img_index
            })

            total_images = len(session['history'])
            prev_idx = (img_index - 1 + total_images) % total_images
            next_idx = (img_index + 1) % total_images
            
            html += f'<a href="/icomfy?session_id={session_id}&nav_to={prev_idx}" class="softkey softkey-small">&lt;</a>'
            html += f'<span> Image {img_index + 1} of {total_images} </span>'
            html += f'<a href="/icomfy?session_id={session_id}&nav_to={next_idx}" class="softkey softkey-small">&gt;</a><br>'
            
            blinking_class = "blinking" if session['status'] == 'generating' else ""
            html += f'<a href="/slideshow_start?{slideshow_qs}"><img src="{img_src}" class="icomfy-thumb {blinking_class}"></a>'
        else:
            html += "<i>No image generated yet.</i>"
        html += '</div>'

        # Status Box
        if session['status'] != 'idle':
            html += f'<div class="status-box">Status: {session["status"].capitalize()}...</div>'

        # Parameter Controls
        def _select(name, options, selected):
            s = f'<select name="{name}">'
            for opt in options:
                sel = 'selected' if opt == selected else ''
                s += f'<option value="{quote(opt)}" {sel}>{os.path.basename(opt)}</option>'
            s += '</select>'
            return s
        
        html += '<div class="icomfy-controls-grid">'
        # --- Left Column
        html += '<div>'
        html += '<div class="icomfy-control-group"><label>Model</label>' + _select('model', COMFY_DATA['models'], settings['model']) + '</div>'
        html += '<div class="icomfy-control-group"><label>VAE</label>' + _select('vae', COMFY_DATA['vaes'], settings['vae']) + '</div>'
        html += '<div class="icomfy-control-group"><label>Sampler</label>' + _select('sampler_name', COMFY_DATA['samplers'], settings['sampler_name']) + '</div>'
        html += '<div class="icomfy-control-group"><label>Scheduler</label>' + _select('scheduler', COMFY_DATA['schedulers'], settings['scheduler']) + '</div>'
        html += '</div>' # end left column
        
        # --- Right Column
        html += '<div>'
        html += '<div class="icomfy-control-group"><label>Width</label><input type="number" name="width" value="' + str(settings['width']) + '"></div>'
        html += '<div class="icomfy-control-group"><label>Height</label><input type="number" name="height" value="' + str(settings['height']) + '"></div>'
        html += '<div class="icomfy-control-group"><label>Steps</label><input type="number" name="steps" value="' + str(settings['steps']) + '"></div>'
        html += '<div class="icomfy-control-group"><label>CFG Scale</label><input type="number" step="0.1" name="cfg" value="' + str(settings['cfg']) + '"></div>'
        html += '<div class="icomfy-control-group"><label>Denoise</label><input type="number" step="0.05" name="denoise" value="' + str(settings['denoise']) + '"></div>'
        html += '</div>' # end right column
        html += '</div>' # end grid

        # Prompts
        html += "<h3>Prompts</h3>"
        html += '<div class="icomfy-control-group"><label>Positive Prompt</label><textarea name="positive_prompt">' + settings['positive_prompt'] + '</textarea></div>'
        html += '<div class="icomfy-control-group"><label>Negative Prompt</label><textarea name="negative_prompt">' + settings['negative_prompt'] + '</textarea></div>'
        
        # LoRAs
        html += "<h3>LoRAs</h3>"
        html += '<div class="icomfy-controls-grid">'
        html += '<div><div class="icomfy-control-group"><label>LoRA 1</label>' + _select('lora1_name', ['None'] + COMFY_DATA['loras'], settings['lora1_name']) + '</div></div>'
        html += '<div><div class="icomfy-control-group"><label>Strength</label><input type="number" step="0.1" name="lora1_strength" value="' + str(settings['lora1_strength']) + '"></div></div>'
        html += '<div><div class="icomfy-control-group"><label>LoRA 2</label>' + _select('lora2_name', ['None'] + COMFY_DATA['loras'], settings['lora2_name']) + '</div></div>'
        html += '<div><div class="icomfy-control-group"><label>Strength</label><input type="number" step="0.1" name="lora2_strength" value="' + str(settings['lora2_strength']) + '"></div></div>'
        html += '</div>'
        
        # HiRes Fix
        hires_checked = 'checked' if settings['hires_enabled'] else ''
        html += f'<h3>HiRes Fix <input type="checkbox" name="hires_enabled" {hires_checked}></h3>'
        html += '<div class="icomfy-controls-grid">'
        html += '<div><div class="icomfy-control-group"><label>HiRes Steps</label><input type="number" name="hires_steps" value="' + str(settings['hires_steps']) + '"></div></div>'
        html += '<div><div class="icomfy-control-group"><label>HiRes Denoise</label><input type="number" step="0.05" name="hires_denoise" value="' + str(settings['hires_denoise']) + '"></div></div>'
        html += '</div>'
        
        # Generate Button & Autogen
        html += '<hr>'
        autogen_checked = 'checked' if settings['autogeneration'] else ''
        is_generating = session['status'] in ['queued', 'generating']

        if is_generating and settings['autogeneration']:
            stop_qs = self._build_query_string({'session_id': session_id})
            stop_button_class = self.color_cycler.get_class()
            html += f'<a href="/icomfy_stop?{stop_qs}" class="softkey stop-button {stop_button_class}">Stop Autogeneration</a>'
        else:
            submit_disabled = 'disabled' if is_generating else ''
            generate_button_class = self.color_cycler.get_class()
            html += f'<input type="submit" value="Generate" class="softkey generate-button {generate_button_class}" {submit_disabled}>'
        
        html += f'<div style="text-align: center; margin-top: 5px;"><input type="checkbox" name="autogeneration" id="autogen" {autogen_checked}><label for="autogen"> Autogenerate</label></div>'

        html += '</form>'
        html += HTML_FOOTER
        self._send_html_response(html)

    def serve_icomfy_stop(self, query):
        """Stops an active iComfy autogeneration session."""
        session_id = query.get('session_id', [None])[0]
        if session_id and session_id in ICOMFY_SESSIONS:
            with ICOMFY_LOCK:
                ICOMFY_SESSIONS[session_id]['settings']['autogeneration'] = False
            print(f"-> [iComfy] Autogeneration stopped by user for session {session_id}.")
        
        self.send_response(302)
        self.send_header('Location', f'/icomfy?session_id={session_id}')
        self.end_headers()

    def serve_icomfy_image(self, query):
        """Serves a generated iComfy image from its history."""
        session_id = query.get('session_id', [''])[0]
        index = int(query.get('index', [-1])[0])
        if not session_id or index == -1: self.send_error(400); return

        with ICOMFY_LOCK:
            if session_id not in ICOMFY_SESSIONS or index >= len(ICOMFY_SESSIONS[session_id]['history']):
                self.send_error(404); return
            image_path = ICOMFY_SESSIONS[session_id]['history'][index]
        
        relative_path = os.path.relpath(image_path, ICOMFY_DIR).replace('\\', '/')
        self.send_response(302)
        self.send_header('Location', f'/icomfy_media/{relative_path}')
        self.end_headers()

    def handle_icomfy_generate(self):
        """Handles the POST request from the iComfy form to start a generation task."""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        params = parse_qs(post_data)
        
        session_id = params.get('session_id', [''])[0]
        if not session_id or session_id not in ICOMFY_SESSIONS:
            self.send_error(400, "Invalid session"); return

        with ICOMFY_LOCK:
            session = ICOMFY_SESSIONS[session_id]
            s = session['settings']
            # Update all settings from the form data
            s['model'] = unquote_plus(params.get('model', [s['model']])[0])
            s['vae'] = unquote_plus(params.get('vae', [s['vae']])[0])
            s['sampler_name'] = unquote_plus(params.get('sampler_name', [s['sampler_name']])[0])
            s['scheduler'] = unquote_plus(params.get('scheduler', [s['scheduler']])[0])
            s['positive_prompt'] = params.get('positive_prompt', [s['positive_prompt']])[0]
            s['negative_prompt'] = params.get('negative_prompt', [s['negative_prompt']])[0]
            s['width'] = int(params.get('width', [s['width']])[0])
            s['height'] = int(params.get('height', [s['height']])[0])
            s['steps'] = int(params.get('steps', [s['steps']])[0])
            s['cfg'] = float(params.get('cfg', [s['cfg']])[0])
            s['denoise'] = float(params.get('denoise', [s['denoise']])[0])
            s['lora1_name'] = unquote_plus(params.get('lora1_name', [s['lora1_name']])[0])
            s['lora1_strength'] = float(params.get('lora1_strength', [s['lora1_strength']])[0])
            s['lora2_name'] = unquote_plus(params.get('lora2_name', [s['lora2_name']])[0])
            s['lora2_strength'] = float(params.get('lora2_strength', [s['lora2_strength']])[0])
            s['hires_enabled'] = 'hires_enabled' in params
            s['hires_steps'] = int(params.get('hires_steps', [s['hires_steps']])[0])
            s['hires_denoise'] = float(params.get('hires_denoise', [s['hires_denoise']])[0])
            s['autogeneration'] = 'autogeneration' in params
            
            session['status'] = 'queued'

        self.send_response(302)
        self.send_header('Location', f'/icomfy?session_id={session_id}')
        self.end_headers()

# --- ############################################################### ---
# ---                             MAIN                              ---
# --- ############################################################### ---

class QuietHTTPHandler(VintageHttpHandler):
    """A custom handler to suppress logging for high-volume asset requests."""
    def log_message(self, format, *args):
        asset_paths = ['/thumbnail', '/cache/', '/view_slideshow_image', '/icomfy_image']
        if any(self.path.startswith(p) for p in asset_paths):
            return
        super().log_message(format, *args)

def _create_new_istream_session(session_type, source_dir, playlist):
    """Creates a new iStream session, aborting any previous one."""
    with ISTREAM_LOCK:
        global ACTIVE_ISTREAM_SESSION_ID
        if ACTIVE_ISTREAM_SESSION_ID and ACTIVE_ISTREAM_SESSION_ID in ISTREAM_SESSIONS:
            print(f"-> Aborting previous iStream session: {ACTIVE_ISTREAM_SESSION_ID}")
            ISTREAM_SESSIONS[ACTIVE_ISTREAM_SESSION_ID]['is_aborted'] = True
            ISTREAM_SESSIONS[ACTIVE_ISTREAM_SESSION_ID]['queue'].clear()
            ISTREAM_SESSIONS[ACTIVE_ISTREAM_SESSION_ID]['priority_queue'].clear()

        session_id = f"{int(time.time())}-{random.randint(1000, 9999)}"
        new_session = {
            'session_id': session_id, 'type': session_type, 'source_dir': source_dir,
            'playlist': playlist, 'queue': deque(), 'priority_queue': deque(),
            'completed': set(), 'failed': set(), 'current_task': None,
            'progress': {}, 'is_aborted': False, 'is_started': False
        }

        for path in playlist:
            if not CacheManager.get_item('istream', path):
                output_path = get_cache_path(path, 'istream', '.mov')
                new_session['queue'].append({'original_path': path, 'output_path': output_path})
        
        ISTREAM_SESSIONS[session_id] = new_session
        ACTIVE_ISTREAM_SESSION_ID = session_id
        
        # Clean up old, aborted sessions
        for s_id in list(ISTREAM_SESSIONS.keys()):
            if s_id != ACTIVE_ISTREAM_SESSION_ID and ISTREAM_SESSIONS[s_id].get('is_aborted'):
                del ISTREAM_SESSIONS[s_id]

        print(f"-> Created and activated new iStream session: {session_id}")
        return session_id

def _reset_active_istream_session():
    """Marks the active iStream session as aborted, stopping new tasks."""
    global ACTIVE_ISTREAM_SESSION_ID
    if ACTIVE_ISTREAM_SESSION_ID and ACTIVE_ISTREAM_SESSION_ID in ISTREAM_SESSIONS:
        session = ISTREAM_SESSIONS[ACTIVE_ISTREAM_SESSION_ID]
        session['is_aborted'] = True
        session['queue'].clear()
        session['priority_queue'].clear()
        print(f"-> Reset command issued for session {ACTIVE_ISTREAM_SESSION_ID}.")
    
def _prioritize_istream_task(session_id, file_to_prio):
    """Moves a file from the main queue to the priority queue."""
    with ISTREAM_LOCK:
        if session_id not in ISTREAM_SESSIONS: return
        session = ISTREAM_SESSIONS[session_id]
        
        task_to_move = None
        for task in session['queue']:
            if task['original_path'] == file_to_prio:
                task_to_move = task
                break
        
        if not task_to_move: return

        session['queue'].remove(task_to_move)
        session['priority_queue'].append(task_to_move)

        # Reorder the rest of the queue to maintain a logical sequence
        try:
            prio_index_in_playlist = session['playlist'].index(file_to_prio)
            
            new_order = [p for p in session['playlist'][prio_index_in_playlist + 1:]]
            new_order.extend([p for p in session['playlist'][:prio_index_in_playlist]])
            
            new_queue = deque()
            current_queued_paths = {t['original_path'] for t in session['queue']}
            
            for path in new_order:
                if path in current_queued_paths:
                    for task in session['queue']:
                        if task['original_path'] == path:
                            new_queue.append(task)
                            break
            session['queue'] = new_queue
            print(f"-> Prioritized {os.path.basename(file_to_prio)} and reordered queue.")
        except ValueError:
            print("!! Could not reorder queue: prioritized item not in original playlist.")

def _generate_thumbnail(original_path, thumb_type, size):
    """Generates a JPG thumbnail for a given media file."""
    thumb_path = get_cache_path(original_path, thumb_type, '.jpg')
    source_for_pillow, temp_frame_path = original_path, None
    ext = os.path.splitext(original_path)[1].lower()
    
    try:
        if ext in VIDEO_EXTENSIONS:
            # For videos, extract a frame first with ffmpeg
            temp_frame_path = os.path.join(CACHE_DIR, f"frame_{str(hash(original_path))}.jpg")
            subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-ss', '10', '-i', original_path, '-vframes', '1', '-y', temp_frame_path], check=True, timeout=10)
            source_for_pillow = temp_frame_path
        
        with Image.open(source_for_pillow) as img:
            img.thumbnail(size, Image.Resampling.LANCZOS)
            if img.mode not in ('RGB', 'L'): img = img.convert('RGB')
            img.save(thumb_path, "JPEG", quality=80)
        
        CacheManager.add_item(thumb_type, original_path, thumb_path)
        return thumb_path
    except Exception as e:
        if "$RECYCLE.BIN" not in original_path and ".Trash-" not in original_path:
            print(f"!! FAILED to generate thumbnail for {os.path.basename(original_path)}: {e}")
        return None
    finally:
        # Clean up temporary frame file if one was created
        if temp_frame_path and os.path.exists(temp_frame_path):
            os.remove(temp_frame_path)

def _get_default_icomfy_session():
    """Returns a dictionary with default settings for a new iComfy session."""
    def safe_first(lst, default=''):
        return lst[0] if lst else default

    return {
        'status': 'idle', 
        'history': [],
        'current_image_index': 0,
        'settings': {
            'model': safe_first(COMFY_DATA['models']),
            'vae': 'vae-ft-mse-840000-ema-pruned.safetensors',
            'sampler_name': 'euler_ancestral',
            'scheduler': 'normal',
            'positive_prompt': '', 
            'negative_prompt': '',
            'width': 680, 
            'height': 1024,
            'steps': 30, 
            'cfg': 8.0, 
            'denoise': 1.0,
            'lora1_name': 'None', 'lora1_strength': 1.0,
            'lora2_name': 'None', 'lora2_strength': 1.0,
            'hires_enabled': False, 
            'hires_steps': 16, 
            'hires_denoise': 0.8,
            'autogeneration': False
        }
    }

def _build_comfy_prompt(settings):
    """Dynamically builds a ComfyUI API prompt from user settings."""
    # This is a simplified workflow; it could be expanded to support more complex node graphs.
    prompt = {
        "3": {"class_type": "KSampler", "inputs": {}},
        "4": {"class_type": "CheckpointLoaderSimple", "inputs": {}},
        "5": {"class_type": "EmptyLatentImage", "inputs": {}},
        "6": {"class_type": "CLIPTextEncode", "inputs": {}},
        "7": {"class_type": "CLIPTextEncode", "inputs": {}},
        "8": {"class_type": "VAEDecode", "inputs": {}},
        "9": {"class_type": "SaveImage", "inputs": {"filename_prefix": "SASI-CaTS"}},
        "10": {"class_type": "VAELoader", "inputs": {}},
    }

    # Populate base nodes
    prompt["4"]["inputs"]["ckpt_name"] = settings['model']
    prompt["10"]["inputs"]["vae_name"] = settings['vae']
    prompt["5"]["inputs"]["width"] = settings['width']
    prompt["5"]["inputs"]["height"] = settings['height']
    prompt["5"]["inputs"]["batch_size"] = 1
    
    last_model_node_id = "4"
    last_clip_node_id = "4"

    # Conditionally add LoRA 1
    if settings['lora1_name'] != 'None':
        prompt["14"] = {
            "class_type": "LoraLoader",
            "inputs": {
                "lora_name": settings['lora1_name'],
                "strength_model": settings['lora1_strength'],
                "strength_clip": settings['lora1_strength'],
                "model": [last_model_node_id, 0],
                "clip": [last_clip_node_id, 1]
            }
        }
        last_model_node_id = "14"
        last_clip_node_id = "14"

    # Conditionally add LoRA 2, chaining from LoRA 1 if it exists
    if settings['lora2_name'] != 'None':
        prompt["15"] = {
            "class_type": "LoraLoader",
            "inputs": {
                "lora_name": settings['lora2_name'],
                "strength_model": settings['lora2_strength'],
                "strength_clip": settings['lora2_strength'],
                "model": [last_model_node_id, 0],
                "clip": [last_clip_node_id, 1]
            }
        }
        last_model_node_id = "15"
        last_clip_node_id = "15"

    # Populate CLIPTextEncode nodes, connecting to the final CLIP output
    prompt["6"]["inputs"]["text"] = settings['positive_prompt']
    prompt["6"]["inputs"]["clip"] = [last_clip_node_id, 1]
    prompt["7"]["inputs"]["text"] = settings['negative_prompt']
    prompt["7"]["inputs"]["clip"] = [last_clip_node_id, 1]

    # Populate KSampler, connecting to the final model output
    prompt["3"]["inputs"] = {
        "seed": random.randint(0, 2**32 - 1),
        "steps": settings['steps'],
        "cfg": settings['cfg'],
        "sampler_name": settings['sampler_name'],
        "scheduler": settings['scheduler'],
        "denoise": settings['denoise'],
        "model": [last_model_node_id, 0],
        "positive": ["6", 0],
        "negative": ["7", 0],
        "latent_image": ["5", 0]
    }

    # VAEDecode and SaveImage connections
    prompt["8"]["inputs"]["samples"] = ["3", 0]
    prompt["8"]["inputs"]["vae"] = ["10", 0]
    prompt["9"]["inputs"]["images"] = ["8", 0]

    # Conditionally add HiRes Fix nodes
    if settings['hires_enabled']:
        prompt["65"] = {"class_type": "VAEEncode", "inputs": {"pixels": ["67", 0], "vae": ["10", 0]}}
        prompt["66"] = {"class_type": "KSampler", "inputs": {
            "seed": random.randint(0, 2**32 - 1),
            "steps": settings['hires_steps'],
            "cfg": settings['cfg'],
            "sampler_name": settings['sampler_name'],
            "scheduler": settings['scheduler'],
            "denoise": settings['hires_denoise'],
            "model": [last_model_node_id, 0],
            "positive": ["6", 0],
            "negative": ["7", 0],
            "latent_image": ["65", 0]
        }}
        prompt["67"] = {"class_type": "ImageScaleBy", "inputs": {"upscale_method": "bicubic", "scale_by": 1.5, "image": ["8", 0]}}
        prompt["68"] = {"class_type": "VAEDecode", "inputs": {"samples": ["66", 0], "vae": ["10", 0]}}
        prompt["9"]["inputs"]["images"] = ["68", 0] # Reroute SaveImage to the HiRes output
    
    return prompt

def start_worker_threads():
    """Initializes and starts all background worker threads."""
    global ISTREAM_WORKER, THUMB_CRAWLER_WORKER, ICOMFY_WORKER
    
    if not (ISTREAM_WORKER and ISTREAM_WORKER.is_alive()):
        ISTREAM_WORKER = threading.Thread(target=istream_transcode_worker, daemon=True)
        ISTREAM_WORKER.start()
        
    if not (THUMB_CRAWLER_WORKER and THUMB_CRAWLER_WORKER.is_alive()):
        THUMB_CRAWLER_PAUSE_EVENT.set()
        THUMB_CRAWLER_WORKER = threading.Thread(target=thumbnail_crawler_worker, daemon=True)
        THUMB_CRAWLER_WORKER.start()
        
    if not (ICOMFY_WORKER and ICOMFY_WORKER.is_alive()):
        ICOMFY_WORKER = threading.Thread(target=icomfy_worker, daemon=True)
        ICOMFY_WORKER.start()
        
def _fetch_comfy_data(data_type):
    """Fetches object info from the ComfyUI API (e.g., list of models, VAEs)."""
    try:
        res = requests.get(f"http://{COMFYUI_URL}/object_info/{data_type}", timeout=5)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"!! [iComfy] API request failed for {data_type}: {e}")
        return {}


def _queue_comfy_prompt(prompt):
    """Sends a prompt to the ComfyUI queue."""
    p = {"prompt": prompt, "client_id": COMFY_CLIENT_ID}
    data = json.dumps(p).encode('utf-8')
    req = requests.post(f"http://{COMFYUI_URL}/prompt", data=data)
    req.raise_for_status()
    return req.json()

def _get_comfy_history(prompt_id):
    """Retrieves the output history for a completed ComfyUI prompt."""
    req = requests.get(f"http://{COMFYUI_URL}/history/{prompt_id}")
    req.raise_for_status()
    return req.json()

def _get_comfy_image(filename, subfolder, folder_type):
    """Retrieves the raw image data from a ComfyUI output."""
    data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
    url_values = urlencode(data)
    req = requests.get(f"http://{COMFYUI_URL}/view?{url_values}")
    req.raise_for_status()
    return req.content

def main():
    """Main entry point for the server."""
    global ROOT_MEDIA_FOLDER, CACHE_DIR, ITUBE_DIR, ICOMFY_DIR, CACHE_INDEX_FILE, AVAILABLE_DRIVES, COMFY_DATA, CPU_MODE, APPLE_SILICON_MODE, COMFYUI_URL
    parser = argparse.ArgumentParser(description="SASI-CaTS a0.1")
    parser.add_argument("media_folder", help="The root folder to serve.")
    parser.add_argument("--fresh", action="store_true", help="Clean all cached files and exit.")
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--cpu", action="store_true", help="Use CPU-only for video transcoding.")
    mode_group.add_argument("--AppleS", action="store_true", help="Use Apple Silicon (VideoToolbox) for GPU transcoding.")
    
    args = parser.parse_args()

    if args.cpu:
        CPU_MODE = True
        print("-> Transcoding mode: CPU Only")
    elif args.AppleS:
        APPLE_SILICON_MODE = True
        print("-> Transcoding mode: Apple Silicon (VideoToolbox)")
    else:
        print("-> Transcoding mode: NVIDIA (CUDA) - Default")

    if COMFYUI_IP and COMFYUI_IP != "ComfyUI_IP":
        COMFYUI_URL = COMFYUI_IP
    
    if shutil.which('ffmpeg') is None or shutil.which('ffprobe') is None:
        print("!! ERROR: 'ffmpeg' and 'ffprobe' must be installed and in your system's PATH.")
        sys.exit(1)
    if shutil.which('yt-dlp') is None:
        print("!! WARNING: 'yt-dlp' is not found. iTube will not work.")
    try:
        import PIL
    except ImportError:
        print("!! ERROR: The 'Pillow' library is required. Please install it.")
        sys.exit(1)
    try:
        import websocket
        if not hasattr(websocket, 'create_connection'):
            raise AttributeError
    except (ImportError, AttributeError):
        print("\n" + "="*60)
        print("!! FATAL ERROR: Incorrect 'websocket' library installed!")
        print("   SASI-CaTS requires 'websocket-client'.")
        print("\n   To fix this, please run the following commands:")
        print("   1. pip uninstall websocket")
        print("   2. pip uninstall websocket-client")
        print("   3. pip install websocket-client")
        print("="*60 + "\n")
        sys.exit(1)

    ROOT_MEDIA_FOLDER = os.path.abspath(args.media_folder)
    AVAILABLE_DRIVES = get_available_drives()
    CACHE_DIR = os.path.join(ROOT_MEDIA_FOLDER, "_sasi_cache")
    ITUBE_DIR = os.path.join(ROOT_MEDIA_FOLDER, "_itube_sasi")
    ICOMFY_DIR = os.path.join(ROOT_MEDIA_FOLDER, "_icomfy_generations")
    CACHE_INDEX_FILE = os.path.join(CACHE_DIR, "cache_index.json")

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(ITUBE_DIR, exist_ok=True)
    os.makedirs(ICOMFY_DIR, exist_ok=True)
    
    if args.fresh:
        CacheManager.perform_fresh_cleanup()
        sys.exit(0)

    print("-> Fetching data from ComfyUI instance...")
    
    model_info = _fetch_comfy_data('CheckpointLoaderSimple')
    COMFY_DATA['models'] = model_info.get('CheckpointLoaderSimple', {}).get('input', {}).get('required', {}).get('ckpt_name', [[]])[0]

    vae_info = _fetch_comfy_data('VAELoader')
    COMFY_DATA['vaes'] = vae_info.get('VAELoader', {}).get('input', {}).get('required', {}).get('vae_name', [[]])[0]

    lora_info = _fetch_comfy_data('LoraLoader')
    COMFY_DATA['loras'] = lora_info.get('LoraLoader', {}).get('input', {}).get('required', {}).get('lora_name', [[]])[0]

    ksampler_info = _fetch_comfy_data('KSampler')
    required_inputs = ksampler_info.get('KSampler', {}).get('input', {}).get('required', {})
    COMFY_DATA['samplers'] = required_inputs.get('sampler_name', [[]])[0]
    COMFY_DATA['schedulers'] = required_inputs.get('scheduler', [[]])[0]

    if not all(COMFY_DATA.values()):
        print("!! WARNING: Could not fetch all required data (models, VAEs, etc.) from ComfyUI. iComfy may not work correctly.")

    CacheManager.load_cache()
    atexit.register(CacheManager.save_cache)
    
    lan_ip = get_lan_ip()
    server_address = (lan_ip, PORT)
    if lan_ip == '127.0.0.1':
        print("!! Could not determine LAN IP. Server may only be accessible from this machine.")
        server_address = ('127.0.0.1', PORT)

    class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        daemon_threads = True
        allow_reuse_address = True

    with ThreadingTCPServer(server_address, QuietHTTPHandler) as httpd:
        print("\n--- SASI-CaTS Server a0.1 is RUNNING ---")
        print(f"Serving files from: {ROOT_MEDIA_FOLDER}")
        print(f"Cache location:   {CACHE_DIR}")
        print(f"iComfy location:  {ICOMFY_DIR}")
        print(f"Available drives: {AVAILABLE_DRIVES}")
        print(f"ComfyUI URL:      {COMFYUI_URL}")
        print("\nOn your vintage Mac, open Classilla and go to:")
        print(f"  http://{lan_ip}:{PORT}/\n")
        print("-------------------------------------------------")
        print(f"Server started on http://{lan_ip}:{PORT}. Press Ctrl+C to stop.")

        start_worker_threads()

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n-> Shutting down server...")
            STOP_EVENT.set()
            httpd.shutdown()

if __name__ == "__main__":
    main()

