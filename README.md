========================================
SASI-CaTS (Software Applet & Search Interface - Caching auto-Transcoding Server)

A Python-based web server designed to serve a modern multimedia library to
vintage computers. It works by transcoding media on-the-fly into legacy-
compatible formats and providing a simple, lightweight web interface that
works on period-appropriate browsers.

========================================

--- QUICK START GUIDE ---
1. Setup a Virtual Environment (Recommended)
On systems with managed Python installations (like Debian/APT or
macOS/Homebrew), using a virtual environment (venv) is required.

* Create the environment:

python3 -m venv sasi_env

* Activate it (do this every time you start a new terminal session):

source sasi_env/bin/activate

* Install required libraries:

pip install requests Pillow websocket-client

2. Start the Server
Run the script from your terminal, pointing it to the directory
containing your media files.

* Commands:
- Windows: python SASI-CaTS_a0.1.py C:/path/to/your/media
- Linux/Mac: python SASI-CaTS_a0.1.py /path/to/your/media

3. Connect Your Vintage Machine

* Find your server's local IP address (displayed on startup).

* Open a recommended browser and navigate to http://<server_ip>:8000.

--- STARTUP FLAGS ---
* --fresh : Deletes the entire _sasi_cache directory on startup.

* --cpu : Forces iStream to use a CPU-only transcode workflow.

* --AppleS : Uses a hardware-accelerated workflow for Apple Silicon Macs.

--- CLIENT RECOMMENDATIONS ---
* Browser:
- Mac OS 9: Classilla
- Mac OS X (PPC): TenFourFox
- Windows XP: MyPal
* Media Player:
- Mac OS 9: Built-in QuickTime player.
- Mac OS X / Win: An older, compatible version of VLC.

--- APPLET GUIDE ---
* Homepage
The central navigation hub with a FrogFind! search bar and applet links.

* Files
A paginated file browser for your media library.
- Controls: Use the "Mount" dropdown to switch between drive roots.
- Integration: Launches iStream for videos and iMagery for directories.

* iMagery
A grid-based image gallery.
- Controls: "Slideshow" and "Random Show" softkeys start a fullscreen
slideshow. "Recursive" versions include all subdirectories.

* iStream
The core video transcoding and streaming applet.
- Behavior: Transcodes videos to a highly compatible format.
- Target Format: MPEG-4 video and ADPCM audio in a .mov container,
at ~346x260 resolution and 19 frames per second.
- Controls: Manage the queue, prioritize files, and play completed videos.

* iTube
A simple front-end for yt-dlp to download YouTube videos.
- Behavior: Downloads videos up to 360p to the _itube_sasi folder.
- Controls: "View Queue" shows download progress.

* iGem
A terminal-style chat client for the Google Gemini API.
- Configuration: To enable, insert your API key into the
GEMINI_API_KEY variable in the script.

* iComfy
An API client for a local or LAN ComfyUI instance.
- Behavior: Generate AI images by writing prompts and selecting models.
- Configuration: Edit the COMFYUI_IP variable to connect to a remote
ComfyUI instance (e.g., "192.168.1.50:8188").
