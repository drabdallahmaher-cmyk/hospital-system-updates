import os
import sys
import json
import time
import shutil
import urllib.request
import urllib.error
import zipfile
from PySide6.QtWidgets import QMessageBox

def get_current_version():
    try:
        from version import APP_VERSION
        return APP_VERSION
    except ImportError:
        return "1.0.0"

def version_tuple(v):
    try:
        return tuple(map(int, v.split(".")))
    except ValueError:
        return (0, 0, 0)

def check_for_updates():
    try:
        url = "https://github.com/drabdallahmaher-cmyk/hospital-system-updates/releases/latest/download/version.json"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            remote_version = data.get("version")
            download_url = data.get("download_url")
            
            if not remote_version or not download_url:
                return None
                
            if version_tuple(remote_version) > version_tuple(get_current_version()):
                return {"version": remote_version, "download_url": download_url}
    except Exception:
        pass
    return None

def backup_database():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, "data")
        backup_dir = os.path.join(base_dir, "backup")
        os.makedirs(backup_dir, exist_ok=True)
        
        db_path = os.path.join(data_dir, "local_cache.db")
        if os.path.exists(db_path):
            backup_path = os.path.join(backup_dir, "local_cache_pre_update.db")
            shutil.copy2(db_path, backup_path)
    except Exception as e:
        print(f"Backup failed: {e}")

def apply_update(download_url, write_queue_manager):
    try:
        if not download_url:
            return

        base_dir = os.path.dirname(os.path.abspath(__file__))
        temp_zip = os.path.join(base_dir, "update_temp.zip")
        temp_extract = os.path.join(base_dir, "update_extracted")

        # 1. Wait until queue is empty and stop worker safely
        if write_queue_manager:
            while not write_queue_manager.queue.empty():
                time.sleep(0.2)
            write_queue_manager.stop_worker()

        # 2. Backup DB
        backup_database()

        # 3. Download update.zip safely
        try:
            req = urllib.request.Request(download_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as response, open(temp_zip, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
        except urllib.error.URLError as e:
            print(f"Update download failed: {e}")
            return
            
        if not os.path.exists(temp_zip):
            return

        # 4. Extract to temp folder
        if os.path.exists(temp_extract):
            shutil.rmtree(temp_extract)
        os.makedirs(temp_extract)

        try:
            with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                zip_ref.extractall(temp_extract)
        except zipfile.BadZipFile as e:
            print(f"Update extraction failed: {e}")
            return

        # 5. Apply update safely (protect data, logs, backup, config.json)
        protected_dirs = ["data", "logs", "backup"]
        protected_files = ["config.json", "version.py"]
        
        for root, dirs, files in os.walk(temp_extract):
            rel_path = os.path.relpath(root, temp_extract)
            if rel_path == ".":
                rel_path = ""
                
            if any(rel_path.startswith(p) for p in protected_dirs):
                continue
                
            target_dir = os.path.join(base_dir, rel_path)
            os.makedirs(target_dir, exist_ok=True)
            
            for file in files:
                # Protect specific files from being overwritten (except version.py which SHOULD update)
                if rel_path == "" and file == "config.json":
                    continue
                    
                src_file = os.path.join(root, file)
                dst_file = os.path.join(target_dir, file)
                
                if file.endswith((".py", ".exe")) or "resources" in rel_path:
                    try:
                        shutil.copy2(src_file, dst_file)
                    except Exception:
                        pass # Ignore locked files momentarily

        # Cleanup temp
        try:
            if os.path.exists(temp_zip):
                os.remove(temp_zip)
            if os.path.exists(temp_extract):
                shutil.rmtree(temp_extract)
        except Exception:
            pass

        # 6. Restart app
        os.execl(sys.executable, sys.executable, *sys.argv)

    except Exception as e:
        print(f"Update failed critically: {e}")
        # Fail-safe: do nothing

def run_updater_on_startup(write_queue_manager=None, parent_widget=None):
    update_info = check_for_updates()
    if update_info:
        reply = QMessageBox.question(
            parent_widget,
            "تحديث جديد",
            f"يوجد تحديث جديد (الإصدار {update_info['version']})، هل تريد التحديث الآن؟",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )
        if reply == QMessageBox.Yes:
            apply_update(update_info["download_url"], write_queue_manager)
