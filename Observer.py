from __future__ import annotations
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import shutil

import io
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Iterable

__all__ = ["extract_failed_logs"]

fail_dir_path ="./Fail Logs"
watchpath = sys.argv[1]
keyword = "test failed"

class NewFolderAndFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            print(f"New folder created: {event.src_path}")
            # You can start a new observer for this folder or process its contents
            # For simplicity, we'll just list existing files immediately
            for root, _, files in os.walk(event.src_path):
                for file in files:
                    print(f"Existing file in new folder: {os.path.join(root, file)}")
        else:
            print(f"New file created: {event.src_path}")
            # Add logic here
            #archive = os.path.join(event.src_path,".log")
            # find_fail_in_log(event.src_path,fail_dir_path)
            moved = list(extract_failed_logs(event.src_path, fail_dir_path))
            print(f"Extracted {len(moved)} failed log(s) to {fail_dir_path!s}")

def monitor_with_watchdog(target_directory):
    event_handler = NewFolderAndFileHandler()
    observer = Observer()
    observer.schedule(event_handler, target_directory, recursive=True)
    observer.start()
    print("Observer Started")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def _contains_fail(buf: bytes) -> bool:
    """
    Return True if the binary buffer contains the substring “fail”
    in any case.  The function operates on bytes to avoid encoding
    complications and keeps the memory footprint small.
    """
    return keyword.encode("utf-8") in buf.lower()

def extract_failed_logs(archive_path: str | Path,
                        out_dir: str | Path = "fail") -> Iterable[Path]:
    """
    Scan *archive_path* (a ZIP file) for *.log files that contain
    “fail”.  Matching files are extracted into *out_dir*.

    Parameters
    ----------
    archive_path : str | Path
        Path to the ZIP archive.
    out_dir : str | Path, default "fail"
        Destination directory for the extracted logs.

    Yields
    ------
    pathlib.Path
        Full path of each extracted file.

    Raises
    ------
    FileNotFoundError
        If *archive_path* does not exist.
    zipfile.BadZipFile
        If the file is not a valid ZIP archive.
    """
    archive_path = Path(archive_path).expanduser().resolve()
    out_dir = Path(out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(archive_path, "r") as z:
        for member in z.infolist():
            if not member.filename.lower().endswith(".log") or member.is_dir():
                continue

            with z.open(member) as f:
                buf = f.read()

            if _contains_fail(buf):
                try:
                    out_path = out_dir / Path(member.filename).name
                except:
                    os.makedirs(out_dir)
                    out_path = out_dir / Path(member.filename).name
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(buf)
                yield out_path

#monitor_with_watchdog(".")
monitor_with_watchdog(watchpath)