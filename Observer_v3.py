#!/usr/bin/env python3
# how to run
# ใช้พาธสัมพัทธ์
# python watch_and_extract_fail_logs.py /tmp/dir1 /tmp/dir2 /tmp/dir3 /tmp/dir4 ./fail
# ใช้พาธสมบูรณ์
# python watch_and_extract_fail_logs.py /tmp/dir1 /tmp/dir2 /tmp/dir3 /tmp/dir4 /var/log/fail

"""
watch_and_extract_fail_logs.py
"""
from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, List, Union

import zipfile
from concurrent.futures import ThreadPoolExecutor
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# --------------------------------------------------------------------------- #
# 1. Core extraction logic – unchanged
# --------------------------------------------------------------------------- #
def _contains_fail(buf: bytes) -> bool:
    """Return True if the binary buffer contains 'fail' (case‑insensitive)."""
    return b"fail" in buf.lower()

def extract_failed_logs(archive_path: Path, out_dir: Path) -> List[Path]:
    """Yield absolute paths of logs extracted from *archive_path* that contain
    the keyword “fail”.  Destination directory *out_dir* is created if needed."""
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "r") as z:
        for member in z.infolist():
            if not member.filename.lower().endswith(".log") or member.is_dir():
                continue
            with z.open(member) as f:
                buf = f.read()
            if _contains_fail(buf):
                out_path = out_dir / Path(member.filename).name
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(buf)
                yield out_path

# --------------------------------------------------------------------------- #
# 2. Helper – wait until file is stable (size does not change)
# --------------------------------------------------------------------------- #
def _wait_until_stable(p: Path, timeout: float = 2.0, poll_interval: float = 0.1) -> bool:
    """Return True if file size is unchanged for *timeout* seconds."""
    if not p.exists():
        return False
    size = p.stat().st_size
    start = time.time()
    while time.time() - start < timeout:
        time.sleep(poll_interval)
        if not p.exists():
            return False
        if p.stat().st_size != size:
            size = p.stat().st_size
            start = time.time()  # reset timer
    return True

# --------------------------------------------------------------------------- #
# 3. Event handler – covers created, modified and moved
# --------------------------------------------------------------------------- #
class ZipEventHandler(FileSystemEventHandler):
    """Handles *.zip* creation, modification and move events."""
    DEBOUNCE_SECONDS = 1.0   # ignore duplicate events within this window

    def __init__(self, executor: ThreadPoolExecutor, root_out: Path):
        super().__init__()
        self._executor = executor
        self._root_out = root_out
        self._last_processed: Dict[Path, float] = {}

    def _enqueue(self, archive: Path):
        """Schedule extraction if the file is stable and not processed recently."""
        now = time.time()
        last = self._last_processed.get(archive, 0)
        if now - last < self.DEBOUNCE_SECONDS:
            return
        if not _wait_until_stable(archive):
            # file disappeared or is still growing
            return
        self._last_processed[archive] = now
        self._executor.submit(self._process_zip, archive)

    # ----------------------------------------------------------------------- #
    # Event callbacks
    # ----------------------------------------------------------------------- #
    def on_created(self, event):
        if event.is_directory:
            return
        self._handle_event(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        self._handle_event(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self._handle_event(event.dest_path)

    # ----------------------------------------------------------------------- #
    def _handle_event(self, path_str: str):
        path = Path(path_str)
        if path.suffix.lower() != ".zip":
            return
        self._enqueue(path)

    # ----------------------------------------------------------------------- #
    def _process_zip(self, archive: Path):
        subdir = "fail"
        target_dir = self._root_out / subdir
        try:
            for _ in extract_failed_logs(archive, target_dir):
                pass  # optional: log here
        except Exception as exc:  # pragma: no cover
            # Keep the daemon alive – report the problem and skip
            print(f"[ERROR] Failed to process {archive!s}: {exc}", file=sys.stderr)

# --------------------------------------------------------------------------- #
# 4. Monitor – orchestrates observers and thread‑pool
# --------------------------------------------------------------------------- #
class ZipMonitor:
    def __init__(
        self,
        watch_folders: List[Path],
        root_out: Union[Path, str],
    ):
        self.watch_folders = [p.expanduser().resolve() for p in watch_folders]
        self.root_out = Path(root_out).expanduser().resolve()
        # Use CPU count to decide how many workers
        workers = max(1, os.cpu_count() or 1) * 2
        self.executor = ThreadPoolExecutor(max_workers=workers)
        self.observers: List[Observer] = []

    def start(self):
        handler = ZipEventHandler(self.executor, self.root_out)
        for folder in self.watch_folders:
            observer = Observer()
            observer.schedule(handler, str(folder), recursive=True)  # <‑‑ recursive
            observer.start()
            self.observers.append(observer)
            print(f"Watching {folder} (recursive)")

    def stop(self):
        for observer in self.observers:
            observer.stop()
        for observer in self.observers:
            observer.join()
        self.executor.shutdown(wait=True)
        print("All observers stopped.")

# --------------------------------------------------------------------------- #
# 5. CLI entry point
# --------------------------------------------------------------------------- #
def _signal_handler(signum, frame, monitor: ZipMonitor):
    print("\nReceived interrupt – shutting down.")
    monitor.stop()
    sys.exit(0)

def main():
    if len(sys.argv) < 6:
        print(
            "Usage: python watch_and_extract_fail_logs.py "
            "<watch_dir1> <watch_dir2> <watch_dir3> <watch_dir4> <output_root>"
        )
        sys.exit(1)
    watch_dirs = [Path(p) for p in sys.argv[1:5]]
    output_root = sys.argv[5]
    monitor = ZipMonitor(watch_dirs, output_root)
    monitor.start()

    signal.signal(signal.SIGINT, lambda s, f: _signal_handler(s, f, monitor))
    signal.signal(signal.SIGTERM, lambda s, f: _signal_handler(s, f, monitor))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:  # pragma: no cover
        monitor.stop()

if __name__ == "__main__":
    main()