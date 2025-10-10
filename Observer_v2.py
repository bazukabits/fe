"""
watch_and_extract_fail_logs.py

Monitors four directories simultaneously.  Whenever a new *.zip file is
created in any of them, the archive is scanned for *.log files that
contain the word “fail” (case‑insensitive).  Matching logs are extracted
into a common destination called `fail`.  The monitor runs until the
process is stopped (Ctrl‑C).

Author: Koala
Date:   2025‑10‑09
"""
from pathlib import Path
from typing import List, Union
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import zipfile
import time
import sys
import signal

# --------------------------------------------------------------------------- #
# Core extraction logic – unchanged
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
# Watch‑er implementation
# --------------------------------------------------------------------------- #
class ZipEventHandler(FileSystemEventHandler):
    def __init__(self, executor: ThreadPoolExecutor, root_out: Path):
        super().__init__()
        self._executor = executor
        self._root_out = root_out

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != ".zip":
            return
        self._executor.submit(self._process_zip, path)

    def _process_zip(self, archive: Path):
        subdir = archive.parent.name
        target_dir = self._root_out / subdir
        for _ in extract_failed_logs(archive, target_dir):
            pass  # you can log here if you wish


class ZipMonitor:
    def __init__(
        self,
        watch_folders: List[Path],
        root_out: Union[Path, str],
    ):
        # Normalise everything to Path objects
        self.watch_folders = [p.expanduser().resolve() for p in watch_folders]
        self.root_out = Path(root_out).expanduser().resolve()
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.observers: List[Observer] = []

    def start(self):
        handler = ZipEventHandler(self.executor, self.root_out)
        for folder in self.watch_folders:
            observer = Observer()
            observer.schedule(handler, str(folder), recursive=False)
            observer.start()
            self.observers.append(observer)
            print(f"Watching {folder}")

    def stop(self):
        for observer in self.observers:
            observer.stop()
        for observer in self.observers:
            observer.join()
        self.executor.shutdown(wait=True)
        print("All observers stopped.")


# --------------------------------------------------------------------------- #
# CLI entry point
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

    # Graceful shutdown on SIGINT/SIGTERM
    signal.signal(signal.SIGINT, lambda s, f: _signal_handler(s, f, monitor))
    signal.signal(signal.SIGTERM, lambda s, f: _signal_handler(s, f, monitor))

    # Keep the main thread alive while observers run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:  # pragma: no cover
        monitor.stop()


if __name__ == "__main__":
    main()