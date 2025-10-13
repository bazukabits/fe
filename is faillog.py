from __future__ import annotations
import os
import shutil
import logging
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

# ----------------------------------------------------------------------
# Logging configuration (colourised, pretty!)
# ----------------------------------------------------------------------
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
log = logging.getLogger("log_sender")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def is_failed_log(file_path: Path) -> bool:
    """
    Return True if *file_path* contains the words 'FAIL' or 'ERROR'
    (case‑insensitive).  The file is read lazily line by line.
    """
    try:
        with file_path.open("r", encoding="utf‑8") as f:
            for line in f:
                if "FAIL" in line.upper() or "ERROR" in line.upper():
                    return True
    except (OSError, UnicodeDecodeError) as exc:
        log.warning(f"Could not read {file_path!s}: {exc!s}")
    return False


def get_latest_file(
    res_dir_path: Path, serial_num: str, file_type: str, is_cft: bool = False
) -> Optional[Path]:
    """
    Return the most recent file in *res_dir_path* that matches the naming
    convention used by the production system.

    Parameters
    ----------
    res_dir_path : Path
        Directory containing the log files.
    serial_num   : str
        Expected serial number (e.g. 'SN1234').
    file_type    : str
        File suffix, e.g. '.log'.
    is_cft       : bool
        If True, the expected serial format is '{serial_num}-A'.

    Returns
    -------
    Optional[Path]
        Path to the newest matching file or ``None`` if no file is found.
    """
    if not res_dir_path.is_dir():
        log.error(f"Directory does not exist: {res_dir_path}")
        return None

    latest_file: Optional[Path] = None
    latest_ts: str = ""

    for path in res_dir_path.iterdir():
        if not path.is_file() or not path.name.endswith(file_type):
            continue

        # Expected naming: <something>_<something>_<serial>_<YYYYMMDD>_<HHMMSS>...
        parts = path.name.split("_")
        if len(parts) < 5:          # guard against malformed names
            continue

        file_serial = parts[2]
        expected_serial = f"{serial_num}-A" if is_cft else serial_num
        if file_serial != expected_serial:
            continue

        file_ts = parts[3] + parts[4]
        if file_ts > latest_ts:
            latest_ts = file_ts
            latest_file = path

    return latest_file


def send_fail_log(
    log_dir_path: Path,
    serial_num: str,
    file_type: str,
    dest_path: Path,
    is_cft: bool = False,
) -> bool:
    """
    Copy the most recent *failed* log file to *dest_path*.

    Parameters
    ----------
    log_dir_path : Path
        Directory where source logs are stored.
    serial_num   : str
        Serial number to match in the filename.
    file_type    : str
        File suffix (e.g. '.log').
    dest_path    : Path
        Directory that will receive the copied file.
    is_cft       : bool
        True if the serial format includes a trailing '-A'.

    Returns
    -------
    bool
        ``True`` if a file was copied; ``False`` otherwise.
    """
    src_file = get_latest_file(log_dir_path, serial_num, file_type, is_cft)
    if src_file is None:
        log.info("No matching file found.")
        return False

    if not is_failed_log(src_file):
        log.info(f"Latest file '{src_file.name}' is not a failure log.")
        return False

    dest_path.mkdir(parents=True, exist_ok=True)
    dest_file = dest_path / src_file.name

    try:
        shutil.copy2(src_file, dest_file)
    except OSError as exc:
        log.error(f"Failed to copy {src_file} -> {dest_file}: {exc}")
        return False

    log.info(f"Copied {src_file.name} to {dest_path}")
    return True


# ----------------------------------------------------------------------
# Example usage
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # These paths are placeholders; replace with your real directories.
    LOG_DIR = Path("/path/to/source/logs")
    DEST_DIR = Path("/path/to/destination")
    SERIAL  = "SN1234"
    TYPE    = ".log"

    success = send_fail_log(LOG_DIR, SERIAL, TYPE, DEST_DIR)
    if success:
        log.success("Operation completed successfully.")
    else:
        log.warning("No failure log was transferred.")