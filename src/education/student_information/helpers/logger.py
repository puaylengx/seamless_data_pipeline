import logging
from pathlib import Path

def get_logger(name: str,repo_root: Path,module_folder: Path,log_file_name: str) -> logging.Logger:
    # บน Cloud Run/Jobs ระบบจะเก็บ stdout/stderr เข้า Cloud Logging ให้อยู่แล้ว
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    # Console handle
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(ch)
    
    # optional: file handler (for local dev only)
    logs_dir = repo_root / "logs" / module_folder
    logs_dir.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(logs_dir / log_file_name, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(fh)
    
    return logger