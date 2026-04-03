import os
import sys
import logging
import inspect
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union
import stackprinter
import re
from loguru import logger
from stream_fusion.settings import settings

_LOG_DIR = Path("/app/config/logs")

REDACTED = settings.log_redacted
patterns = [
    r"/ey[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]*/",  # JWT tokens in URL path segments
    r"(ADToken|RDToken|TBToken|PMToken|apikey|api_key|passkey|api-key)=([^&\s\"']+)",  # API key params
]

class SecretFilter:
    def __init__(self, patterns):
        self._patterns = patterns

    def __call__(self, record):
        record["message"] = self.redact(record["message"])
        if "stack" in record["extra"]:
            record["extra"]["stack"] = self.redact(record["extra"]["stack"])
        return record

    def redact(self, message):
        for pattern in self._patterns:
            if "=" in pattern:
                message = re.sub(pattern, r"\1=<REDACTED>", message)
            else:
                message = re.sub(pattern, "/<REDACTED>/", message)
        return message

def format_console(record):
    format_ = "<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - {message}\n"
    if record["exception"] is not None:
        stack = stackprinter.format(
            record["exception"],
            suppressed_vars=[r".*ygg_playload.*"],
        )
        if REDACTED:
            for pat in patterns:
                if "=" in pat:
                    stack = re.sub(pat, r"\1=<REDACTED>", stack)
                else:
                    stack = re.sub(pat, "/<REDACTED>/", stack)
        record["extra"]["stack"] = stack
        format_ += "\n{extra[stack]}"
    return format_

def format_file(record):
    format_ = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - {message}\n"
    if record["exception"] is not None:
        stack = stackprinter.format(
            record["exception"],
            suppressed_vars=[r".*ygg_playload.*"],
        )
        if REDACTED:
            for pat in patterns:
                if "=" in pat:
                    stack = re.sub(pat, r"\1=<REDACTED>", stack)
                else:
                    stack = re.sub(pat, "/<REDACTED>/", stack)
        record["extra"]["stack"] = stack
        format_ += "{extra[stack]}\n"
    return format_

class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: Union[str, int] = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )

def _cleanup_stale_log_files() -> None:
    """Supprime les fichiers de log des workers morts non nettoyés par la rotation.

    Problème : la rotation loguru ne nettoie que les fichiers rotatés par le sink
    courant. Si un worker meurt avant sa rotation journalière, son fichier .log
    reste indéfiniment. Cette fonction le purge au démarrage de chaque worker.
    """
    if not _LOG_DIR.exists():
        return
    cutoff = datetime.now() - timedelta(days=7)
    for f in _LOG_DIR.glob("stream-fusion.w*.log"):
        try:
            if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                f.unlink()
        except OSError:
            pass


def configure_logging() -> None:
    _cleanup_stale_log_files()
    logger.remove()

    log_level = settings.log_level.value

    logger.add(
        sys.stdout,
        format=format_console,
        level=log_level,
        colorize=True,
        backtrace=True,
        diagnose=True,
        filter=SecretFilter(patterns) if REDACTED else None,
        enqueue=True,
    )

    # File logging — un fichier par worker (PID), rotation journalière.
    # La rétention s'applique aux fichiers rotatés ; _cleanup_stale_log_files()
    # gère les fichiers des workers morts entre deux rotations.
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(_LOG_DIR / f"stream-fusion.w{os.getpid()}.log"),
        format=format_file,
        level="DEBUG",
        rotation="1 day",
        retention="7 days",
        compression="zip",
        enqueue=True,
        filter=SecretFilter(patterns) if REDACTED else None,
    )

    # Intercept standard library logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Disable uvicorn access logs
    for logger_name in logging.root.manager.loggerDict:
        if logger_name.startswith("uvicorn."):
            logging.getLogger(logger_name).handlers = []

    # Explicitly handle uvicorn loggers
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(logger_name).handlers = [InterceptHandler()]