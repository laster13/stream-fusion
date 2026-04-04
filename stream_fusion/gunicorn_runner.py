from typing import Any
import os

from gunicorn.app.base import BaseApplication
from gunicorn.util import import_app
from uvicorn.workers import UvicornWorker as BaseUvicornWorker

from stream_fusion.logging_config import configure_logging

try:
    import uvloop
except ImportError:
    uvloop = None  # type: ignore


class UvicornWorker(BaseUvicornWorker):
    """
    Configuration for uvicorn workers.

    This class subclasses UvicornWorker and defines some parameters
    class-wide because Gunicorn does not expose them cleanly.
    """

    CONFIG_KWARGS: dict[str, Any] = {
        "loop": "uvloop" if uvloop is not None else "asyncio",
        "http": "httptools",
        "lifespan": "on",
        "factory": True,
        "proxy_headers": True,
        "forwarded_allow_ips": os.getenv("FORWARDED_ALLOW_IPS", "*"),
    }


class GunicornApplication(BaseApplication):
    def __init__(self, app, host, port, workers, **kwargs):
        self.options = {
            "bind": f"{host}:{port}",
            "workers": workers,
            "worker_class": "stream_fusion.gunicorn_runner.UvicornWorker",
            **kwargs,
        }
        self.app = app
        super().__init__()

    def load_config(self):
        for key, value in self.options.items():
            if key in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

    def load(self):
        return import_app(self.app)

    def init(self, parser, opts, args):
        configure_logging()
        return super().init(parser, opts, args)
