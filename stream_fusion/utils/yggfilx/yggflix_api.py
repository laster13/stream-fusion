from typing import List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from stream_fusion.settings import settings
from stream_fusion.logging_config import logger


class YggflixAPI:
    def __init__(self, pool_connections=10, pool_maxsize=50, max_retries=0, timeout=5):
        self.base_url = f"{settings.yggflix_url}/api"
        self.timeout = timeout
        self.session = requests.Session()

        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _make_request(self, method, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error occurred: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error occurred: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred during the request: {e}")
            raise

    def search(self, query=""):
        return self._make_request("GET", "/search", params={"q": query})

    def get_home(self):
        return self._make_request("GET", "/home")

    def get_movie_detail(self, movie_id: int):
        return self._make_request("GET", f"/movie/{movie_id}")

    def get_movie_torrents(self, movie_id: int):
        return self._make_request("GET", f"/movie/{movie_id}/torrents")

    def get_tvshow_detail(self, tvshow_id: int):
        return self._make_request("GET", f"/tvshow/{tvshow_id}")

    def get_tvshow_torrents(self, tvshow_id: int):
        return self._make_request("GET", f"/tvshow/{tvshow_id}/torrents")

    def get_torrents(self, page: int = 1, q: str = "", category_id: Optional[int] = None, order_by: str = "uploaded_at") -> List[dict]:
        params = {"page": page, "q": q, "order_by": order_by}
        if category_id is not None:
            params["category_id"] = category_id
        return self._make_request("GET", "/torrents", params=params)

    def get_torrent_detail(self, torrent_id: int) -> dict:
        return self._make_request("GET", f"/torrent/{torrent_id}")

    def download_torrent(self, torrent_id: int, passkey: str) -> bytes:
        if len(passkey) != 32:
            raise ValueError("Passkey must be exactly 32 characters long.")
        url = f"{self.base_url}/torrent/{torrent_id}/download"
        try:
            response = self.session.get(url, params={"passkey": passkey}, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred while downloading torrent: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while downloading torrent: {e}")
            raise

    def __del__(self):
        self.session.close()
