import time
import requests
import urllib.parse
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging

# Initialiser le logger
logger = logging.getLogger(__name__)


class RateLimiter:
    """Permet de limiter le nombre d'appels par seconde."""
    def __init__(self, calls_per_second=1):
        self.calls_per_second = calls_per_second
        self.last_call = 0

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            now = time.time()
            time_since_last_call = now - self.last_call
            if time_since_last_call < 1 / self.calls_per_second:
                time.sleep(1 / self.calls_per_second - time_since_last_call)
            self.last_call = time.time()
            return func(*args, **kwargs)
        return wrapper


class XthorAPI:
    def __init__(self, xthor_passkey: str, max_retries=3, timeout=10):
        """
        Initialise l'API Xthor.

        Args:
            xthor_passkey (str): La clé utilisateur pour l'API Xthor.
            max_retries (int): Nombre maximum de tentatives en cas d'erreur réseau.
            timeout (int): Temps d'attente maximal pour une requête.
        """
        self.base_url = "https://api.xthor.tk/"
        if not xthor_passkey or len(xthor_passkey) != 32:
            raise ValueError("Xthor passkey must be 32 characters long")
        self.xthor_passkey = xthor_passkey
        self.timeout = timeout
        self.session = requests.Session()

        # Configuration de la gestion des erreurs réseau
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    @RateLimiter()
    def search(self, query: str, category: int = None, limit: int = None):
        """
        Effectue une recherche sur l'API Xthor.

        Args:
            query (str): Le mot-clé de recherche.
            category (int, optional): La catégorie à filtrer (exemple : 1 pour les films).
            limit (int, optional): Limiter le nombre de résultats retournés.

        Returns:
            list: Liste des torrents correspondants.
        """
        params = {
            "passkey": self.xthor_passkey,
            "search": query,
        }
        if category:
            params["category"] = category
        if limit:
            params["limit"] = limit

        url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
        try:
            logger.info(f"Envoi d'une requête vers l'URL : {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            # Vérifie si la clé 'torrents' est présente
            torrents = data.get("torrents", [])
            logger.info(f"Nombre de torrents trouvés : {len(torrents)}")
            return torrents

        except requests.HTTPError as http_err:
            logger.error(f"Erreur HTTP : {http_err}")
            raise RuntimeError(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur de connexion ou autre problème : {e}")
            raise RuntimeError(f"An error occurred during the request: {e}")

    def __del__(self):
        """Ferme la session lors de la destruction de l'objet."""
        self.session.close()
