import asyncio
import aiohttp
import json as json_lib
import re
from urllib.parse import quote, unquote

from stream_fusion.logging_config import logger
from stream_fusion.utils.debrid.base_debrid import BaseDebrid
from stream_fusion.utils.debrid.debrid_exceptions import DebridError
from stream_fusion.settings import settings
from stream_fusion.utils.general import (
    season_episode_in_filename,
    smart_episode_fallback,
    is_video_file,
)


class StremThru(BaseDebrid):
    def __init__(self, config, session: aiohttp.ClientSession = None):
        super().__init__(config, session)
        self.config = config
        self.stremthru_url = settings.stremthru_url or "https://stremthru.13377001.xyz"
        self.base_url = f"{self.stremthru_url}/v0/store"
        self.store_name = None
        self.token = None
        self._headers = {}

        if not self.store_name:
            self.auto_detect_store()

    def auto_detect_store(self):
        """Détecte automatiquement le debrideur à utiliser selon les tokens disponibles."""
        priority_order = [
            ("realdebrid", "RDToken"),
            ("premiumize", "PMToken"),
            ("torbox", "TBToken"),
            ("alldebrid", "ADToken"),
            ("debridlink", "DLToken"),
            ("easydebrid", "EDToken"),
            ("offcloud", "OCCredentials"),
            ("pikpak", "PPCredentials"),
        ]

        for store_name, token_key in priority_order:
            token = self.config.get(token_key)
            if token:
                token_str = token if isinstance(token, str) else str(token)
                if len(token_str.strip()) > 5:
                    logger.info(
                        f"StremThru: Utilisation automatique de {store_name} détecté avec le token {token_key}"
                    )
                    self.set_store_credentials(store_name, token_str)
                    break

        if not self.store_name:
            logger.warning("StremThru: Aucun debrideur détecté automatiquement")

    @property
    def service_name(self) -> str:
        # Include store name so caches don't collide across services using StremThru
        return f"stremthru_{self.store_name or 'generic'}"

    def _index_results_by_hash(self, response) -> dict:
        # StremThru returns a list of {"hash": ..., "status": "cached", ...}
        if not isinstance(response, list):
            return {}
        return {item["hash"]: item for item in response if item.get("hash")}

    def _reconstruct_response(self, items: list):
        # StremThru-based providers consume a plain list
        return items

    def set_store_credentials(self, store_name, token):
        """Configure les informations d'identification du store pour StremThru."""
        self.store_name = store_name
        self.token = token
        self._headers = {
            "X-StremThru-Store-Name": store_name,
            "X-StremThru-Store-Authorization": f"Bearer {token}",
            "User-Agent": "stream-fusion",
        }

    @staticmethod
    def get_underlying_debrid_code(store_name=None):
        """Retourne le code du service de debrid sous-jacent."""
        debrid_codes = {
            "realdebrid": "RD",
            "alldebrid": "AD",
            "torbox": "TB",
            "premiumize": "PM",
            "offcloud": "OC",
            "debridlink": "DL",
            "easydebrid": "ED",
            "pikpak": "PK",
        }
        return debrid_codes.get(store_name)

    def parse_store_creds(self, token):
        """Parse les informations d'identification du store."""
        if ":" in token:
            parts = token.split(":", 1)
            return parts[0], parts[1]
        return token, ""

    def _normalize_hash(self, value):
        if not value:
            return None

        value = str(value).strip()

        if value.startswith("magnet:?"):
            match = re.search(r"btih:([a-fA-F0-9]{40})", value, re.IGNORECASE)
            if match:
                return match.group(1).lower()
            return None

        value = value.lower()
        if len(value) == 40 and all(c in "0123456789abcdef" for c in value):
            return value

        return None

    def _to_magnet(self, value):
        if not value:
            return None

        if isinstance(value, str) and value.startswith("magnet:?"):
            return value

        normalized_hash = self._normalize_hash(value)
        if normalized_hash:
            return f"magnet:?xt=urn:btih:{normalized_hash}"

        return None

    def _is_text_or_subtitle_file(self, file_name: str) -> bool:
        if not file_name:
            return False
        lowered = file_name.lower()
        return any(
            lowered.endswith(ext)
            for ext in (
                ".nfo",
                ".txt",
                ".jpg",
                ".jpeg",
                ".png",
                ".gif",
                ".webp",
                ".srt",
                ".sub",
                ".idx",
                ".ass",
                ".ssa",
                ".vtt",
            )
        )

    def _select_best_video_file(self, files):
        if not isinstance(files, list):
            return None

        video_files = []
        for file in files:
            if not isinstance(file, dict):
                continue
            file_name = file.get("name", "")
            if is_video_file(file_name):
                video_files.append(file)

        if video_files:
            return max(video_files, key=lambda x: x.get("size", 0))

        if files:
            valid_files = [f for f in files if isinstance(f, dict)]
            if valid_files:
                return valid_files[0]

        return None

    async def check_premium(self, ip=None):
        """Vérifie si l'utilisateur a un compte premium."""
        try:
            client_ip_param = f"&client_ip={ip}" if ip else ""
            response = await self.json_response(
                f"{self.base_url}/user?{client_ip_param}",
                headers=self._headers,
            )
            if response and "data" in response:
                return response["data"]["subscription_status"] == "premium"
        except Exception as e:
            logger.warning(
                f"Exception lors de la vérification du statut premium sur StremThru-{self.store_name}: {e}"
            )
        return False

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """Vérifie la disponibilité des torrents avec l'API StremThru"""
        if not hashes_or_magnets:
            return []

        if not self.store_name:
            self.auto_detect_store()

        if not self.store_name or not self._headers:
            logger.warning(
                "StremThru: Aucun store configuré, impossible de vérifier la disponibilité"
            )
            return []

        results = []
        session = await self._get_session()
        timeout = aiohttp.ClientTimeout(total=5)

        normalized_inputs = []
        for hash_or_magnet in hashes_or_magnets:
            magnet_url = self._to_magnet(hash_or_magnet)
            if magnet_url:
                normalized_inputs.append(magnet_url)

        if not normalized_inputs:
            logger.warning("StremThru: Aucun hash ou magnet valide à vérifier")
            return []

        chunk_size = 50
        for i in range(0, len(normalized_inputs), chunk_size):
            chunk = normalized_inputs[i : i + chunk_size]

            try:
                encoded_magnets = ",".join(quote(m, safe="") for m in chunk)
                url = f"{self.base_url}/magnets/check?magnet={encoded_magnets}"
                if ip:
                    url += f"&client_ip={ip}"

                logger.debug(
                    f"StremThru: Vérification de {len(chunk)} magnets sur StremThru-{self.store_name}"
                )

                async with session.get(
                    url, headers=self._headers, timeout=timeout
                ) as response:
                    if response.status != 200:
                        text = await response.text()
                        logger.warning(
                            f"StremThru: Erreur HTTP sur StremThru-{self.store_name}: {response.status} - {text[:500]}"
                        )
                        continue

                    try:
                        json_data = await response.json()
                    except Exception as json_e:
                        text = await response.text()
                        logger.warning(
                            f"StremThru: Erreur parsing JSON sur StremThru-{self.store_name}: {json_e} - body={text[:500]}"
                        )
                        continue

                    items = json_data.get("data", {}).get("items", [])
                    if not isinstance(items, list):
                        logger.warning(
                            f"StremThru: Réponse inattendue sur StremThru-{self.store_name}: {json_data}"
                        )
                        continue

                    logger.info(
                        f"StremThru: API raw returned {len(items)} items for {len(chunk)} requested magnets on store {self.store_name}"
                    )

                    status_counts = {}
                    parsed_count = 0
                    cached_count = 0

                    for item in items:
                        if not isinstance(item, dict):
                            logger.debug(
                                f"StremThru: Item ignoré car non dict: {item}"
                            )
                            continue

                        parsed_count += 1

                        item_hash = self._normalize_hash(item.get("hash"))
                        item_status = str(item.get("status", "")).lower()
                        item_files = item.get("files", [])
                        status_counts[item_status] = status_counts.get(item_status, 0) + 1

                        logger.debug(
                            "StremThru: item raw "
                            f"hash={item_hash} "
                            f"status={item_status} "
                            f"files={len(item_files) if isinstance(item_files, list) else 'invalid'} "
                            f"keys={list(item.keys())}"
                        )

                        if not item_hash:
                            logger.debug(
                                f"StremThru: Item ignoré car hash invalide: {item}"
                            )
                            continue

                        if item_status == "cached":
                            results.append(
                                {
                                    "hash": item_hash,
                                    "status": "cached",
                                    "files": item_files if isinstance(item_files, list) else [],
                                    "store_name": self.store_name,
                                    "debrid": StremThru.get_underlying_debrid_code(
                                        self.store_name
                                    ),
                                }
                            )
                            cached_count += 1
                            logger.debug(
                                f"StremThru: Magnet caché trouvé sur StremThru-{self.store_name}: {item_hash}"
                            )
                        else:
                            logger.debug(
                                f"StremThru: Item ignoré car status != cached : hash={item_hash}, status={item_status}"
                            )

                    logger.info(
                        f"StremThru: Parsed {parsed_count} items on store {self.store_name} — "
                        f"status_counts={status_counts} — kept_cached={cached_count}"
                    )

            except Exception as e:
                logger.warning(
                    f"StremThru: Erreur lors de la vérification des magnets sur StremThru-{self.store_name}: {e}"
                )

        logger.info(
            f"StremThru: Final cached results kept = {len(results)} / {len(normalized_inputs)} requested magnets on store {self.store_name}"
        )
        return results

    async def add_magnet(self, magnet, ip=None, torrent_file_content=None):
        """Ajoute un magnet à StremThru."""
        if not self.store_name:
            self.auto_detect_store()

        if not self.store_name or not self._headers:
            logger.error("StremThru: Aucun store configuré pour add_magnet")
            return None

        try:
            client_ip_param = f"?client_ip={ip}" if ip else ""
            url = f"{self.base_url}/magnets{client_ip_param}"
            session = await self._get_session()
            timeout = aiohttp.ClientTimeout(total=30)

            if torrent_file_content:
                logger.debug(
                    f"Ajout du fichier .torrent sur StremThru-{self.store_name}"
                )
                form_data = aiohttp.FormData()
                form_data.add_field(
                    "torrent",
                    torrent_file_content,
                    filename="file.torrent",
                    content_type="application/x-bittorrent",
                )
                async with session.post(
                    url, data=form_data, headers=self._headers, timeout=timeout
                ) as response:
                    if response.status in [200, 201]:
                        try:
                            json_data = await response.json()
                            if json_data and "data" in json_data:
                                logger.debug(
                                    f"Magnet ajouté avec succès sur StremThru-{self.store_name} (code: {response.status})"
                                )
                                return json_data["data"]
                        except Exception as json_e:
                            logger.warning(f"Erreur lors du parsing JSON: {json_e}")
                    else:
                        text = await response.text()
                        logger.error(
                            f"Erreur lors de l'ajout du magnet: {response.status} - {text}"
                        )
            else:
                normalized_magnet = self._to_magnet(magnet)
                if not normalized_magnet:
                    logger.error(
                        f"StremThru: Magnet invalide pour add_magnet: {str(magnet)[:80]}"
                    )
                    return None

                logger.debug(
                    f"Ajout du magnet sur StremThru-{self.store_name}: {normalized_magnet[:60]}..."
                )
                async with session.post(
                    url,
                    json={"magnet": normalized_magnet},
                    headers=self._headers,
                    timeout=timeout,
                ) as response:
                    if response.status in [200, 201]:
                        try:
                            json_data = await response.json()
                            if json_data and "data" in json_data:
                                logger.debug(
                                    f"Magnet ajouté avec succès sur StremThru-{self.store_name} (code: {response.status})"
                                )
                                return json_data["data"]
                        except Exception as json_e:
                            logger.warning(f"Erreur lors du parsing JSON: {json_e}")
                    else:
                        text = await response.text()
                        logger.error(
                            f"Erreur lors de l'ajout du magnet: {response.status} - {text}"
                        )
                        try:
                            err_json = json_lib.loads(text)
                            error = err_json.get("error", {})
                            error_code = error.get("code")
                            error_message = error.get("message", "")
                            upstream = (
                                error.get("__upstream_cause__")
                                or error.get("__cause__")
                                or {}
                            )
                            upstream_error_code = upstream.get("error") or upstream.get(
                                "code"
                            )
                            if "per 1 hour" in error_message or "per hour" in error_message:
                                error_code = "TORBOX_RATE_LIMIT"
                        except Exception:
                            error_code = None
                            upstream_error_code = None

                        raise DebridError(
                            f"StremThru error: {response.status}",
                            error_code=error_code,
                            upstream_error_code=upstream_error_code,
                        )
        except DebridError:
            raise
        except Exception as e:
            logger.warning(
                f"Erreur lors de l'ajout du magnet sur StremThru-{self.store_name}: {e}"
            )

        return None

    async def get_magnet_info(self, magnet_info, ip=None):
        """Récupère les informations d'un magnet."""
        if isinstance(magnet_info, dict):
            if "files" in magnet_info and "id" in magnet_info:
                logger.debug(
                    f"Utilisation des informations de magnet déjà disponibles pour {magnet_info.get('id')}"
                )
                return magnet_info

            magnet_id = magnet_info.get("id")
            if not magnet_id:
                logger.error("Aucun ID de magnet trouvé dans les informations fournies")
                return None
        else:
            magnet_id = magnet_info

        try:
            client_ip_param = f"?client_ip={ip}" if ip else ""
            url = f"{self.base_url}/magnets/{magnet_id}{client_ip_param}"
            session = await self._get_session()
            timeout = aiohttp.ClientTimeout(total=30)

            logger.debug(
                f"Récupération des informations du magnet {magnet_id} sur StremThru-{self.store_name}"
            )

            async with session.get(
                url, headers=self._headers, timeout=timeout
            ) as response:
                if response.status in [200, 201]:
                    try:
                        json_data = await response.json()
                        if json_data and "data" in json_data:
                            logger.debug(
                                f"Informations du magnet {magnet_id} récupérées avec succès"
                            )
                            return json_data["data"]
                    except Exception as json_e:
                        logger.warning(f"Erreur lors du parsing JSON: {json_e}")
                else:
                    text = await response.text()
                    logger.error(
                        f"Erreur lors de la récupération du magnet: {response.status} - {text}"
                    )

                    if isinstance(magnet_info, dict) and "files" in magnet_info:
                        logger.debug(
                            "Utilisation des informations de fichiers déjà disponibles dans le magnet"
                        )
                        return magnet_info
        except Exception as e:
            logger.warning(
                f"Erreur lors de la récupération du magnet {magnet_id}: {e}"
            )

            if isinstance(magnet_info, dict) and "files" in magnet_info:
                logger.debug(
                    "Utilisation des informations de fichiers déjà disponibles dans le magnet après erreur"
                )
                return magnet_info

        return None

    async def get_stream_link(self, query, config=None, ip=None):
        """Génère un lien de streaming à partir d'une requête."""
        try:
            logger.debug(f"StremThru: Génération d'un lien de streaming pour {query}")

            if not self.store_name:
                self.auto_detect_store()

            if not self.store_name:
                logger.error("StremThru: Aucun debrideur configuré pour StremThru")
                return None

            stream_type = query.get("type")
            if not stream_type:
                logger.error("StremThru: Le type de média n'est pas défini dans la requête")
                return None

            season = query.get("season")
            episode = query.get("episode")

            magnet_url = query.get("magnet")
            info_hash = query.get("infoHash")
            file_idx = query.get("file_index", query.get("fileIdx"))

            if magnet_url and not info_hash:
                info_hash = self._normalize_hash(magnet_url)

            if not info_hash:
                logger.error("StremThru: Aucun hash trouvé dans la requête")
                return None

            info_hash = self._normalize_hash(info_hash)
            if not info_hash:
                logger.error("StremThru: Hash invalide dans la requête")
                return None

            service = query.get("service")
            if service and service != "ST":
                logger.debug(
                    f"StremThru: Utilisation du service {service} spécifié dans la requête"
                )

            magnet = magnet_url or f"magnet:?xt=urn:btih:{info_hash}"

            magnet_data = None
            magnet_info = None

            cached_files = await self.get_availability_bulk([info_hash], ip)
            if cached_files:
                files = cached_files[0].get("files", [])
                has_playable_links = any(
                    isinstance(f, dict) and f.get("link")
                    for f in files
                )

                if files and has_playable_links:
                    logger.info(
                        f"StremThru: Torrent {info_hash} en cache avec liens exploitables, bypass createtorrent"
                    )
                    magnet_data = {
                        "files": files,
                        "id": info_hash,
                        "status": "cached",
                    }
                else:
                    logger.info(
                        f"StremThru: Torrent {info_hash} en cache mais sans liens exploitables, récupération complète requise"
                    )

            if not magnet_data:
                logger.debug(
                    f"StremThru: Ajout du magnet {magnet} via le store {self.store_name}"
                )
                magnet_info = await self.add_magnet(magnet, ip)

                if not magnet_info:
                    logger.error(f"StremThru: Impossible d'ajouter le magnet {info_hash}")
                    return None

                magnet_data = magnet_info

                if not magnet_data or "files" not in magnet_data:
                    magnet_id = magnet_info.get("id")
                    if magnet_id:
                        logger.debug(
                            f"StremThru: Récupération des informations du magnet {magnet_id}"
                        )
                        magnet_data = await self.get_magnet_info(magnet_info, ip)

                    if not magnet_data:
                        logger.error(
                            "StremThru: Impossible de récupérer les informations du magnet"
                        )
                        return None

            if "files" not in magnet_data:
                logger.error(
                    f"StremThru: Aucun fichier dans le magnet {magnet_data.get('id', 'inconnu')}"
                )
                return None

            files = magnet_data.get("files", [])
            if not isinstance(files, list) or not files:
                logger.error("StremThru: Aucun fichier exploitable trouvé dans le torrent")
                return None

            normalized_files = []
            for file in files:
                if not isinstance(file, dict):
                    continue
                normalized_files.append(
                    {
                        "index": file.get("index"),
                        "name": file.get("name", ""),
                        "size": file.get("size", 0),
                        "link": file.get("link"),
                    }
                )

            if not normalized_files:
                logger.error("StremThru: Aucun fichier valide après normalisation")
                return None

            target_file = None

            if stream_type == "series" and season and episode:
                try:
                    numeric_season = int(str(season).replace("S", ""))
                    numeric_episode = int(str(episode).replace("E", ""))

                    for file in normalized_files:
                        file_name = file.get("name", "")
                        if not is_video_file(file_name):
                            continue

                        if season_episode_in_filename(
                            file_name, numeric_season, numeric_episode
                        ):
                            target_file = file
                            logger.info(
                                f"StremThru: Fichier trouvé par NOM: {file_name} (index: {file.get('index')})"
                            )
                            break
                except Exception as e:
                    logger.warning(
                        f"StremThru: Erreur lors de la recherche par nom: {str(e)}"
                    )

            if target_file is None and file_idx is not None:
                try:
                    idx = int(file_idx)
                    target_file = next(
                        (
                            f
                            for f in normalized_files
                            if f.get("index") is not None and int(f.get("index")) == idx
                        ),
                        None,
                    )
                except (TypeError, ValueError):
                    target_file = None

                if target_file:
                    logger.info(
                        f"StremThru: Fichier trouvé par INDEX {file_idx}: {target_file.get('name')}"
                    )

                    if stream_type == "movie" and self._is_text_or_subtitle_file(
                        target_file.get("name", "")
                    ):
                        logger.warning(
                            f"StremThru: Le fichier à l'index {file_idx} n'est pas une vidéo: {target_file.get('name')}"
                        )
                        target_file = None

            if target_file is None and stream_type == "series" and season and episode:
                try:
                    video_files = [
                        f for f in normalized_files if is_video_file(f.get("name", ""))
                    ]

                    if video_files:
                        numeric_season = int(str(season).replace("S", ""))
                        numeric_episode = int(str(episode).replace("E", ""))
                        fallback_file = smart_episode_fallback(
                            video_files, numeric_season, numeric_episode
                        )

                        if fallback_file:
                            target_file = fallback_file
                            logger.info(
                                f"StremThru: Fichier trouvé par FALLBACK INTELLIGENT: {target_file.get('name')} (index: {target_file.get('index')})"
                            )
                except Exception as e:
                    logger.warning(
                        f"StremThru: Erreur lors du fallback intelligent: {str(e)}"
                    )

            if target_file is None:
                logger.debug(
                    "StremThru: Aucun fichier trouvé par nom ou index, recherche du plus gros fichier"
                )
                target_file = self._select_best_video_file(normalized_files)

                if target_file:
                    logger.info(
                        f"StremThru: Sélection du meilleur fichier vidéo: {target_file.get('name')} (index: {target_file.get('index')})"
                    )
                else:
                    logger.error("StremThru: Aucun fichier trouvé dans le torrent")
                    return None

            if not target_file or not target_file.get("link"):
                logger.error("StremThru: Fichier cible non trouvé ou sans lien")
                return None

            torrent_id = (
                (magnet_info or {}).get("id")
                or magnet_data.get("id", "")
                or info_hash
            )
            file_id = target_file.get("index", "")

            if stream_type == "series" and season and episode:
                logger.info(
                    f"StremThru: Sélection finale de {season}{episode} dans le torrent {torrent_id}, fichier: {target_file.get('name')} (index: {file_id})"
                )
            else:
                logger.info(
                    f"StremThru: Sélection finale du fichier {target_file.get('name')} (index: {file_id}) dans le torrent {torrent_id}"
                )

            client_ip_param = f"?client_ip={ip}" if ip else ""
            url = f"{self.base_url}/link/generate{client_ip_param}"
            session = await self._get_session()
            timeout = aiohttp.ClientTimeout(total=30)

            logger.debug(
                f"StremThru: Génération du lien pour {target_file.get('name')}"
            )

            if (
                self.store_name == "torbox"
                and isinstance(target_file.get("link"), str)
                and target_file["link"].startswith("stremthru://store/torbox/")
            ):
                try:
                    import base64 as _b64

                    link_token = target_file["link"].split(
                        "stremthru://store/torbox/"
                    )[1]
                    decoded = _b64.b64decode(link_token + "==").decode()
                    tb_torrent_id, tb_file_id = decoded.split(":")
                    tb_token = self.token
                    tb_url = (
                        "https://api.torbox.app/v1/api/torrents/requestdl"
                        f"?token={tb_token}&torrent_id={tb_torrent_id}&file_id={tb_file_id}&zip_link=false"
                    )
                    logger.info(
                        "StremThru-torbox: Appel direct requestdl (bypass CDN ceur)"
                    )
                    async with session.get(tb_url, timeout=timeout) as tb_resp:
                        if tb_resp.status == 200:
                            tb_data = await tb_resp.json()
                            if tb_data.get("success") and tb_data.get("data"):
                                stream_link = tb_data["data"]
                                logger.info(
                                    f"StremThru-torbox: Lien direct généré: {stream_link}"
                                )
                                return stream_link
                    logger.warning(
                        "StremThru-torbox: requestdl échoué, fallback StremThru"
                    )
                except Exception as e:
                    logger.warning(
                        f"StremThru-torbox: Erreur requestdl: {e}, fallback StremThru"
                    )

            json_data = {"link": target_file["link"]}

            try:
                async with session.post(
                    url, json=json_data, headers=self._headers, timeout=timeout
                ) as response:
                    if response.status in [200, 201]:
                        try:
                            resp_data = await response.json()
                            if (
                                resp_data
                                and "data" in resp_data
                                and "link" in resp_data["data"]
                            ):
                                stream_link = resp_data["data"]["link"]
                                logger.info(
                                    f"StremThru: Lien de streaming généré avec succès: {stream_link}"
                                )
                                return stream_link
                        except Exception:
                            stream_link = (await response.text()).strip()
                            if stream_link.startswith(("http://", "https://")):
                                logger.info(
                                    f"StremThru: Lien de streaming reçu directement: {stream_link}"
                                )
                                return stream_link
                            logger.error(
                                f"StremThru: Réponse non-JSON invalide: {stream_link[:100]}..."
                            )
                    else:
                        text = await response.text()
                        logger.error(
                            f"StremThru: Échec de la génération du lien de streaming: {response.status} - {text[:100]}..."
                        )
            except Exception as e:
                logger.error(f"StremThru: Erreur lors de la génération du lien: {str(e)}")

            return None

        except DebridError:
            raise
        except Exception as e:
            logger.warning(
                f"Erreur lors de la génération du lien sur StremThru-{self.store_name}: {e}"
            )

        return None

    async def start_background_caching(self, magnet, query=None):
        """Démarre le téléchargement d'un magnet en arrière-plan."""
        logger.info(
            f"Démarrage du téléchargement en arrière-plan pour un magnet via StremThru-{self.store_name}"
        )

        if not self.store_name:
            self.auto_detect_store()

        if not self.store_name:
            logger.error(
                "StremThru: Aucun store configuré pour start_background_caching"
            )
            return False

        try:
            torrent_file_content = None

            if self.store_name == "torbox":
                logger.info(
                    "StremThru-torbox: Using magnet only (TorBox ignores seed parameter with .torrent files)"
                )
            elif query and query.get("torrent_download"):
                torrent_download = unquote(query["torrent_download"])

                if not torrent_download.startswith("magnet:"):
                    logger.info(
                        f"Tentative de téléchargement du fichier .torrent pour StremThru-{self.store_name}: {torrent_download[:100]}"
                    )

                    try:
                        session = await self._get_session()
                        timeout = aiohttp.ClientTimeout(total=10)
                        async with session.get(
                            torrent_download, timeout=timeout
                        ) as response:
                            if response.status == 200:
                                content = await response.read()
                                if content and len(content) > 0 and content[0:1] == b"d":
                                    torrent_file_content = content
                                    logger.info(
                                        f"Fichier .torrent téléchargé et validé avec succès pour StremThru-{self.store_name}"
                                    )
                                else:
                                    logger.warning(
                                        f"Le contenu téléchargé n'est pas un fichier .torrent valide pour StremThru-{self.store_name}, fallback sur magnet"
                                    )
                            else:
                                logger.warning(
                                    f"Impossible de télécharger le fichier .torrent pour StremThru-{self.store_name}: {response.status}"
                                )
                    except Exception as e:
                        logger.warning(
                            f"Erreur lors du téléchargement du .torrent pour StremThru-{self.store_name}: {str(e)}, fallback sur magnet"
                        )

            result = None
            if torrent_file_content:
                try:
                    logger.debug(
                        f"Tentative d'ajout avec le fichier .torrent pour StremThru-{self.store_name}"
                    )
                    result = await self.add_magnet(
                        magnet, torrent_file_content=torrent_file_content
                    )
                except Exception as e:
                    logger.warning(
                        f"Échec de l'ajout du fichier .torrent pour StremThru-{self.store_name}: {str(e)}, fallback sur magnet"
                    )
                    result = None

            if not result:
                logger.info(f"Utilisation du magnet pour StremThru-{self.store_name}")
                result = await self.add_magnet(magnet, torrent_file_content=None)

            if not result:
                logger.error(
                    f"Échec du démarrage du téléchargement en arrière-plan via StremThru-{self.store_name}"
                )
                return False

            magnet_id = result.get("id")
            if not magnet_id:
                logger.error(
                    f"Aucun ID de magnet retourné par StremThru-{self.store_name}"
                )
                return False

            logger.info(
                f"Téléchargement en arrière-plan démarré avec succès via StremThru-{self.store_name}, ID: {magnet_id}"
            )
            return True
        except DebridError:
            raise
        except Exception as e:
            logger.error(
                f"Erreur lors du démarrage du téléchargement en arrière-plan via StremThru-{self.store_name}: {str(e)}"
            )
            return False