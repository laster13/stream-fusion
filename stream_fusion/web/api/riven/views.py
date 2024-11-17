from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi_simple_rate_limiter import rate_limiter
from redis import Redis
import asyncio
import json
import requests
import logging
from tmdbv3api import TMDb, Find
from urllib.parse import quote_plus, urljoin
from typing import List, Optional
from RTN.models import ParsedData
from stream_fusion.utils.yggfilx.yggflix_api import YggflixAPI
from stream_fusion.utils.zilean.zilean_api import ZileanAPI
from stream_fusion.utils.detection import detect_languages
from stream_fusion.utils.filter_results import filter_items
from stream_fusion.settings import settings
from stream_fusion.services.redis.redis_config import get_redis
from stream_fusion.logging_config import logger
from .schemas import RivenResponse, RivenResult, ErrorResponse, MediaTypes
from RTN import parse
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


router = APIRouter()

# Configuration TMDB API
tmdb = TMDb()
tmdb.api_key = settings.tmdb_api_key
tmdb.language = "fr-FR"
find = Find()


async def search_by_tmdb_id(
    yggflix: YggflixAPI, tmdb_id: int, type: str = "movie"
) -> List[dict]:
    """
    Search for torrents using a TMDB ID.

    Args:
        yggflix (YggflixAPI): Instance of YggflixAPI
        tmdb_id (int): TMDB ID to search for
        type (str): Media type ("movie" or "tv")

    Returns:
        List[dict]: List of torrent results
    """
    try:
        if type == MediaTypes.movie:
            return await asyncio.to_thread(yggflix.get_movie_torrents, tmdb_id)
        elif type == MediaTypes.tv:
            return await asyncio.to_thread(yggflix.get_tvshow_torrents, tmdb_id)
        return []
    except Exception as e:
        logger.error(f"Error searching by TMDB ID {tmdb_id}: {e}")
        return []


async def get_tmdb_id_from_imdb(imdb_id: str) -> Optional[str]:
    """
    Convert an IMDB ID to a TMDB ID.

    Args:
        imdb_id (str): IMDB ID to convert

    Returns:
        Optional[str]: TMDB ID if found, None otherwise
    """
    try:
        results = await asyncio.to_thread(find.find_by_imdb_id, imdb_id)
        if results.movie_results:
            return str(results.movie_results[0]["id"])
        elif results.tv_results:
            return str(results.tv_results[0]["id"])
        return None
    except Exception as e:
        logger.error(f"Error converting IMDB ID {imdb_id} to TMDB ID: {e}")
        return None


def __process_download_link(id: int, ygg_passkey) -> str:
    """Generate the download link for a given torrent."""
    if settings.yggflix_url:
        return f"https://yggapi.eu/torrent/{id}/download?passkey={ygg_passkey}"


def __process_magnet_link(
    ygg_passkey: str,
    info_hash: str,
    title: str = "unknown",
    announce_base: str = "http://connect.maxp2p.org:8080"
) -> str:
    """
    Generate a properly URL-encoded magnet link for a torrent.
    
    Args:
        ygg_passkey (str): User's YGG passkey for authentication
        info_hash (str): Torrent's info hash
        title (str, optional): Title of the torrent. Defaults to "unknown"
        announce_base (str, optional): Base announce URL. 
            Defaults to "http://connect.maxp2p.org:8080"
    
    Returns:
        str: Properly formatted and encoded magnet link
    
    Example:
        >>> process_magnet_link("abc123", "HASH123", "My Movie (2024)")
        'magnet:?xt=urn:btih:HASH123&dn=My+Movie+%282024%29&tr=http%3A%2F%2Fconnect.maxp2p.org%3A8080%2Fabc123%2Fannounce'
    """
    info_hash = info_hash.strip().lower()
    encoded_title = quote_plus(title)
    tracker_url = urljoin(announce_base.rstrip('/') + '/', f"{ygg_passkey}/announce")
    encoded_tracker = quote_plus(tracker_url)
    magnet_link = (
        f"magnet:?xt=urn:btih:{info_hash}"
        f"&dn={encoded_title}"
        f"&tr={encoded_tracker}"
    )
    return magnet_link

@router.get(
    "/yggflix",
    response_model=RivenResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
@rate_limiter(limit=40, seconds=60)  # Augmenter la limite si nécessaire
async def search_yggflix(
    type: MediaTypes = Query(
        MediaTypes.movie, description="Type of media (movie or tv)"
    ),
    ygg_passkey: str = Query(..., description="Ygg passkey"),
    query: Optional[str] = Query(None, description="Keyword search query"),
    tmdb_id: Optional[str] = Query(None, description="TMDB ID"),
    imdb_id: Optional[str] = Query(None, description="IMDB ID"),
    year: Optional[int] = Query(None, description="Year of release for movies"),
    season: Optional[str] = Query("00", description="Season number for series"),
    episode: Optional[str] = Query("00", description="Episode number for series"),
    redis_client: Redis = Depends(get_redis),
    per_page: int = Query(100, description="Number of results per page"),
) -> RivenResponse:

    try:
        # Vérifier si au moins un paramètre de recherche est fourni
        if not any([query, tmdb_id, imdb_id]):
            raise HTTPException(
                status_code=400,
                detail="At least one search parameter (query, tmdb_id, or imdb_id) must be provided",
            )

        yggflix = YggflixAPI()
        search_results = []
        final_tmdb_id = None
        final_imdb_id = imdb_id

        # IMDB ID search path
        if imdb_id:
            final_tmdb_id = await get_tmdb_id_from_imdb(imdb_id)
            if final_tmdb_id:
                search_results = await search_by_tmdb_id(
                    yggflix, int(final_tmdb_id), type
                )
            else:
                logger.warning(f"No TMDB ID found for IMDB ID: {imdb_id}")
                return RivenResponse(
                    query=imdb_id,
                    tmdb_id=None,
                    imdb_id=imdb_id,
                    total_results=0,
                    results=[],
                )

        # TMDB ID search path
        elif tmdb_id:
            final_tmdb_id = tmdb_id
            search_results = await search_by_tmdb_id(yggflix, int(tmdb_id), type)

        else:
            search_results = await asyncio.to_thread(
                yggflix._make_request,
                "GET",
                "/torrents",
                params={"q": query, "order_by": "uploaded_at", "per_page": 100}
            )

        # Traitement des résultats de recherche
        torrent_results = []
        for torrent in search_results:
            logger.debug(f"Processing torrent result: {torrent}")
            result = await asyncio.to_thread(yggflix.get_torrent_detail, torrent.get("id"))
            try:
                raw_title = result.get("title", "unknown")
                info_hash = result.get("hash", "").lower() if result.get("hash") else ""

                torrent_results.append(
                    RivenResult(
                        raw_title=raw_title,
                        size=result.get("size"),
                        link=__process_download_link(
                            id=result.get("id"), ygg_passkey=ygg_passkey
                        ),
                        seeders=result.get("seeders"),
                        magnet=__process_magnet_link(
                            ygg_passkey=ygg_passkey,
                            info_hash=result.get("hash", "").lower(),
                            title=result.get("title", "unknown"),
                        ),
                        info_hash=result.get("hash", "").lower(),
                        privacy=result.get("privacy", "private"),
                        languages=result.get("languages", ["fr"]),
                        type=type,
                        source="yggflix",
                        parsed_data=parse(raw_title)  # Utilisation de parse pour parsed_data
                )
        )
            except Exception as e:
                logger.error(f"Error processing torrent result: {e}")
                continue

        return RivenResponse(
            query=query or f"tmdbid:{final_tmdb_id}" or f"imdbid:{final_imdb_id}",
            tmdb_id=final_tmdb_id,
            imdb_id=final_imdb_id,
            total_results=len(torrent_results),
            results=torrent_results,
        )

    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while processing the search request.",
        )

### ZILEAN API HANDLER

class ZileanAPI:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "http://zilean:8181"  # Port sur lequel fonctionne Zilean
        self.logger = logging.getLogger(__name__)

    def dmm_filtered(self, query: str, season: Optional[int] = None, episode: Optional[int] = None,
                     year: Optional[int] = None, language: Optional[str] = None, resolution: Optional[str] = None,
                     imdb_id: Optional[str] = None):
        self.logger.info(f"Performing filtered search with query: {query}, season: {season}, episode: {episode}, "
                         f"year: {year}, language: {language}, resolution: {resolution}, imdb_id: {imdb_id}")
        
        params = {
            "query": query,
            "Season": season,
            "Episode": episode,
            "Year": year,
            "Language": language,
            "Resolution": resolution,
            "ImdbId": imdb_id
        }
        
        params = {k: v for k, v in params.items() if v is not None}

        self.logger.info(f"Sending request to Zilean with parameters: {params}")
        
        response = self.session.get(f"{self.base_url}/dmm/filtered", params=params)

        self.logger.info(f"Response status code from Zilean: {response.status_code}")
        self.logger.info(f"Raw response content from Zilean: {response.text}")

        if response.status_code != 200:
            self.logger.error(f"Zilean API returned an error: {response.status_code}")
            return []

        if not response.text.strip():
            self.logger.error("Zilean API returned an empty response.")
            return []

        try:
            return [self._convert_to_dmm_torrent_info(entry) for entry in response.json()]
        except ValueError as e:
            self.logger.error(f"Error parsing JSON from Zilean: {e}")
            return []

    def _convert_to_dmm_torrent_info(self, entry):
        return entry


@router.get(
    "/zilean/dmm/filtered",
    response_model=RivenResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
@rate_limiter(limit=20, seconds=60)
async def filtered_search_zilean(
    query: str,
    season: Optional[int] = Query(None, description="Season number"),
    episode: Optional[int] = Query(None, description="Episode number"),
    redis_client: Redis = Depends(get_redis),
) -> RivenResponse:
    """
    Effectue une recherche filtrée sur Zilean avec les paramètres fournis.
    """
    try:
        logger.info(f"Zilean filtered search request received with query={query}, season={season}, episode={episode}")

        zilean = ZileanAPI()
        search_results = await asyncio.to_thread(zilean.dmm_filtered, query=query, season=season, episode=episode)

        # Process search results and apply filters
        french_results = []
        for torrent in search_results:
            if "languages" not in torrent:
                torrent["languages"] = []
            if "indexer" not in torrent:
                torrent["indexer"] = "DMM - API"
            
            # Filtering logic based on languages and title
            detected_languages = detect_languages(torrent.get("raw_title", ""))
            if "fr" in detected_languages or ("multi" in detected_languages and "fr" in torrent["languages"]):
                french_results.append(torrent)

        # Build the final results in the expected format
        torrent_results = []
        for torrent in french_results:
            parsed_data = parse(torrent.get("raw_title", "unknown"))  # parse() must return a ParsedData object
            torrent_results.append(
                RivenResult(
                    raw_title=torrent.get("raw_title", "unknown"),
                    size=torrent.get("size"),
                    link=f"{zilean.base_url}/torrent/{torrent.get('info_hash')}/download",
                    seeders=torrent.get("seeders", 0),
                    magnet=f"magnet:?xt=urn:btih:{torrent.get('info_hash')}",
                    info_hash=torrent.get("info_hash", "").lower(),
                    privacy=torrent.get("privacy", "private"),
                    languages=torrent.get("languages", ["fr"]),
                    type="movie",
                    source="zilean",
                    parsed_data=parsed_data  # Ensure parsed_data is of type ParsedData
                )
            )

        # Return as a RivenResponse instance
        return RivenResponse(query=query, total_results=len(torrent_results), results=torrent_results)

    except Exception as e:
        logger.error(f"Error during Zilean filtered search: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred while processing the search request: {str(e)}")
