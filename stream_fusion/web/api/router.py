from fastapi.routing import APIRouter

from stream_fusion.web.api import auth, docs, monitoring
from stream_fusion.web.api.peer import router as peer_router
from stream_fusion.web.api.config import router as config_router

api_router = APIRouter()
api_router.include_router(docs.router)
api_router.include_router(auth.router, prefix="/auth", tags=["_auth"])
api_router.include_router(monitoring.router, prefix="/monitoring", tags=["monitoring"])
api_router.include_router(peer_router.router, prefix="/peer", tags=["peer"])
api_router.include_router(config_router, prefix="/config", tags=["config"])
