from fastapi.routing import APIRouter

from stream_fusion.web.api import auth, docs, monitoring, admin
from stream_fusion.web.api.peer import router as peer_router

api_router = APIRouter()
api_router.include_router(docs.router)
api_router.include_router(auth.router, prefix="/auth", tags=["_auth"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])
api_router.include_router(monitoring.router, prefix="/monitoring", tags=["monitoring"])
api_router.include_router(peer_router.router, prefix="/peer", tags=["peer"])
