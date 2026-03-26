from fastapi.routing import APIRouter

from stream_fusion.web.api.share import views

router = APIRouter()
router.include_router(views.router, prefix="/cache", tags=["share"])
