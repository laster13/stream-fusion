from fastapi.routing import APIRouter

from stream_fusion.web.api.peer import views

router = APIRouter()
router.include_router(views.router)
