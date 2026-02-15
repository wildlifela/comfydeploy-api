import logging
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class SubscriptionMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        try:
            if request.url.path.startswith("/api"):
                await self.check_subscription_access(request)
            response = await call_next(request)
            return response
        except HTTPException as exc:
            return JSONResponse(
                status_code=exc.status_code, content={"detail": exc.detail}
            )

    async def check_subscription_access(self, request: Request):
        # Always set plan to enterprise — billing is fully bypassed
        if (
            not hasattr(request.state, "current_user")
            or request.state.current_user is None
        ):
            return

        request.state.current_user["plan"] = "enterprise"
