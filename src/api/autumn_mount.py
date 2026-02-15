import os
from fastapi import HTTPException
from typing import TYPE_CHECKING, Optional, TypedDict
from clerk_backend_api import Clerk
from api.utils.multi_level_cache import multi_level_cached

if TYPE_CHECKING:
    from starlette.requests import Request as StarletteRequest


EmailAndName = TypedDict("EmailAndName", {"email": Optional[str], "name": str})


@multi_level_cached(
    key_prefix="customer_clerk_customer_name",
    ttl_seconds=60,
    redis_ttl_seconds=120,
    version="1.0",
    key_builder=lambda customer_id: f"customer_clerk_customer_name:{customer_id}",
)
async def get_customer_clerk_name_cached(customer_id: str) -> EmailAndName:
    async with Clerk(
        bearer_auth=os.getenv("CLERK_SECRET_KEY"),
    ) as clerk:
        if customer_id.startswith("org_"):
            org = await clerk.organizations.get_async(organization_id=customer_id)
            return EmailAndName(email=None, name=org.name)
        else:
            user = await clerk.users.get_async(user_id=customer_id)
            return EmailAndName(email=user.email_addresses[0].email_address, name=user.first_name + " " + user.last_name)


async def identify(request: "StarletteRequest"):
    """Identify the current user from the request state."""
    current_user = getattr(request.state, 'current_user', None)

    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user_id = current_user.get("user_id", "unknown")
    org_id = current_user.get("org_id")

    customer_id = org_id if org_id else user_id

    customer_data = await get_customer_clerk_name_cached(customer_id)

    customer_data = {
        "name": customer_data["name"],
        "email": customer_data["email"],
    }

    return {
        "customer_id": customer_id,
        "customer_data": customer_data
    }


def _build_autumn_app():
    """Build a stub ASGI app. Autumn billing is fully bypassed."""
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from api.utils.self_hosted_mocks import MOCK_CUSTOMER, MOCK_CHECK_RESPONSE, MOCK_FEATURES

    # --- Endpoint handlers ---

    async def customers_endpoint(request):
        """POST /customers/ — autumn-js SDK fetches customer data here."""
        return JSONResponse(MOCK_CUSTOMER)

    async def customer_get_endpoint(request):
        """GET /customer/{id}/ — original Autumn ASGI route."""
        return JSONResponse(MOCK_CUSTOMER)

    async def check_endpoint(request):
        """POST /check/ — feature access check."""
        return JSONResponse(MOCK_CHECK_RESPONSE)

    async def checkout_endpoint(request):
        """POST /checkout/"""
        return JSONResponse({"success": True, "message": "Billing bypassed"})

    async def attach_endpoint(request):
        """POST /attach/"""
        return JSONResponse({"success": True, "message": "Billing bypassed"})

    async def track_endpoint(request):
        """POST /track/"""
        return JSONResponse({"success": True})

    async def usage_endpoint(request):
        """POST /usage/"""
        return JSONResponse({"success": True})

    async def billing_portal_endpoint(request):
        """POST /billing_portal/ — autumn-js SDK calls this for openBillingPortal()."""
        return JSONResponse({"url": None, "message": "Billing bypassed"})

    async def pricing_table_endpoint(request):
        """GET /components/pricing_table/ — autumn-js SDK calls this."""
        return JSONResponse({"products": [], "message": "Billing bypassed"})

    async def entitled_endpoint(request):
        """GET /entitled/"""
        return JSONResponse({"allowed": True, "features": MOCK_FEATURES})

    async def fallback_endpoint(request):
        """Catch-all for any other /api/autumn/* path."""
        return JSONResponse({"success": True, "self_hosted": True})

    # Routes cover both with and without trailing slash (TrailingSlashMiddleware adds them).
    # The SDK calls POST /customers, POST /check, POST /billing_portal, etc.
    routes = [
        # autumn-js SDK endpoints (POST)
        Route("/customers", customers_endpoint, methods=["POST"]),
        Route("/customers/", customers_endpoint, methods=["POST"]),
        Route("/check", check_endpoint, methods=["POST"]),
        Route("/check/", check_endpoint, methods=["POST"]),
        Route("/checkout", checkout_endpoint, methods=["POST"]),
        Route("/checkout/", checkout_endpoint, methods=["POST"]),
        Route("/attach", attach_endpoint, methods=["POST"]),
        Route("/attach/", attach_endpoint, methods=["POST"]),
        Route("/track", track_endpoint, methods=["POST"]),
        Route("/track/", track_endpoint, methods=["POST"]),
        Route("/billing_portal", billing_portal_endpoint, methods=["POST"]),
        Route("/billing_portal/", billing_portal_endpoint, methods=["POST"]),
        Route("/usage", usage_endpoint, methods=["POST"]),
        Route("/usage/", usage_endpoint, methods=["POST"]),

        # autumn-js SDK endpoints (GET)
        Route("/components/pricing_table", pricing_table_endpoint, methods=["GET"]),
        Route("/components/pricing_table/", pricing_table_endpoint, methods=["GET"]),

        # Original Autumn ASGI routes (GET)
        Route("/customer/", customer_get_endpoint, methods=["GET"]),
        Route("/customer/{customer_id:path}", customer_get_endpoint, methods=["GET"]),
        Route("/entitled/", entitled_endpoint, methods=["GET"]),
        Route("/entitled", entitled_endpoint, methods=["GET"]),

        # Catch-all
        Route("/{path:path}", fallback_endpoint),
    ]

    stub = Starlette(routes=routes)

    # Add async close() for lifecycle compatibility (router.py:67)
    async def _close():
        pass
    stub.close = _close

    return stub


# Initialize Autumn app (stub — billing fully bypassed)
autumn_app = _build_autumn_app()
