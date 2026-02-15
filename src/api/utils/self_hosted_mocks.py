"""
Mock data for self-hosted deployment. Autumn billing is fully bypassed.
Used by autumn_mount.py (ASGI stub) and platform.py (API endpoints).

Every feature includes `unlimited: True` so the frontend helpers
(autumn-helpers.ts) display "Unlimited" instead of a raw number.
"""

# Mock features aligned with feature_gate.py + autumn-helpers.ts
# Fields required by frontend:
#   - id, type, name, balance, included_usage, usage  (autumn-helpers.ts)
#   - unlimited  (autumn-helpers.ts: getWorkflowLimits, getMachineLimits)
#   - interval, interval_count, overage_allowed, next_reset_at  (autumn-v2.ts Feature type)
_FEATURE_DEFAULTS = {
    "unlimited": True,
    "interval": None,
    "interval_count": None,
    "overage_allowed": True,
    "next_reset_at": None,
}

MOCK_FEATURES = {
    "workflow_limit": {
        "id": "workflow_limit",
        "type": "continuous_use",
        "name": "Workflow Limit",
        "balance": 999999,
        "included_usage": 999999,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "machine_limit": {
        "id": "machine_limit",
        "type": "continuous_use",
        "name": "Machine Limit",
        "balance": 999999,
        "included_usage": 999999,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "self_hosted_machines": {
        "id": "self_hosted_machines",
        "type": "static",
        "name": "Self Hosted Machines",
        "balance": 1,
        "included_usage": 1,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "gpu_concurrency_limit": {
        "id": "gpu_concurrency_limit",
        "type": "continuous_use",
        "name": "GPU Concurrency Limit",
        "balance": 100,
        "included_usage": 100,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "gpu-credit": {
        "id": "gpu-credit",
        "type": "credit_system",
        "name": "GPU Credit",
        "balance": 99999900,
        "included_usage": 99999900,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "gpu-credit-topup": {
        "id": "gpu-credit-topup",
        "type": "credit_system",
        "name": "GPU Credit Top-up",
        "balance": 99999900,
        "included_usage": 99999900,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "custom_s3": {
        "id": "custom_s3",
        "type": "static",
        "name": "Custom S3",
        "balance": 1,
        "included_usage": 1,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "max_always_on_machine": {
        "id": "max_always_on_machine",
        "type": "continuous_use",
        "name": "Max Always On Machine",
        "balance": 10,
        "included_usage": 10,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
    "seats": {
        "id": "seats",
        "type": "continuous_use",
        "name": "Seats",
        "balance": 999999,
        "included_usage": 999999,
        "usage": 0,
        **_FEATURE_DEFAULTS,
    },
}

MOCK_CUSTOMER = {
    "id": "self_hosted",
    "name": "Self Hosted",
    "email": None,
    "created_at": 0,
    "fingerprint": None,
    "stripe_id": "",
    "env": "self_hosted",
    "metadata": {},
    "products": [
        {
            "id": "self_hosted_enterprise",
            "name": "Self Hosted Enterprise",
            "status": "active",
            "group": None,
            "canceled_at": None,
            "started_at": 0,
            "is_default": True,
            "is_add_on": False,
            "version": 1,
            "items": [],
            "prices": [],
            "subscription_ids": [],
        }
    ],
    "add_ons": [],
    "invoices": [],
    "features": MOCK_FEATURES,
}

# Response for POST /api/autumn/check — used by autumn-js SDK's check() method.
# Must include every field the frontend accesses:
#   .data.allowed          (workflow-list, machine-create, user-settings)
#   .data.balance          (useCredit)
#   .data.unlimited        (general)
#   .data.included_usage   (workflow-list, machine-settings)
#   .data.usage            (workflow-list, OrgSelector)
#   .data.customer_id      (OrgSelector)
MOCK_CHECK_RESPONSE = {
    "allowed": True,
    "balance": 999999,
    "unlimited": True,
    "included_usage": 999999,
    "usage": 0,
    "customer_id": "self_hosted",
}
