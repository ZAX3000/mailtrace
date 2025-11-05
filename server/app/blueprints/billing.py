# app/blueprints/billing.py
from __future__ import annotations

from typing import Any, Optional, cast

from flask import Blueprint, current_app, jsonify, redirect, request, session, url_for
from flask.typing import ResponseReturnValue

from ..extensions import db, stripe
from ..models import Subscription, User

billing_bp = Blueprint("billing", __name__)


@billing_bp.get("/paywall")
def paywall() -> ResponseReturnValue:
    # Static asset – let Flask serve it
    return current_app.send_static_file("paywall.html")


@billing_bp.post("/checkout")
def checkout() -> ResponseReturnValue:
    # Require an authenticated user
    if "user_id" not in session:
        return redirect(url_for("auth.login"))

    # Config presence checks (fail fast with 400 instead of 500)
    base = current_app.config.get("STRIPE_PRICE_BASE")
    metered = current_app.config.get("STRIPE_PRICE_METERED")
    success = current_app.config.get("STRIPE_SUCCESS_URL")
    cancel = current_app.config.get("STRIPE_CANCEL_URL")
    if not all([base, metered, success, cancel]):
        return jsonify({"error": "stripe_config_missing"}), 400

    email: Optional[str] = cast(Optional[str], session.get("email"))

    try:
        session_obj = stripe.checkout.Session.create(
            mode="subscription",
            customer_email=email,
            line_items=[
                {"price": base, "quantity": 1},
                {"price": metered, "quantity": 1},  # metered item
            ],
            success_url=success,
            cancel_url=cancel,
            automatic_tax={"enabled": False},
        )
    except Exception as e:
        current_app.logger.exception("Stripe checkout session failed: %s", e)
        return jsonify({"error": "stripe_checkout_failed"}), 502

    return jsonify({"checkout_url": session_obj.url})


@billing_bp.post("/stripe/webhook")
def webhook() -> ResponseReturnValue:
    # Stripe sends raw bytes; signature header required
    payload: bytes = request.data
    sig: Optional[str] = request.headers.get("Stripe-Signature")
    whsec: Optional[str] = current_app.config.get("STRIPE_WEBHOOK_SECRET")
    if not sig or not whsec:
        return jsonify({"error": "missing_signature"}), 400

    try:
        # Construct and verify event
        event = stripe.Webhook.construct_event(payload, sig, whsec)
    except Exception as e:
        current_app.logger.warning("Stripe webhook signature/parse failed: %s", e)
        return jsonify({"error": "invalid_signature"}), 400

    etype: str = event.get("type", "")
    data: dict[str, Any] = event.get("data", {}).get("object", {})

    try:
        if etype == "checkout.session.completed":
            _handle_checkout_completed(data)
        elif etype == "invoice.payment_succeeded":
            _handle_invoice_paid(data)
        elif etype == "customer.subscription.deleted":
            _handle_subscription_deleted(data)
        else:
            # Unhandled; don't error (Stripe will keep retrying for 4xx/5xx)
            current_app.logger.debug("Unhandled Stripe event: %s", etype)
    except Exception as e:
        current_app.logger.exception("Stripe webhook handler error (%s): %s", etype, e)
        # 200 with empty body tells Stripe we handled it (prevents retries);
        # if you want retries, return 500 instead — but be sure your handler is idempotent.
        return "", 200

    return "", 200


# ---------------------------
# Webhook handler helpers
# ---------------------------

def _handle_checkout_completed(data: dict[str, Any]) -> None:
    """
    When a Checkout Session completes, attach customer/subscription IDs to our user,
    capture metered item id, and mark active.
    """
    customer_id = data.get("customer")
    subscription_id = data.get("subscription")
    email = (data.get("customer_details") or {}).get("email")

    if not subscription_id or not email:
        current_app.logger.warning("checkout.session.completed missing fields: %s", data)
        return

    user = db.session.execute(
        db.select(User).where(User.email == email)
    ).scalar_one_or_none()
    if not user:
        current_app.logger.info("No user for email %s on checkout completion", email)
        return

    sub = db.session.execute(
        db.select(Subscription).where(Subscription.user_id == user.id)
    ).scalar_one_or_none()
    if not sub:
        sub = Subscription(user_id=user.id)
        db.session.add(sub)

    sub.stripe_customer_id = customer_id
    sub.stripe_subscription_id = subscription_id

    # Retrieve subscription to find the metered item id
    try:
        subscription = stripe.Subscription.retrieve(subscription_id)
        items = getattr(subscription, "items", None)
        data_list = getattr(items, "data", []) if items is not None else []
        metered_price = current_app.config.get("STRIPE_PRICE_METERED")
        metered_item_id: Optional[str] = None
        for it in data_list:
            # Stripe returns objects with attribute access; be defensive
            price = getattr(it, "price", None)
            price_id = getattr(price, "id", None)
            if price_id == metered_price:
                metered_item_id = getattr(it, "id", None)
                break
        sub.metered_item_id = metered_item_id
    except Exception as e:
        current_app.logger.warning("Stripe Subscription.retrieve failed: %s", e)
        # proceed without metered item id; usage reporting may fail later

    sub.status = "active"
    db.session.commit()


def _handle_invoice_paid(data: dict[str, Any]) -> None:
    sub_id = data.get("subscription")
    if not sub_id:
        return
    sub = db.session.execute(
        db.select(Subscription).where(Subscription.stripe_subscription_id == sub_id)
    ).scalar_one_or_none()
    if sub:
        sub.status = "active"
        db.session.commit()


def _handle_subscription_deleted(data: dict[str, Any]) -> None:
    sub_id = data.get("id")
    if not sub_id:
        return
    sub = db.session.execute(
        db.select(Subscription).where(Subscription.stripe_subscription_id == sub_id)
    ).scalar_one_or_none()
    if sub:
        sub.status = "canceled"
        db.session.commit()