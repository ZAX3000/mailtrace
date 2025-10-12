import json, os, time
from flask import Blueprint, current_app, request, jsonify, redirect, url_for, session
from .extensions import stripe, db
from .models import Subscription, User

billing_bp = Blueprint("billing", __name__)

@billing_bp.get("/paywall")
def paywall():
    return current_app.send_static_file("paywall.html")

@billing_bp.post("/checkout")
def checkout():
    if "user_id" not in session:
        return redirect(url_for("auth.login"))
    base = current_app.config["STRIPE_PRICE_BASE"]
    metered = current_app.config["STRIPE_PRICE_METERED"]
    success = current_app.config["STRIPE_SUCCESS_URL"]
    cancel = current_app.config["STRIPE_CANCEL_URL"]
    email = session.get("email")

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
    return jsonify({"checkout_url": session_obj.url})

@billing_bp.post("/stripe/webhook")
def webhook():
    payload = request.data
    sig = request.headers.get("Stripe-Signature")
    whsec = current_app.config["STRIPE_WEBHOOK_SECRET"]
    try:
        event = stripe.Webhook.construct_event(payload, sig, whsec)
    except Exception as e:
        return str(e), 400

    etype = event["type"]
    data = event["data"]["object"]

    if etype == "checkout.session.completed":
        # Create/update subscription row
        customer_id = data.get("customer")
        subscription_id = data.get("subscription")
        email = data.get("customer_details", {}).get("email")

        user = db.session.execute(db.select(User).where(User.email==email)).scalar_one_or_none()
        if not user:
            return "", 200

        sub = db.session.execute(db.select(Subscription).where(Subscription.user_id==user.id)).scalar_one_or_none()
        if not sub:
            sub = Subscription(user_id=user.id)
            db.session.add(sub)

        sub.stripe_customer_id = customer_id
        sub.stripe_subscription_id = subscription_id
        # find metered item id
        items = stripe.Subscription.retrieve(subscription_id).items.data
        metered_price = current_app.config["STRIPE_PRICE_METERED"]
        metered_item_id = None
        for it in items:
            if it.price.id == metered_price:
                metered_item_id = it.id
                break
        sub.metered_item_id = metered_item_id
        sub.status = "active"
        db.session.commit()

    elif etype == "invoice.payment_succeeded":
        sub_id = data.get("subscription")
        sub = db.session.execute(db.select(Subscription).where(Subscription.stripe_subscription_id==sub_id)).scalar_one_or_none()
        if sub:
            sub.status = "active"
            db.session.commit()

    elif etype == "customer.subscription.deleted":
        sub_id = data.get("id")
        sub = db.session.execute(db.select(Subscription).where(Subscription.stripe_subscription_id==sub_id)).scalar_one_or_none()
        if sub:
            sub.status = "canceled"
            db.session.commit()

    return "", 200
