from services.query import get_option_history, get_option_latest
import boto3
import logging
from botocore.exceptions import ClientError
from fastapi import APIRouter, Query, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/history")
async def get_history(
    symbol: str = Query(..., description="e.g. NIFTY25400CE"),
    expiry: str = Query(..., description="e.g. 2026-02-24"),
    range: str = Query(..., description="1D | 1W | 1M | MAX"),
    reqId: int = Query(..., description="Request counter for stale response handling"),
):
    if range not in ["1D", "1W", "1M", "MAX"]:
        raise HTTPException(status_code=400, detail="range must be 1D, 1W, 1M or MAX")

    data = get_option_history(symbol, expiry, range)

    return {
        "reqId": reqId,
        "symbol": symbol,
        "expiry": expiry,
        "range": range,
        "data": data,
    }


@router.get("/api/latest")
async def get_latest(
    symbol: str = Query(..., description="e.g. NIFTY25400CE"),
    expiry: str = Query(..., description="e.g. 2026-02-24"),
    reqId: int = Query(..., description="Request counter for stale response handling"),
):
    data = get_option_latest(symbol, expiry)

    if not data:
        raise HTTPException(status_code=404, detail="Option not found")

    return {
        "reqId": reqId,
        **data,
    }


@router.post("/api/email-alert")
async def subscribe_email_alert(payload: dict):
    """
    Payload: {
      "option": "NIFTY25400CE",
      "email": "user@example.com",
      "risk": "75",
      "expiry": "2026-02-24"
    }
    """
    option_val = payload.get("option")
    email_val = payload.get("email")
    risk_val = payload.get("risk")
    expiry_val = payload.get("expiry")

    if not all([option_val, email_val, risk_val, expiry_val]):
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: option, email, risk, expiry",
        )

    email_val = email_val.strip().lower()

    logger.info("subscribe_email_alert called: %s", payload)

    try:
        session = boto3.Session(profile_name="Absc")
        ses = session.client("ses", region_name="ap-south-1")

        # Check current verification status
        verification = ses.get_identity_verification_attributes(Identities=[email_val])
        attrs = verification.get("VerificationAttributes", {})
        info = attrs.get(email_val)
        status = info.get("VerificationStatus") if info else None

        logger.info("SES verification status for %s: %s", email_val, status or "NONE")

        # Only send verification email if not already verified
        if status != "Success":
            logger.info("Email %s not verified (status=%s), sending verification email", email_val, status or "NONE")
            ses.verify_email_identity(EmailAddress=email_val)
            logger.info("Verification email sent to: %s", email_val)
        else:
            logger.info("Email %s already verified, skipping verification email", email_val)

        # Save subscription in DynamoDB
        dynamodb = session.resource("dynamodb", region_name="ap-south-1")
        table = dynamodb.Table("StockSubscriptions")

        table.put_item(
            Item={
                "Option": option_val,
                "Email": email_val,
                "Risk": risk_val,
                "Expiry": expiry_val,
            }
        )

        logger.info("Subscription saved: %s %s (risk=%s, expiry=%s)", email_val, option_val, risk_val, expiry_val)

        # Different response message based on verification status
        if status == "Success":
            message = f"Successfully subscribed {email_val} to {option_val} ({expiry_val}). You will receive alerts when risk exceeds {risk_val}."
        else:
            message = f"Subscription request received. Please check {email_val} and click the verification link to activate alerts for {option_val} ({expiry_val})."

        return {
            "status": "ok",
            "verified": status == "Success",
            "message": message
        }

    except ClientError as e:
        logger.error("AWS error: %s", e.response["Error"]["Message"])
        raise HTTPException(status_code=500, detail="Failed to save subscription")