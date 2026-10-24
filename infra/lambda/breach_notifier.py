import os

import boto3

sns = boto3.client("sns")
TOPIC = os.environ["SNS_TOPIC_ARN"]


def _publish(subject: str, message: str) -> None:
    sns.publish(TopicArn=TOPIC, Subject=subject[:100], Message=message)


def _breached(image: dict) -> bool:
    return image.get("breached", {}).get("BOOL", False)


def handler(event, context):
    if event.get("source") == "aws.events":
        print("scheduled maintenance tick")
        return {"ok": True}

    alerts = 0
    for record in event.get("Records", []):
        image = record.get("dynamodb", {}).get("NewImage", {})
        old_image = record.get("dynamodb", {}).get("OldImage", {})
        status = image.get("status", {}).get("S")
        breached = _breached(image)
        # alert once, when breached flips from false/absent to true (needs stream view NEW_AND_OLD_IMAGES)
        if (breached and not _breached(old_image)) or status == "RETURNED":
            parcel_id = image.get("parcel_id", {}).get("S", "unknown")
            _publish(
                f"outfordelivery alert: {parcel_id}",
                f"Parcel {parcel_id}: status={status}, breached={breached}.",
            )
            alerts += 1
    return {"alerts": alerts}
