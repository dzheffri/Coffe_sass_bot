        except Exception:
            reason = response.text

    if response.status_code == 200:
        return {
            "ok": True,
            "status": 200,
            "reason": None,
        }

    if reason in {
        "BadDeviceToken",
        "DeviceTokenNotForTopic",
        "Unregistered",
    }:
        try:
            remove_invalid_app_push_token(
                clean_token
            )
        except Exception as exc:
            print(
                "APP PUSH TOKEN CLEANUP ERROR:",
                repr(exc),
            )

    return {
        "ok": False,
        "status": response.status_code,
        "reason": reason,
    }


async def send_app_pushes(
    devices: list,
    title: str,
    body: str,
    data: dict | None = None,
    badge: int | None = None,
):
    sent = 0
    failed = 0
    errors = []

    for device in devices:
        if isinstance(
            device,
            dict,
        ):
            device_token = (
                device.get(
                    "device_token"
                )
            )

            environment = (
                device.get(
                    "environment"
                )
                or "production"
            )
        else:
            device_token = str(
                device
            )

            environment = "production"

        try:
            result = await send_app_push(
                device_token=device_token,
                title=title,
                body=body,
                environment=environment,
                data=data,
                badge=badge,
            )

            if result.get("ok"):
                sent += 1

            else:
                failed += 1

                errors.append({
                    "device_token": (
                        f"{str(device_token)[:8]}..."
                        if device_token
                        else ""
                    ),
                    "status":
                        result.get("status"),
                    "reason":
                        result.get("reason"),
                })

        except Exception as exc:
            failed += 1

            errors.append({
                "device_token": (
                    f"{str(device_token)[:8]}..."
                    if device_token
                    else ""
                ),
                "status": 0,
                "reason": repr(exc),
            })

    return {
        "sent": sent,
        "failed": failed,
        "errors": errors,
    }
