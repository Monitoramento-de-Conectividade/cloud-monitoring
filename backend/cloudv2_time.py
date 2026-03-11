from datetime import datetime, timedelta, timezone


DASHBOARD_FIXED_TZ = timezone(timedelta(hours=-3), name="GMT-3")


def ts_to_dashboard_str(ts):
    if ts is None:
        return "-"
    try:
        parsed = float(ts)
    except (TypeError, ValueError):
        return "-"
    try:
        localized = datetime.fromtimestamp(parsed, tz=timezone.utc).astimezone(DASHBOARD_FIXED_TZ)
    except (OverflowError, OSError, ValueError):
        return "-"
    return localized.strftime("%Y-%m-%d %H:%M:%S")
