"""Security helpers for sensitive local-only configuration.

How to change the database purge password:
1) Copy `cloudv2_local_secrets.example.py` to `cloudv2_local_secrets.py`.
2) Edit `DB_PURGE_PASSWORD` inside `cloudv2_local_secrets.py`.

Important:
- `cloudv2_local_secrets.py` is ignored by git on purpose.
- Do not commit or push the real password.
"""

from importlib import import_module


def get_db_purge_password():
    try:
        module = import_module("cloudv2_local_secrets")
    except Exception:
        return ""

    raw_value = getattr(module, "DB_PURGE_PASSWORD", "")
    return str(raw_value or "").strip()

