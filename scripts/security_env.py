import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def _first_non_empty(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "")
        if value:
            return value
    return ""


def normalize_secret_aliases() -> None:
    """Normalize secret aliases so jobs can rely on canonical env names.

    Canonical names:
    - ODOO_SECRET (mirrored to ODOO_API_KEY for backward compatibility)
    - DB_PASSWORD (can be sourced from BLISSYDAH_DB_PASSWORD)
    """
    odoo_secret = _first_non_empty("ODOO_SECRET", "ODOO_API_KEY", "ODOO_PASSWORD")
    if odoo_secret:
        os.environ.setdefault("ODOO_SECRET", odoo_secret)
        os.environ.setdefault("ODOO_API_KEY", odoo_secret)

    db_password = _first_non_empty("BLISSYDAH_DB_PASSWORD", "DB_PASSWORD")
    if db_password:
        os.environ.setdefault("BLISSYDAH_DB_PASSWORD", db_password)
        os.environ.setdefault("DB_PASSWORD", db_password)


def load_project_env(project_root: Optional[str] = None) -> Path:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env", override=False)

    db_env = root / "config" / "db.env"
    if db_env.exists():
        load_dotenv(db_env, override=True)

    normalize_secret_aliases()
    return root


def get_odoo_secret(required: bool = True) -> str:
    normalize_secret_aliases()
    secret = _first_non_empty("ODOO_SECRET", "ODOO_API_KEY", "ODOO_PASSWORD")
    if required and not secret:
        raise RuntimeError("Missing ODOO secret. Set ODOO_SECRET (preferred) or ODOO_API_KEY in .env")
    return secret


def get_db_password(required: bool = True) -> str:
    normalize_secret_aliases()
    password = _first_non_empty("BLISSYDAH_DB_PASSWORD", "DB_PASSWORD")
    if required and not password:
        raise RuntimeError("Missing DB password. Set BLISSYDAH_DB_PASSWORD (preferred) or DB_PASSWORD in config/db.env")
    return password
