"""Redact credentials out of receipts, logs, exceptions, and PR bodies."""
from __future__ import annotations

import json
import os
import re
from typing import Any, Iterable, Mapping, Sequence

_USERINFO = re.compile(r"(://)([^/@:\s]+):([^@\s]+)@")
_SECRET_ENV = (
    "GITHUB_TOKEN", "GH_TOKEN", "GH_ENTERPRISE_TOKEN", "GIT_ASKPASS",
    "SSH_PRIVATE_KEY", "NOTION_TOKEN", "NOTION_API_KEY", "OPENAI_API_KEY",
    "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
)


def env_secrets() -> list[str]:
    found = []
    for name in _SECRET_ENV:
        value = os.environ.get(name, "")
        if isinstance(value, str) and len(value) >= 8:
            found.append(value)
    return found


def redact_text(text: str, secrets: Iterable[str] = ()) -> str:
    if not text:
        return text
    out = _USERINFO.sub(r"\1***:***@", text)
    for secret in secrets:
        if secret and secret in out:
            out = out.replace(secret, "***")
    return out


def identity_key(url_or_identity: str) -> str:
    """Compare remotes without credentials, schemes, or trailing .git."""
    raw = (url_or_identity or "").strip()
    stripped = _USERINFO.sub(r"\1", raw)
    stripped = stripped.replace("://", "://", 1)
    # Drop userinfo leftovers of the form ://@host
    stripped = stripped.replace("://@", "://")
    for prefix in ("https://", "http://", "ssh://", "git://"):
        if stripped.lower().startswith(prefix):
            stripped = stripped[len(prefix):]
            break
    if stripped.lower().startswith("git@"):
        stripped = stripped[4:].replace(":", "/", 1)
    stripped = stripped.rstrip("/")
    if stripped.endswith(".git"):
        stripped = stripped[:-4]
    if os.path.exists(raw) or os.path.exists(stripped):
        existing = raw if os.path.exists(raw) else stripped
        try:
            return os.path.realpath(existing)
        except OSError:
            return stripped
    return stripped


def collect_secrets(*groups: Iterable[str] | None,
                    extra: Sequence[str] | None = None) -> tuple[str, ...]:
    values: list[str] = []
    for group in groups:
        if group:
            values.extend(s for s in group if s)
    if extra:
        values.extend(s for s in extra if s)
    values.extend(env_secrets())
    # Longest first so overlapping secrets redact fully.
    unique = sorted(set(values), key=len, reverse=True)
    return tuple(unique)


def redact_any(value: Any, secrets: Sequence[str]) -> Any:
    if isinstance(value, str):
        return redact_text(value, secrets)
    if isinstance(value, Mapping):
        return {str(k): redact_any(v, secrets) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_any(v, secrets) for v in value]
    if isinstance(value, tuple):
        return [redact_any(v, secrets) for v in value]
    return value


def assert_no_secrets(value: Any, secrets: Sequence[str], *, where: str) -> None:
    blob = value if isinstance(value, str) else json.dumps(value, default=str)
    for secret in secrets:
        if secret and secret in blob:
            from .errors import GitCustodianRefused
            raise GitCustodianRefused(
                f"refusing to emit secret material via {where}",
                {"where": where}, reason="SECRET_LEAK_BLOCKED")
