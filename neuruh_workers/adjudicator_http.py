"""Loopback HTTP adapter to the independent Sentinel court organ.

Workers never imports the organ.  The process boundary IS the independence: the
court runs in its own crash domain, judged by code this repository does not
build, and reached only over loopback.  An in-process import would make the
builder the judge inside one interpreter; that was ruled out deliberately.

What crosses the boundary is the ``court.requested`` envelope, verbatim, and
what comes back is the organ's sealed record carrying its own
``switchboard_ingest`` projection.  This module adds no field, chooses no
verdict, and retries nothing.  When the organ cannot be reached it raises
``AdjudicatorUnavailable`` -- a named, visible blocked state -- because a court
that did not happen must never be scored as a court that failed.
"""
from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.request
from typing import Any, Mapping
from urllib.parse import urlparse, urlunparse

from .switchboard import AdjudicatorUnavailable

#: The organ's own default bind (sentinel_court/service.py ``serve_forever``).
DEFAULT_COURT_URL = "http://127.0.0.1:18731"
#: The organ exposes POST /adjudicate (alias /court) and GET /health.
ADJUDICATE_PATH = "/adjudicate"
HEALTH_PATH = "/health"
COURT_URL_ENV = "NEURUH_SENTINEL_COURT_URL"
COURT_TIMEOUT_ENV = "NEURUH_SENTINEL_COURT_TIMEOUT"
#: One attempt, bounded. The board owns retry policy; a client that retried on
#: its own would hide the blocked state the board has to be able to see.
DEFAULT_TIMEOUT_SECONDS = 30.0
#: Mirrors the organ's own refusal (sentinel_court/service.py assert_loopback_bind)
#: and the board's bind_host contract. A court reached over a public network is
#: not the court this seam was authorised to consult.
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_MAX_BODY_BYTES = 1_048_576


def court_url() -> str:
    """The organ's base URL: env first, else the organ's own default bind."""
    return (os.environ.get(COURT_URL_ENV) or DEFAULT_COURT_URL).rstrip("/")


def court_timeout() -> float:
    raw = os.environ.get(COURT_TIMEOUT_ENV)
    if not raw:
        return DEFAULT_TIMEOUT_SECONDS
    try:
        timeout = float(raw)
    except ValueError as exc:
        raise AdjudicatorUnavailable(
            f"{COURT_TIMEOUT_ENV}={raw!r} is not a number") from exc
    if timeout <= 0:
        raise AdjudicatorUnavailable(
            f"{COURT_TIMEOUT_ENV}={raw!r} must be greater than zero")
    return timeout


def _require_loopback(base_url: str) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        raise AdjudicatorUnavailable(
            f"sentinel court URL must be http(s), got {base_url!r}")
    host = (parsed.hostname or "").strip("[]")
    if host not in LOOPBACK_HOSTS:
        raise AdjudicatorUnavailable(
            f"sentinel court URL must be loopback, got {host!r}")
    return base_url


def _endpoint(base_url: str, path: str) -> str:
    parsed = urlparse(base_url)
    return urlunparse(parsed._replace(path=path, params="", query="", fragment=""))


class SentinelCourtHTTPAdjudicator:
    """Carry one ``court.requested`` envelope to the organ and return its record.

    Callable with the signature the board injects
    (``Callable[[Mapping], Mapping]``), so it drops into ``Switchboard(
    adjudicator=...)`` exactly where an in-process organ would have gone.
    """

    def __init__(self, base_url: str | None = None, *, timeout: float | None = None) -> None:
        self.base_url = _require_loopback((base_url or court_url()).rstrip("/"))
        self.timeout = timeout if timeout is not None else court_timeout()

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"SentinelCourtHTTPAdjudicator({self.base_url!r}, timeout={self.timeout})"

    def health(self) -> Mapping[str, Any]:
        """Liveness for operators. Raises AdjudicatorUnavailable when absent."""
        return self._request(_endpoint(self.base_url, HEALTH_PATH), body=None)

    def __call__(self, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        body = json.dumps(envelope).encode("utf-8")
        if len(body) > _MAX_BODY_BYTES:
            # The organ refuses a body over 1 MiB outright; say so here rather
            # than spend a round trip to be told.
            raise AdjudicatorUnavailable(
                f"court.requested packet is {len(body)} bytes; the organ accepts "
                f"at most {_MAX_BODY_BYTES}")
        record = self._request(_endpoint(self.base_url, ADJUDICATE_PATH), body=body)
        if not isinstance(record, Mapping):
            raise AdjudicatorUnavailable(
                "sentinel court returned a body that is not a JSON object")
        return record

    # -- transport ---------------------------------------------------------- #

    def _request(self, url: str, *, body: bytes | None) -> Mapping[str, Any]:
        request = urllib.request.Request(url, data=body, method="POST" if body else "GET")
        request.add_header("Accept", "application/json")
        if body is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read(_MAX_BODY_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raise self._from_http_error(url, exc) from exc
        except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
            # Unreachable, refused, DNS, or timed out. No verdict exists.
            raise AdjudicatorUnavailable(
                f"sentinel court at {url} is unreachable: "
                f"{type(exc).__name__}: {exc}") from exc
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdjudicatorUnavailable(
                f"sentinel court at {url} returned a non-JSON body") from exc

    def _from_http_error(self, url: str, exc: urllib.error.HTTPError) -> AdjudicatorUnavailable:
        """A non-2xx is never a verdict. Name what the organ said, then block.

        A 4xx carries the organ's own refusal code (PACKET_INVALID,
        ORGAN_FAULT, ...). That is a real answer about the request, and it is
        surfaced verbatim -- but it is still not a PASS or a FAIL the candidate
        earned, so it blocks rather than being ingested as an outcome.
        """
        try:
            detail = json.loads(exc.read(_MAX_BODY_BYTES).decode("utf-8"))
        except Exception:  # noqa: BLE001 - the status is the fact that matters
            detail = {}
        code = ""
        if isinstance(detail, Mapping):
            code = str(detail.get("code") or detail.get("error") or "")
        named = f" ({code})" if code else ""
        return AdjudicatorUnavailable(
            f"sentinel court at {url} refused with HTTP {exc.code}{named}; "
            f"no verdict was sealed")


def sentinel_court_adjudicator(base_url: str | None = None, *,
                               timeout: float | None = None) -> SentinelCourtHTTPAdjudicator:
    """Production adjudicator for the composition roots.

    Mirrors ``observe_canonical_base_via_git``: a module-level production
    default the roots inject, with no organ import anywhere in workers.
    """
    return SentinelCourtHTTPAdjudicator(base_url, timeout=timeout)
