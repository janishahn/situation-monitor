from __future__ import annotations

import re

import httpx


_MAX_AGE_RE = re.compile(r"max-age=(\d+)")


def cache_control_max_age_seconds(cache_control: str | None) -> int | None:
    if cache_control is None:
        return None
    match = _MAX_AGE_RE.search(cache_control)
    if match is None:
        return None
    return int(match.group(1))


async def fetch(
    client: httpx.AsyncClient,
    *,
    url: str,
    user_agent: str,
    etag: str | None,
    last_modified: str | None,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, bytes | None, dict[str, str], int]:
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json, application/xml, application/rss+xml, text/xml, */*",
    }
    if etag is not None:
        headers["If-None-Match"] = etag
    if last_modified is not None:
        headers["If-Modified-Since"] = last_modified
    if extra_headers:
        headers.update(extra_headers)

    timeout = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
    response = await client.get(url, headers=headers, timeout=timeout)
    elapsed_ms = int(response.elapsed.total_seconds() * 1000)
    return (
        response.status_code,
        (response.content if response.status_code == 200 else None),
        dict(response.headers),
        elapsed_ms,
    )
