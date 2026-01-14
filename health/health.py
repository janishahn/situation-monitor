from __future__ import annotations

from datetime import UTC, datetime, timedelta

from store.db import Database


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def compute_backoff_seconds(
    poll_interval_seconds: int, consecutive_failures: int
) -> int:
    if consecutive_failures <= 0:
        return poll_interval_seconds
    return min(60 * 60, poll_interval_seconds * (2**consecutive_failures))


def record_fetch_success(
    db: Database,
    *,
    source_id: str,
    status_code: int,
    fetch_ms: int,
    etag: str | None,
    last_modified: str | None,
    next_fetch_in_seconds: int,
) -> None:
    now_iso = _utc_now_iso()
    next_iso = (
        (datetime.now(tz=UTC) + timedelta(seconds=next_fetch_in_seconds))
        .isoformat()
        .replace("+00:00", "Z")
    )
    with db.lock:
        db.conn.execute(
            """
            UPDATE sources
            SET last_fetch_at = ?,
                last_success_at = ?,
                last_status_code = ?,
                last_fetch_ms = ?,
                consecutive_failures = 0,
                last_error = NULL,
                last_error_at = NULL,
                success_count = success_count + 1,
                etag = ?,
                last_modified = ?,
                next_fetch_at = ?
            WHERE source_id = ?;
            """,
            (
                now_iso,
                now_iso,
                status_code,
                fetch_ms,
                etag,
                last_modified,
                next_iso,
                source_id,
            ),
        )
        db.conn.commit()


def record_fetch_error(
    db: Database,
    *,
    source_id: str,
    status_code: int | None,
    fetch_ms: int | None,
    error: str,
) -> int:
    now_iso = _utc_now_iso()
    with db.lock:
        row = db.conn.execute(
            "SELECT poll_interval_seconds, consecutive_failures FROM sources WHERE source_id = ?;",
            (source_id,),
        ).fetchone()
        if row is None:
            return 300
        poll_seconds = int(row["poll_interval_seconds"])
        failures = int(row["consecutive_failures"]) + 1
        backoff_seconds = compute_backoff_seconds(poll_seconds, failures)
        next_iso = (
            (datetime.now(tz=UTC) + timedelta(seconds=backoff_seconds))
            .isoformat()
            .replace("+00:00", "Z")
        )

        db.conn.execute(
            """
            UPDATE sources
            SET last_fetch_at = COALESCE(?, last_fetch_at),
                last_error_at = ?,
                last_status_code = COALESCE(?, last_status_code),
                last_fetch_ms = COALESCE(?, last_fetch_ms),
                consecutive_failures = ?,
                last_error = ?,
                error_count = error_count + 1,
                next_fetch_at = ?
            WHERE source_id = ?;
            """,
            (
                now_iso,
                now_iso,
                status_code,
                fetch_ms,
                failures,
                error,
                next_iso,
                source_id,
            ),
        )
        db.conn.commit()
    return backoff_seconds
