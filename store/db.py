from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Database:
    conn: sqlite3.Connection
    lock: threading.Lock


_MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version INTEGER NOT NULL PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS sources (
          source_id TEXT NOT NULL PRIMARY KEY,
          name TEXT NOT NULL,
          source_type TEXT NOT NULL,
          url TEXT NOT NULL,
          poll_interval_seconds INTEGER NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,

          etag TEXT NULL,
          last_modified TEXT NULL,

          next_fetch_at TEXT NULL,
          last_fetch_at TEXT NULL,
          last_success_at TEXT NULL,
          last_error_at TEXT NULL,
          consecutive_failures INTEGER NOT NULL DEFAULT 0,
          last_status_code INTEGER NULL,
          last_fetch_ms INTEGER NULL,
          last_error TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS items (
          item_id TEXT NOT NULL PRIMARY KEY,
          source_id TEXT NOT NULL,
          source_type TEXT NOT NULL,
          external_id TEXT NULL,
          url TEXT NOT NULL,
          title TEXT NOT NULL,
          summary TEXT NOT NULL DEFAULT '',
          content TEXT NULL,
          published_at TEXT NOT NULL,
          updated_at TEXT NULL,
          fetched_at TEXT NOT NULL,
          category TEXT NOT NULL,
          tags TEXT NOT NULL DEFAULT '[]',

          geom_geojson TEXT NULL,
          lat REAL NULL,
          lon REAL NULL,
          location_name TEXT NULL,
          location_confidence TEXT NOT NULL,
          location_rationale TEXT NOT NULL,

          raw TEXT NOT NULL,
          hash_title TEXT NOT NULL,
          hash_content TEXT NOT NULL,
          simhash INTEGER NOT NULL,

          FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS items_url_uq ON items(url);
        CREATE UNIQUE INDEX IF NOT EXISTS items_source_external_uq ON items(source_id, external_id);

        CREATE INDEX IF NOT EXISTS items_published_at_idx ON items(published_at);
        CREATE INDEX IF NOT EXISTS items_category_idx ON items(category);
        CREATE INDEX IF NOT EXISTS items_source_id_idx ON items(source_id);
        CREATE INDEX IF NOT EXISTS items_hash_title_idx ON items(hash_title);

        CREATE TABLE IF NOT EXISTS incidents (
          incident_id TEXT NOT NULL PRIMARY KEY,
          title TEXT NOT NULL,
          summary TEXT NOT NULL DEFAULT '',
          category TEXT NOT NULL,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          last_item_at TEXT NOT NULL,
          status TEXT NOT NULL,
          severity_score INTEGER NOT NULL,

          geom_geojson TEXT NULL,
          lat REAL NULL,
          lon REAL NULL,
          bbox TEXT NULL,
          location_confidence TEXT NOT NULL,
          location_rationale TEXT NOT NULL,

          incident_simhash INTEGER NOT NULL,
          token_signature TEXT NULL,

          item_count INTEGER NOT NULL,
          source_count INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS incidents_last_seen_at_idx ON incidents(last_seen_at);
        CREATE INDEX IF NOT EXISTS incidents_category_idx ON incidents(category);

        CREATE TABLE IF NOT EXISTS incident_items (
          incident_id TEXT NOT NULL,
          item_id TEXT NOT NULL,
          PRIMARY KEY (incident_id, item_id),
          FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE,
          FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS saved_views (
          view_id TEXT NOT NULL PRIMARY KEY,
          name TEXT NOT NULL,
          config_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS places (
          place_id INTEGER NOT NULL PRIMARY KEY,
          name TEXT NOT NULL,
          normalized_name TEXT NOT NULL,
          kind TEXT NOT NULL,
          country_code TEXT NULL,
          admin1 TEXT NULL,
          lat REAL NULL,
          lon REAL NULL,
          importance REAL NULL
        );

        CREATE INDEX IF NOT EXISTS places_normalized_name_idx ON places(normalized_name);

        CREATE VIRTUAL TABLE IF NOT EXISTS items_fts
          USING fts5(title, summary, content, content='items', content_rowid='rowid');

        CREATE TRIGGER IF NOT EXISTS items_fts_ai AFTER INSERT ON items BEGIN
          INSERT INTO items_fts(rowid, title, summary, content)
          VALUES (new.rowid, new.title, new.summary, new.content);
        END;
        CREATE TRIGGER IF NOT EXISTS items_fts_ad AFTER DELETE ON items BEGIN
          INSERT INTO items_fts(items_fts, rowid, title, summary, content)
          VALUES('delete', old.rowid, old.title, old.summary, old.content);
        END;
        CREATE TRIGGER IF NOT EXISTS items_fts_au AFTER UPDATE ON items BEGIN
          INSERT INTO items_fts(items_fts, rowid, title, summary, content)
          VALUES('delete', old.rowid, old.title, old.summary, old.content);
          INSERT INTO items_fts(rowid, title, summary, content)
          VALUES (new.rowid, new.title, new.summary, new.content);
        END;

        CREATE VIRTUAL TABLE IF NOT EXISTS incidents_fts
          USING fts5(title, summary, content='incidents', content_rowid='rowid');

        CREATE TRIGGER IF NOT EXISTS incidents_fts_ai AFTER INSERT ON incidents BEGIN
          INSERT INTO incidents_fts(rowid, title, summary)
          VALUES (new.rowid, new.title, new.summary);
        END;
        CREATE TRIGGER IF NOT EXISTS incidents_fts_ad AFTER DELETE ON incidents BEGIN
          INSERT INTO incidents_fts(incidents_fts, rowid, title, summary)
          VALUES('delete', old.rowid, old.title, old.summary);
        END;
        CREATE TRIGGER IF NOT EXISTS incidents_fts_au AFTER UPDATE ON incidents BEGIN
          INSERT INTO incidents_fts(incidents_fts, rowid, title, summary)
          VALUES('delete', old.rowid, old.title, old.summary);
          INSERT INTO incidents_fts(rowid, title, summary)
          VALUES (new.rowid, new.title, new.summary);
        END;
        """,
    ),
    (
        2,
        """
        DELETE FROM places
        WHERE place_id NOT IN (
          SELECT MIN(place_id) FROM places GROUP BY kind, normalized_name
        );

        CREATE UNIQUE INDEX IF NOT EXISTS places_kind_normalized_uq
          ON places(kind, normalized_name);
        """,
    ),
    (
        3,
        """
        CREATE TABLE IF NOT EXISTS app_config (
          key TEXT NOT NULL PRIMARY KEY,
          value TEXT NOT NULL
        );
        """,
    ),
]


def open_database(path: Path) -> Database:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    _apply_migrations(conn)
    return Database(conn=conn, lock=threading.Lock())


def _apply_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL PRIMARY KEY);"
    )
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS v FROM schema_migrations;"
    ).fetchone()
    current_version = int(row["v"])

    for version, sql in _MIGRATIONS:
        if version <= current_version:
            continue
        conn.executescript(sql)
        conn.execute("INSERT INTO schema_migrations(version) VALUES (?);", (version,))
        conn.commit()
