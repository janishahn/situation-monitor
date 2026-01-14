from __future__ import annotations

import argparse
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from app.settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    settings = Settings()
    db_path = args.db or settings.db_path
    out_path = args.out or (
        db_path.parent
        / f"situation-monitor-backup-{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}.db"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(out_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    print(out_path)


if __name__ == "__main__":
    main()
