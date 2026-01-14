from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from app.settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("backup", type=Path)
    parser.add_argument("--db", type=Path, default=None)
    args = parser.parse_args()

    settings = Settings()
    db_path = args.db or settings.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    src = sqlite3.connect(args.backup)
    dst = sqlite3.connect(db_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    print(db_path)


if __name__ == "__main__":
    main()
