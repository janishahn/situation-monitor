from __future__ import annotations

from pathlib import Path

import httpx


URL = "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/airports.csv"


def main() -> None:
    dest = Path(__file__).resolve().parents[1] / "geo" / "data" / "airports.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        res = client.get(URL, headers={"User-Agent": "situation-monitor/0.1"})
        res.raise_for_status()
        dest.write_bytes(res.content)


if __name__ == "__main__":
    main()
