from __future__ import annotations

import xml.etree.ElementTree as ET


def parse_faa_airport_status(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    records: list[dict] = []
    for airport in root.findall(".//AirportStatus"):
        status = airport.find("Status")
        delay_text = status.findtext("Delay") if status is not None else None
        delay = delay_text.strip().casefold() == "true" if delay_text else False

        if not delay:
            continue

        records.append(
            {
                "name": airport.findtext("Name") or "",
                "iata": airport.findtext("IATA") or "",
                "icao": airport.findtext("ICAO") or "",
                "city": airport.findtext("City") or "",
                "state": airport.findtext("State") or "",
                "reason": status.findtext("Reason") if status is not None else None,
                "delay": delay,
                "avg_delay": status.findtext("AvgDelay")
                if status is not None
                else None,
                "trend": status.findtext("Trend") if status is not None else None,
                "type": status.findtext("Type") if status is not None else None,
                "program": status.findtext("Program") if status is not None else None,
                "end_time": status.findtext("EndTime") if status is not None else None,
                "update_time": airport.findtext("UpdateTime"),
            }
        )
    return records
