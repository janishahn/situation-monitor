from pathlib import Path

from ingest.parsers.geojson import parse_geojson
from ingest.parsers.rss import parse_rss
from ingest.parsers.xml import parse_xml_feed


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_usgs_geojson_fixture() -> None:
    data = (FIXTURES / "usgs.geojson").read_bytes()
    features = parse_geojson(data)
    assert len(features) == 1
    assert features[0]["type"] == "Feature"


def test_parse_nws_geojson_fixture() -> None:
    data = (FIXTURES / "nws_alerts.geojson").read_bytes()
    features = parse_geojson(data)
    assert len(features) == 1
    assert features[0]["properties"]["event"] == "Tornado Warning"


def test_parse_rss_fixture() -> None:
    data = (FIXTURES / "sample.rss.xml").read_bytes()
    entries = parse_rss(data)
    assert len(entries) == 1
    assert entries[0]["title"] == "Example headline"


def test_parse_nhc_xml_fixture() -> None:
    data = (FIXTURES / "nhc.xml").read_bytes()
    items = parse_xml_feed(data)
    assert len(items) == 1
    assert items[0]["georss"]["type"] == "Point"
