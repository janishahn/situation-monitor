from geo.gazetteer import match_country_in_text, suggest_places
from store.db import open_database


def test_gazetteer_ambiguity_georgia(tmp_path) -> None:
    db = open_database(tmp_path / "test.db")
    try:
        results = suggest_places(db, "Georgia", limit=10)
        names = {r["name"] for r in results}
        assert "Georgia" in names
        assert any(r["country_code"] == "GE" for r in results)
        assert any(r["country_code"] == "US" for r in results)
    finally:
        with db.lock:
            db.conn.close()


def test_match_country_in_text_word_boundaries() -> None:
    countries = [
        ("Oman", "oman", 21.0, 57.0),
        ("Japan", "japan", 36.0, 138.0),
        ("United States", "united states", 39.0, -98.0),
    ]
    assert match_country_in_text(countries, "A woman was rescued.") is None
    assert match_country_in_text(countries, "Earthquake in Japan") == (
        "Japan",
        36.0,
        138.0,
    )
    assert match_country_in_text(countries, "Flooding in the United States") == (
        "United States",
        39.0,
        -98.0,
    )
