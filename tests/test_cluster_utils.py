from cluster.clusterer import (
    canonicalize_url,
    hamming_distance,
    normalize_title,
    simhash64,
)


def test_normalize_title_basic() -> None:
    assert normalize_title("  Hello,   World!! ") == "hello world"


def test_canonicalize_url_strips_tracking() -> None:
    url = "https://Example.com/path?a=1&utm_source=x&fbclid=y#frag"
    assert canonicalize_url(url) == "https://example.com/path?a=1"


def test_simhash_distance_sanity() -> None:
    a = simhash64("earthquake near tokyo")
    b = simhash64("earthquake near tokyo japan")
    c = simhash64("sports results premier league")
    assert hamming_distance(a, b) <= 12
    assert hamming_distance(a, c) > 12
