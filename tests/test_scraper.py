import pytest
from scraper import extract_price_with_regex, extract_retail_from_blurb

def test_extract_price_with_regex_clean():
    assert extract_price_with_regex("$10.50") == 10.50

def test_extract_price_with_regex_garbage():
    assert extract_price_with_regex("$102.10 > Special offers") == 102.10

def test_extract_price_with_regex_comma():
    assert extract_price_with_regex("$1,234.56") == 1234.56

def test_extract_price_with_regex_invalid():
    assert extract_price_with_regex("No price here") is None

def test_extract_price_with_regex_empty():
    assert extract_price_with_regex("") is None


def test_extract_retail_labeled_proximity():
    assert extract_retail_from_blurb("Retail price $93") == 93.0
    assert extract_retail_from_blurb("Retail\n$10.5") == 10.5
    assert extract_retail_from_blurb("Est. retail: $1,200.00") == 1200.0
    assert extract_retail_from_blurb("List price $2.9") == 2.9


def test_extract_retail_was():
    assert extract_retail_from_blurb("List\nwas $45") == 45.0
    assert extract_retail_from_blurb("Was: $3.00") == 3.0


def test_extract_retail_skips_retail_savings_upsell():
    # "retail" in the phrase "retail savings" must not anchor the proximity regex
    assert extract_retail_from_blurb("Enjoy retail savings. Another line.\n$50 cash") is None
