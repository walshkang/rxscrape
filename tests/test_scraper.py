import pytest
from scraper_utils import parse_price

def test_parse_price_clean():
    assert parse_price("$10.50") == 10.50

def test_parse_price_garbage():
    assert parse_price("$102.10 > Special offers") == 102.10

def test_parse_price_comma():
    assert parse_price("$1,234.56") == 1234.56

def test_parse_price_invalid():
    with pytest.raises(ValueError):
        parse_price("No price here")
