import pytest
from scraper import extract_price_with_regex

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
