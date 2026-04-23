import re

def parse_price(price_str: str) -> float:
    """
    Extracts the numerical dollar amount from a string like '$102.10 > Special offers'.
    """
    if not price_str:
        raise ValueError("Empty price string")
        
    # Regex to match dollar sign, commas, and decimals
    # Pattern: Optional $, then digits, commas, then decimal point and digits
    match = re.search(r"\$?([0-9,]+\.[0-9]{2})", price_str)
    
    if not match:
        raise ValueError(f"Could not parse price from string: {price_str}")
        
    # Remove commas and convert to float
    price_val = match.group(1).replace(",", "")
    return float(price_val)
