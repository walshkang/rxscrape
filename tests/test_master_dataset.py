import io

import pandas as pd

from build_master_dataset import apply_baselines


def test_apply_baselines_spreads():
    baselines = {
        "Atorvastatin": {
            "ndc_description": "X",
            "quantity_units": 30,
            "median_per_unit": 0.1,
            "cms_baseline_total": 3.0,
        }
    }
    buf = """Drug_Name,Zip_Code,Retail_Price,GoodRx_Price
Atorvastatin,10001,12.5,8.0
"""
    df = pd.read_csv(io.StringIO(buf))
    out = apply_baselines(df, baselines)
    assert out["CMS_Baseline_Cost"].iloc[0] == 3.0
    assert out["Retail_Spread"].iloc[0] == 12.5 - 3.0
    assert out["GoodRx_Spread"].iloc[0] == 8.0 - 3.0
    assert out["NADAC_Quantity_Units"].iloc[0] == 30
