import pandas as pd
import numpy as np

REQUIRED_PRODUCT_COLS = ['name_ar','name_en','branch_name','barcodes','brand','available_quantity']

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    newcols = {c: str(c).strip().lower() for c in df.columns}
    df.rename(columns=newcols, inplace=True)
    return df

def validate_product_columns(df: pd.DataFrame):
    cols = [c.lower().strip() for c in df.columns]
    missing = [c for c in REQUIRED_PRODUCT_COLS if c not in cols]
    if missing:
        raise ValueError(f"Products file is missing required columns: {missing}")

def coerce_quantity(df: pd.DataFrame):
    if 'available_quantity' in df.columns:
        df['available_quantity'] = pd.to_numeric(df['available_quantity'], errors='coerce').fillna(0).astype(int)
    return df

