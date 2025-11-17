import pandas as pd

REQUIRED_COLS = [
    "name_ar",
    "name_en",
    "branch_name",
    "barcodes",
    "brand",
    "available_quantity"
]

def normalize_columns(df):
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def validate_product_columns(df):
    df_cols = list(df.columns)
    missing = [col for col in REQUIRED_COLS if col not in df_cols]
    if missing:
        raise ValueError(f"Products file is missing columns: {missing}")

def coerce_quantity(df):
    if "available_quantity" in df.columns:
        df["available_quantity"] = (
            pd.to_numeric(df["available_quantity"], errors="coerce").fillna(0).astype(int)
        )
    return df
