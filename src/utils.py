import pandas as pd

# Required columns changed: remove name_ar, add actual_quantity
REQUIRED_COLS = [
    "name_en",
    "branch_name",
    "barcodes",
    "brand",
    "available_quantity",
    "actual_quantity"
]

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def validate_product_columns(df: pd.DataFrame):
    cols = [c.lower().strip() for c in df.columns]
    missing = [c for c in REQUIRED_COLS if c not in cols]
    if missing:
        raise ValueError(f"Products file is missing required columns: {missing}")

def coerce_quantities(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure numeric quantities and create the difference column
    if "available_quantity" in df.columns:
        df["available_quantity"] = pd.to_numeric(df["available_quantity"], errors="coerce").fillna(0)
    else:
        df["available_quantity"] = 0

    if "actual_quantity" in df.columns:
        df["actual_quantity"] = pd.to_numeric(df["actual_quantity"], errors="coerce").fillna(0)
    else:
        df["actual_quantity"] = 0

    df["difference"] = df["actual_quantity"] - df["available_quantity"]
    return df
