import pandas as pd
from typing import Optional

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

def extract_category_from_name(name_en: str) -> str:
    """
    New exact rules:
      - if tokens == 6 -> take token 4 (index 3)
      - if tokens == 5 -> take token 3 (index 2)
      - if tokens == 4 -> take token 3 (index 2)
      - else -> empty
    """
    if not isinstance(name_en, str):
        return ""
    tokens = [t.strip() for t in name_en.split("-") if t.strip()]
    n = len(tokens)
    try:
        if n == 6:
            return tokens[3]
        if n == 5:
            return tokens[2]
        if n == 4:
            return tokens[2]
    except Exception:
        pass
    return ""

def ensure_category_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = normalize_columns(df)
    if "name_en" not in df.columns:
        df["category"] = ""
    else:
        if "category" not in df.columns:
            df["category"] = df["name_en"].apply(lambda x: extract_category_from_name(x))
        else:
            df["category"] = df["category"].fillna("").astype(str)
            mask = df["category"].str.strip() == ""
            if mask.any():
                df.loc[mask, "category"] = df.loc[mask, "name_en"].apply(lambda x: extract_category_from_name(x))

    # reorder to place category after name_en
    cols = list(df.columns)
    if "name_en" in cols:
        if "category" in cols:
            cols.remove("category")
        idx = cols.index("name_en")
        cols.insert(idx + 1, "category")
        ordered = [c for c in cols if c in df.columns]
        # append any remaining columns
        for c in df.columns:
            if c not in ordered:
                ordered.append(c)
        df = df[ordered]
    return df
