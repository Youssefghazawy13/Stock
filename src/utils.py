import pandas as pd
from typing import Optional

# Required product columns (category is computed, not required in upload)
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
    # Ensure numeric and compute difference
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
    Extract category from name_en using these heuristics:
    - Split by '-' into tokens.
    - If len(tokens) >= 5: category = tokens[-3]
      (covers brand-product-color-category-gender-size and brand-product-category-gender-size)
    - If len(tokens) == 4: category = tokens[-2]
      (covers brand-product-category-size)
    - Else: return empty string.
    """
    if not isinstance(name_en, str):
        return ""

    tokens = [t.strip() for t in name_en.split("-") if t.strip()]
    try:
        if len(tokens) >= 5:
            # e.g. [..., color?, category, gender, size] -> take -3
            return tokens[-3]
        if len(tokens) == 4:
            # e.g. [brand, product, category, size] -> take -2
            return tokens[-2]
    except Exception:
        pass
    return ""

def ensure_category_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a 'category' column exists. If present, keep it; otherwise compute from name_en.
    Position: insert category column after name_en (i.e., between name_en and branch_name).
    """
    df = df.copy()
    df = normalize_columns(df)

    # If name_en not present, we cannot compute; just add empty column
    if "name_en" not in df.columns:
        df["category"] = ""
    else:
        if "category" not in df.columns:
            # compute category
            df["category"] = df["name_en"].apply(lambda x: extract_category_from_name(x))
        else:
            # ensure column exists and fill empties by trying to extract
            df["category"] = df["category"].fillna("").astype(str)
            mask = df["category"].str.strip() == ""
            if mask.any():
                df.loc[mask, "category"] = df.loc[mask, "name_en"].apply(lambda x: extract_category_from_name(x))

    # Reorder columns so that category appears right after name_en if possible
    cols = list(df.columns)
    if "name_en" in cols:
        # remove category then insert after name_en
        if "category" in cols:
            cols.remove("category")
        # place category after name_en
        idx = cols.index("name_en")
        cols.insert(idx + 1, "category")
        df = df[cols]
    return df
