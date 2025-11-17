from pathlib import Path
import pandas as pd
from src.utils import normalize_columns, validate_product_columns, coerce_quantity
from typing import Iterator, List
import xlsxwriter
import tempfile
import zipfile
import math

def _prepare_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    # keep only required columns if present
    needed = ['name_ar','name_en','branch_name','barcodes','brand','available_quantity']
    present = [c for c in needed if c in df.columns]
    return df[present]

def _truncate_sheet_name(name: str) -> str:
    if not isinstance(name, str):
        name = str(name)
    return name[:31]

def create_excel_for_branch_date(branch: str, date_obj, grouped_products: dict, out_path: Path):
    """
    grouped_products: dict[brand] -> DataFrame
    out_path: path to write excel (including filename)
    """
    # use xlsxwriter via pandas ExcelWriter
    with pd.ExcelWriter(out_path, engine='xlsxwriter', options={'strings_to_numbers': False}) as writer:
        for brand, df in grouped_products.items():
            if df is None or df.empty:
                # skip empty
                continue
            sheet_name = _truncate_sheet_name(brand)
            # ensure columns order
            cols = ['name_ar','name_en','barcodes','available_quantity','brand','branch_name']
            present = [c for c in cols if c in df.columns]
            df[present].to_excel(writer, sheet_name=sheet_name, index=False)

def create_error_excel(branch: str, date_obj, message: str, out_path: Path):
    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        pd.DataFrame([{'error': message}]).to_excel(writer, sheet_name='ERROR', index=False)

def create_zip_from_paths(paths: List[Path], zip_path: Path):
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for p in paths:
            zf.write(p, arcname=p.name)
    return zip_path

def generate_branch_date_files(products_iter: Iterator[pd.DataFrame], schedule_df: pd.DataFrame, out_dir: Path) -> List[Path]:
    """
    products_iter: iterator yielding DataFrames (chunks) with product data (strings)
    schedule_df: DataFrame with columns branch, date (datetime.date), brand
    Returns: list of generated file paths
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build schedule mapping: (branch_lower, date) -> set(brands)
    schedule_df = schedule_df.copy()
    schedule_df['branch_norm'] = schedule_df['branch'].astype(str).str.strip().str.lower()
    schedule_df['brand_norm'] = schedule_df['brand'].astype(str).str.strip().str.lower()
    schedule_df['date_str'] = schedule_df['date'].apply(lambda d: d.strftime("%d-%m-%Y"))
    mapping = {}  # (branch_norm, date_str) -> set(brands original)
    for _, r in schedule_df.iterrows():
        key = (r['branch_norm'], r['date_str'])
        mapping.setdefault(key, set()).add(r['brand'].strip())

    if not mapping:
        return []

    # Prepare temporary storage: for each mapping key and brand we will accumulate rows
    # structure: accum[(branch_norm, date_str)][brand] = list of dict rows
    accum = {}
    # Track which branches exist in products
    branches_in_products = set()

    # Iterate over product chunks
    for chunk in products_iter:
        chunk = _prepare_chunk(chunk)
        if chunk.empty:
            continue
        # normalize textual columns
        cols = [c for c in chunk.columns]
        if 'branch_name' in chunk.columns:
            chunk['branch_norm'] = chunk['branch_name'].astype(str).str.strip().str.lower()
            branches_in_products.update(chunk['branch_norm'].unique())
        else:
            chunk['branch_norm'] = ''

        if 'brand' in chunk.columns:
            chunk['brand_norm'] = chunk['brand'].astype(str).str.strip().str.lower()
        else:
            chunk['brand_norm'] = ''

        # coerce quantities
        if 'available_quantity' in chunk.columns:
            chunk['available_quantity'] = pd.to_numeric(chunk['available_quantity'], errors='coerce').fillna(0)

        # for each schedule key, filter chunk by branch and brand set
        for (branch_norm, date_str), brand_set in mapping.items():
            # if this branch not in this chunk, skip
            if branch_norm not in chunk['branch_norm'].values:
                continue
            # filter rows for branch
            branch_rows = chunk[chunk['branch_norm'] == branch_norm]
            # for each brand in brand_set, select rows where brand_norm matches
            for brand in brand_set:
                brand_norm = str(brand).strip().lower()
                matched = branch_rows[branch_rows['brand_norm'] == brand_norm]
                if matched.empty:
                    continue
                key = (branch_norm, date_str)
                accum.setdefault(key, {})
                accum[key].setdefault(brand, [])
                # convert matched rows to records preserving original columns
                records = matched.to_dict(orient='records')
                accum[key][brand].extend(records)

    # Now produce Excel files for each mapping key
    generated_paths = []
    for (branch_norm, date_str), brand_dict in mapping.items():
        # determine original branch name for file naming — try to get one from schedule (preserve original casing)
        # find a schedule row for that branch/date to get original branch casing
        rows = schedule_df[(schedule_df['branch_norm'] == branch_norm) & (schedule_df['date'].apply(lambda d: d.strftime("%d-%m-%Y")) == date_str)]
        if not rows.empty:
            original_branch = rows.iloc[0]['branch']
        else:
            original_branch = branch_norm

        filename = f"{original_branch}_{date_str}.xlsx"
        out_path = out_dir / filename

        key = (branch_norm, date_str)
        if key not in accum:
            # branch exists? if not, create an ERROR file
            if branch_norm not in branches_in_products:
                create_error_excel(original_branch, date_str, f"No products found for branch '{original_branch}' in uploaded products file.", out_path)
                generated_paths.append(out_path)
            else:
                # no matched brands -> create a small empty workbook or skip
                create_error_excel(original_branch, date_str, f"No matching products found for branch '{original_branch}' on {date_str}.", out_path)
                generated_paths.append(out_path)
            continue

        grouped_products = {}
        for brand, rows in accum[key].items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            # normalize columns & coerce
            df.columns = [str(c).strip().lower() for c in df.columns]
            df = coerce_quantity(df)
            grouped_products[brand] = df

        create_excel_for_branch_date(original_branch, date_str, grouped_products, out_path)
        generated_paths.append(out_path)

    return generated_paths

