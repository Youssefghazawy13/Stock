# src/processor.py

from pathlib import Path
import re
import pandas as pd
import zipfile
from typing import Iterable, List, Optional

# Try to import utility helpers from src.utils if available; otherwise provide minimal fallbacks.
try:
    from src.utils import (
        normalize_columns,
        validate_product_columns,
        coerce_quantities,
        ensure_category_column,
    )
except Exception:
    # Minimal fallbacks to keep processor import-safe for tests.
    def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        # strip column names
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    def validate_product_columns(df: pd.DataFrame):
        # Check presence of minimally required columns; raise if missing
        required = {"name_en", "branch_name", "barcodes", "brand", "available_quantity"}
        cols = {str(c).strip().lower() for c in df.columns}
        missing = required - cols
        if missing:
            raise ValueError(f"Missing required product columns: {missing}")

    def coerce_quantities(df: pd.DataFrame) -> pd.DataFrame:
        # Ensure numeric available_quantity; keep actual_quantity as-is
        df = df.copy()
        if "available_quantity" in df.columns:
            df["available_quantity"] = pd.to_numeric(df["available_quantity"], errors="coerce").fillna(0)
        return df

    def ensure_category_column(df: pd.DataFrame) -> pd.DataFrame:
        # If category missing, attempt simple extraction from name_en based on token rules described.
        df = df.copy()
        if "name_en" not in df.columns:
            return df
        if "category" not in df.columns:
            def extract_cat(n):
                try:
                    parts = str(n).split("-")
                    # rules: token counts handled as per spec
                    if len(parts) >= 6:
                        return parts[3].strip()
                    if len(parts) == 5:
                        return parts[2].strip()
                    if len(parts) == 4:
                        return parts[2].strip()
                except Exception:
                    pass
                return ""
            df["category"] = df["name_en"].apply(extract_cat)
        return df


def create_zip_from_paths(paths: List[Path], zip_path: Path):
    """
    Create a ZIP archive containing the given file paths.
    """
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in paths:
            z.write(p, arcname=p.name)


def _truncate_sheet_name(name: str) -> str:
    """
    Truncate sheet name to Excel's 31-character limit.
    """
    if not isinstance(name, str):
        name = str(name)
    return name[:31]


def _col_idx_to_excel_col(idx: int) -> str:
    """
    Convert 0-based column index to Excel column letters (0 -> A, 25 -> Z, 26 -> AA).
    """
    letters = ""
    n = idx + 1
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _normalize_text_for_matching(s: Optional[str]) -> str:
    """
    Normalize text for tolerant matching: lowercase, strip, remove non-alphanumeric.
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^0-9a-z]+", "", s)
    return s


def generate_branch_date_files(products_iter: Iterable[pd.DataFrame], schedule_df: pd.DataFrame, output_dir: Path) -> List[Path]:
    """
    Generate one Excel file per (branch, date). Each generated file contains:
      - A 'Summary' sheet (FIRST sheet) with formulas referencing brand sheets.
      - One sheet per brand with columns:
          name_en, category, branch_name, barcodes, brand, available_quantity, actual_quantity, difference
        where actual_quantity is created blank if missing and difference is an Excel formula
        referencing actual_quantity and available_quantity.
    Inputs:
      - products_iter: an iterator yielding pandas.DataFrame chunks OR a single pandas.DataFrame
      - schedule_df: DataFrame with columns: branch, date (datetime.date), brand
      - output_dir: Path where to write files
    Returns:
      - list of Path to generated Excel files
    """
    import xlsxwriter

    output_dir.mkdir(parents=True, exist_ok=True)

    # --- 1. Consume products_iter safely into a single DataFrame (list of chunks)
    chunks = []
    try:
        # products_iter could be an iterator of DataFrames
        for chunk in products_iter:
            if isinstance(chunk, pd.DataFrame):
                chunks.append(chunk.copy())
    except TypeError:
        # products_iter might itself be a DataFrame
        if isinstance(products_iter, pd.DataFrame):
            chunks = [products_iter.copy()]
        else:
            # not iterable: treat as empty
            chunks = []

    if chunks:
        all_products = pd.concat(chunks, ignore_index=True)
    else:
        all_products = pd.DataFrame()

    # Normalize columns and ensure category
    all_products = normalize_columns(all_products)
    try:
        all_products = ensure_category_column(all_products)
    except Exception:
        pass

    # Coerce quantities
    all_products = coerce_quantities(all_products)

    # Prepare normalized keys for matching
    if "branch_name" in all_products.columns:
        all_products["branch_norm_key"] = all_products["branch_name"].astype(str).str.strip().str.lower()
    else:
        all_products["branch_norm_key"] = ""

    if "brand" in all_products.columns:
        all_products["brand_norm_key"] = all_products["brand"].astype(str).str.strip().str.lower()
    else:
        all_products["brand_norm_key"] = ""

    # Build grouped index: (branch_norm, brand_norm) -> list of records (dicts)
    grouped = {}
    for _, row in all_products.iterrows():
        b = str(row.get("branch_norm_key", "")).strip().lower()
        br = str(row.get("brand_norm_key", "")).strip().lower()
        grouped.setdefault((b, br), []).append(row.to_dict())

    product_branch_keys = {k[0] for k in grouped.keys() if k[0]}

    def find_best_branch_key(schedule_branch_raw: str) -> Optional[str]:
        key = _normalize_text_for_matching(schedule_branch_raw)
        if not key:
            return None
        if key in product_branch_keys:
            return key
        # try fuzzy substring match
        for pb in product_branch_keys:
            if key in pb or pb in key:
                return pb
        return None

    # --- 2. Build schedule_map: (branch_key, date_str) -> set(brands)
    schedule_map = {}
    for _, r in schedule_df.iterrows():
        sched_branch_raw = r.get("branch", "")
        sched_brand = r.get("brand", "")
        sched_date = r.get("date", None)
        if pd.isna(sched_branch_raw) or pd.isna(sched_brand) or pd.isna(sched_date):
            continue
        bkey = find_best_branch_key(sched_branch_raw)
        if bkey is None:
            bkey = str(sched_branch_raw).strip().lower()
        # date_str as dd-mm-YYYY
        try:
            date_str = sched_date.strftime("%d-%m-%Y")
        except Exception:
            date_str = str(sched_date)
        schedule_map.setdefault((bkey, date_str), set()).add(sched_brand)

    if not schedule_map:
        return []

    generated_files: List[Path] = []

    # --- 3. For each (branch_key, date_str) write Excel file
    for (branch_key, date_str), brand_set in schedule_map.items():
        # recover branch display name from products if possible
        sample_rows = all_products[all_products["branch_norm_key"] == branch_key]
        original_branch = sample_rows["branch_name"].iloc[0] if not sample_rows.empty else branch_key
        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsx"
        out_path = output_dir / filename

        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book
            summary_entries = []  # each entry: dict with name_cell, barcode_cell, diff_cell

            # Write brand sheets
            for brand in brand_set:
                brand_norm = str(brand).strip().lower()
                rows = grouped.get((branch_key, brand_norm), [])

                if not rows:
                    # try approximate brand matching within same branch
                    candidate_rows = []
                    for (bk, brk), recs in grouped.items():
                        if bk == branch_key and (brand_norm in brk or brk in brand_norm):
                            candidate_rows.extend(recs)
                    rows = candidate_rows

                if not rows:
                    # no matching products for this brand in this branch => skip
                    continue

                df = pd.DataFrame(rows)
                df = normalize_columns(df)
                try:
                    df = ensure_category_column(df)
                except Exception:
                    pass
                df = coerce_quantities(df)

                # Ensure actual_quantity exists (blank) and drop difference if present
                if "actual_quantity" not in df.columns:
                    df["actual_quantity"] = ""
                else:
                    df["actual_quantity"] = df["actual_quantity"].fillna("")

                if "difference" in df.columns:
                    df = df.drop(columns=["difference"])

                cols_order = [
                    "name_en", "category", "branch_name", "barcodes",
                    "brand", "available_quantity", "actual_quantity"
                ]
                present_cols = [c for c in cols_order if c in df.columns]
                df_to_write = df[present_cols].copy()

                sheet_name = _truncate_sheet_name(str(brand) or "Brand")
                # write sheet by pandas
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]

                # map headers -> excel letters
                header_cols = list(df_to_write.columns)
                col_map = {}
                for idx, col_name in enumerate(header_cols):
                    col_map[col_name] = {"idx": idx, "letter": _col_idx_to_excel_col(idx)}

                # ensure available & actual exist in col_map (append if missing)
                last_idx = len(header_cols) - 1
                if "available_quantity" not in col_map:
                    last_idx += 1
                    letter = _col_idx_to_excel_col(last_idx)
                    worksheet.write(0, last_idx, "available_quantity")
                    for r_idx, val in enumerate(df_to_write.get("available_quantity", []), start=1):
                        worksheet.write(r_idx, last_idx, val)
                    col_map["available_quantity"] = {"idx": last_idx, "letter": letter}
                if "actual_quantity" not in col_map:
                    last_idx += 1
                    letter = _col_idx_to_excel_col(last_idx)
                    worksheet.write(0, last_idx, "actual_quantity")
                    for r_idx in range(1, len(df_to_write) + 1):
                        worksheet.write(r_idx, last_idx, "")
                    col_map["actual_quantity"] = {"idx": last_idx, "letter": letter}

                # difference column placed at end
                diff_col_idx = max(v["idx"] for v in col_map.values()) + 1
                diff_col_letter = _col_idx_to_excel_col(diff_col_idx)
                worksheet.write(0, diff_col_idx, "difference")

                avail_letter = col_map["available_quantity"]["letter"]
                actual_letter = col_map["actual_quantity"]["letter"]

                # Write formulas per data row and collect summary refs
                for row_i in range(len(df_to_write)):
                    excel_row = row_i + 2  # data starts at row 2
                    avail_cell = f"{avail_letter}{excel_row}"
                    actual_cell = f"{actual_letter}{excel_row}"
                    formula = f"={actual_cell}-{avail_cell}"
                    worksheet.write_formula(row_i + 1, diff_col_idx, formula)

                    name_letter = col_map.get("name_en", {}).get("letter")
                    barcode_letter = col_map.get("barcodes", {}).get("letter")
                    name_cell_addr = f"'{sheet_name}'!{name_letter}{excel_row}" if name_letter else None
                    barcode_cell_addr = f"'{sheet_name}'!{barcode_letter}{excel_row}" if barcode_letter else None
                    diff_cell_addr = f"'{sheet_name}'!{diff_col_letter}{excel_row}"

                    summary_entries.append({
                        "name_cell": name_cell_addr,
                        "barcode_cell": barcode_cell_addr,
                        "diff_cell": diff_cell_addr
                    })

            # --- Create Summary sheet as FIRST sheet and fill formulas ---
            # add summary sheet (it will be last by default)
            summary_ws = workbook.add_worksheet("Summary")
            # move it to front (newly added is last; pop & insert at 0)
            workbook.worksheets_objs.insert(0, workbook.worksheets_objs.pop())

            # header
            summary_ws.write_row(0, 0, ["Product Name", "Barcode", "Difference"])

            if not summary_entries:
                summary_ws.write(1, 0, "No products were written to brand sheets; check schedule/product matching.")
            else:
                for i, entry in enumerate(summary_entries, start=1):
                    if entry.get("name_cell"):
                        summary_ws.write_formula(i, 0, f"={entry['name_cell']}")
                    else:
                        summary_ws.write(i, 0, "")
                    if entry.get("barcode_cell"):
                        summary_ws.write_formula(i, 1, f"={entry['barcode_cell']}")
                    else:
                        summary_ws.write(i, 1, "")
                    summary_ws.write_formula(i, 2, f"={entry['diff_cell']}")

        generated_files.append(out_path)

    return generated_files
