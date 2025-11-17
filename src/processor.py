# src/processor.py

from pathlib import Path
import re
import pandas as pd
import zipfile
from typing import Iterable, List

# utility helpers are expected to exist in src.utils
from src.utils import (
    normalize_columns,
    validate_product_columns,
    coerce_quantities,
    ensure_category_column,
)

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

def _normalize_text_for_matching(s):
    if s is None:
        return ""
    s = str(s).strip().lower()
    # remove non-alphanumeric to be more tolerant
    s = re.sub(r'[^0-9a-z]+', '', s)
    return s

def generate_branch_date_files(products_iter: Iterable[pd.DataFrame], schedule_df: pd.DataFrame, output_dir: Path):
    """
    Generate one Excel file per (branch, date). Each brand gets its own sheet where:
      - actual_quantity column is created (blank if missing)
      - difference column is written as an Excel formula: =actual_quantity - available_quantity
    Summary sheet references brand-sheet difference cells using formulas.
    This function is tolerant about branch name matching (normalizes and falls back).
    """
    import xlsxwriter

    output_dir.mkdir(parents=True, exist_ok=True)

    # Consume products_iter into a single DataFrame list (safe for matching).
    chunks = []
    try:
        for chunk in products_iter:
            if isinstance(chunk, pd.DataFrame):
                chunks.append(chunk.copy())
    except TypeError:
        # products_iter might itself be a DataFrame
        if isinstance(products_iter, pd.DataFrame):
            chunks = [products_iter.copy()]

    if chunks:
        all_products = pd.concat(chunks, ignore_index=True)
    else:
        all_products = pd.DataFrame()

    # Normalize product column names and ensure category
    all_products.columns = [str(c).strip() for c in all_products.columns]
    try:
        all_products = ensure_category_column(all_products)
    except Exception:
        # if ensure_category_column not applicable just continue
        pass

    # Normalize and prepare indexing columns
    if "branch_name" in all_products.columns:
        all_products["branch_norm_key"] = all_products["branch_name"].astype(str).str.strip().str.lower()
    else:
        all_products["branch_norm_key"] = ""

    if "brand" in all_products.columns:
        all_products["brand_norm_key"] = all_products["brand"].astype(str).str.strip().str.lower()
    else:
        all_products["brand_norm_key"] = ""

    # Build grouped index: (branch_norm, brand_norm) -> list of records
    grouped = {}
    for _, row in all_products.iterrows():
        b = str(row.get("branch_norm_key", "")).strip().lower()
        br = str(row.get("brand_norm_key", "")).strip().lower()
        grouped.setdefault((b, br), []).append(row.to_dict())

    # Helper to find best matching branch reported in products for a schedule branch
    product_branch_keys = {k[0] for k in grouped.keys() if k[0]}
    def find_best_branch_key(schedule_branch_raw):
        key = _normalize_text_for_matching(schedule_branch_raw)
        if not key:
            return None
        if key in product_branch_keys:
            return key
        # try fuzzy substring
        for pb in product_branch_keys:
            if key in pb or pb in key:
                return pb
        return None

    # Build schedule_map: map (branch_key_used, date_str) -> set(brands)
    schedule_map = {}
    for _, r in schedule_df.iterrows():
        sched_branch_raw = r.get("branch", "")
        sched_brand = r.get("brand", "")
        sched_date = r.get("date", None)
        if pd.isna(sched_branch_raw) or pd.isna(sched_brand) or pd.isna(sched_date):
            continue
        # find best branch key
        bkey = find_best_branch_key(sched_branch_raw)
        if bkey is None:
            # fall back to exact lower branch_name from schedule
            bkey = str(sched_branch_raw).strip().lower()
        date_str = sched_date.strftime("%d-%m-%Y") if hasattr(sched_date, "strftime") else str(sched_date)
        schedule_map.setdefault((bkey, date_str), set()).add(sched_brand)

    if not schedule_map:
        return []

    generated_files = []

    # For each (branch_key, date_str) create file
    for (branch_key, date_str), brand_set in schedule_map.items():
        # recover a display branch name (try from products else use branch_key)
        sample_rows = all_products[all_products["branch_norm_key"] == branch_key]
        original_branch = sample_rows["branch_name"].iloc[0] if not sample_rows.empty else branch_key
        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsx"
        out_path = output_dir / filename

        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book
            summary_entries = []

            for brand in brand_set:
                brand_norm = str(brand).strip().lower()
                rows = grouped.get((branch_key, brand_norm), [])
                if not rows:
                    # attempt approximate brand matching
                    candidate_rows = []
                    for (bk, brk), recs in grouped.items():
                        if bk == branch_key and (brand_norm in brk or brk in brand_norm):
                            candidate_rows.extend(recs)
                    rows = candidate_rows

                if not rows:
                    # no matching products for this brand in this branch
                    continue

                df = pd.DataFrame(rows)
                # ensure columns and category
                df.columns = [str(c).strip() for c in df.columns]
                try:
                    df = ensure_category_column(df)
                except Exception:
                    pass

                if "actual_quantity" not in df.columns:
                    df["actual_quantity"] = ""
                else:
                    df["actual_quantity"] = df["actual_quantity"].fillna("")

                # Remove existing difference column to write formulas
                if "difference" in df.columns:
                    df = df.drop(columns=["difference"])

                cols_order = ["name_en", "category", "branch_name", "barcodes", "brand", "available_quantity", "actual_quantity"]
                present_cols = [c for c in cols_order if c in df.columns]
                df_to_write = df[present_cols].copy()

                sheet_name = _truncate_sheet_name(str(brand) or "Brand")
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]

                # Map header -> Excel column letters
                header_cols = list(df_to_write.columns)
                col_map = {}
                for idx, col_name in enumerate(header_cols):
                    col_map[col_name] = {"idx": idx, "letter": _col_idx_to_excel_col(idx)}

                # ensure available & actual exist (they should)
                if "available_quantity" not in col_map:
                    # append column
                    append_idx = len(header_cols)
                    col_letter = _col_idx_to_excel_col(append_idx)
                    worksheet.write(0, append_idx, "available_quantity")
                    for r_idx, val in enumerate(df_to_write.get("available_quantity", []), start=1):
                        worksheet.write(r_idx, append_idx, val)
                    col_map["available_quantity"] = {"idx": append_idx, "letter": col_letter}

                if "actual_quantity" not in col_map:
                    append_idx = len(header_cols) + (1 if "available_quantity" in col_map else 0)
                    col_letter = _col_idx_to_excel_col(append_idx)
                    worksheet.write(0, append_idx, "actual_quantity")
                    for r_idx in range(1, len(df_to_write) + 1):
                        worksheet.write(r_idx, append_idx, "")
                    col_map["actual_quantity"] = {"idx": append_idx, "letter": col_letter}

                # Place difference at the end
                diff_col_idx = max(v["idx"] for v in col_map.values()) + 1
                diff_col_letter = _col_idx_to_excel_col(diff_col_idx)
                worksheet.write(0, diff_col_idx, "difference")

                avail_letter = col_map["available_quantity"]["letter"]
                actual_letter = col_map["actual_quantity"]["letter"]

                # Write formulas for each row
                for row_i in range(len(df_to_write)):
                    excel_row = row_i + 2  # data starts at row 2 (1-based)
                    avail_cell = f"{avail_letter}{excel_row}"
                    actual_cell = f"{actual_letter}{excel_row}"
                    formula = f"={actual_cell}-{avail_cell}"
                    worksheet.write_formula(row_i + 1, diff_col_idx, formula)

                    # store references for summary
                    name_letter = col_map.get("name_en", {}).get("letter")
                    barcode_letter = col_map.get("barcodes", {}).get("letter")
                    name_cell = f"'{sheet_name}'!{name_letter}{excel_row}" if name_letter else None
                    barcode_cell = f"'{sheet_name}'!{barcode_letter}{excel_row}" if barcode_letter else None
                    diff_cell_addr = f"'{sheet_name}'!{diff_col_letter}{excel_row}"
                    summary_entries.append({"name": name_cell, "barcode": barcode_cell, "diff": diff_cell_addr})

            # Write Summary sheet referencing brand sheets
            if summary_entries:
                summary_ws = workbook.add_worksheet("Summary")
                summary_ws.write_row(0, 0, ["Product Name", "Barcode", "Difference"])
                for i, ent in enumerate(summary_entries, start=1):
                    if ent["name"]:
                        summary_ws.write_formula(i, 0, f"={ent['name']}")
                    else:
                        summary_ws.write(i, 0, "")
                    if ent["barcode"]:
                        summary_ws.write_formula(i, 1, f"={ent['barcode']}")
                    else:
                        summary_ws.write(i, 1, "")
                    summary_ws.write_formula(i, 2, f"={ent['diff']}")

        generated_files.append(out_path)

    return generated_files
