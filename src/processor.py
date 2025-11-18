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
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    def validate_product_columns(df: pd.DataFrame):
        required = {"name_en", "branch_name", "barcodes", "brand", "available_quantity"}
        cols = {str(c).strip().lower() for c in df.columns}
        missing = required - cols
        if missing:
            raise ValueError(f"Missing required product columns: {missing}")

    def coerce_quantities(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "available_quantity" in df.columns:
            df["available_quantity"] = pd.to_numeric(df["available_quantity"], errors="coerce").fillna(0)
        return df

    def ensure_category_column(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "name_en" not in df.columns:
            return df
        if "category" not in df.columns:
            def extract_cat(n):
                try:
                    parts = str(n).split("-")
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
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in paths:
            z.write(p, arcname=p.name)


def _truncate_sheet_name(name: str) -> str:
    if not isinstance(name, str):
        name = str(name)
    return name[:31]


def _col_idx_to_excel_col(idx: int) -> str:
    letters = ""
    n = idx + 1
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _normalize_text_for_matching(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^0-9a-z]+", "", s)
    return s


# Mapping internal column keys -> display names
_DISPLAY_COL_MAP = {
    "name_en": "Product Name",
    "category": "Category",
    "branch_name": "Branch Name",
    "barcodes": "Barcode",            # singular "Barcode"
    "brand": "Brand",
    "available_quantity": "Available Quantity",
    "actual_quantity": "Actual Quantity",
    "difference": "Difference",
}


def _compute_column_widths(df: pd.DataFrame, headers: List[str]) -> List[int]:
    widths = []
    for col in headers:
        max_len = len(str(col))
        if col in df.columns:
            series = df[col].astype(str).fillna("")
            try:
                sample_max = int(series.map(len).max()) if not series.empty else 0
            except Exception:
                sample_max = 0
            max_len = max(max_len, sample_max)
        w = min(max(max_len + 2, 8), 60)
        widths.append(w)
    return widths


def generate_branch_date_files(products_iter: Iterable[pd.DataFrame], schedule_df: pd.DataFrame, output_dir: Path) -> List[Path]:
    """
    Generate .xlsm (macro-enabled) Excel reports per (branch, date).
    If src/vba/vbaProject.bin exists, it will be attached to each workbook.
    """
    import xlsxwriter

    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect product chunks (iterator or single DataFrame)
    chunks = []
    try:
        for chunk in products_iter:
            if isinstance(chunk, pd.DataFrame):
                chunks.append(chunk.copy())
    except TypeError:
        if isinstance(products_iter, pd.DataFrame):
            chunks = [products_iter.copy()]
        else:
            chunks = []

    all_products = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    all_products = normalize_columns(all_products)
    try:
        all_products = ensure_category_column(all_products)
    except Exception:
        pass
    all_products = coerce_quantities(all_products)

    if "branch_name" in all_products.columns:
        all_products["branch_norm_key"] = all_products["branch_name"].astype(str).str.strip().str.lower()
    else:
        all_products["branch_norm_key"] = ""

    if "brand" in all_products.columns:
        all_products["brand_norm_key"] = all_products["brand"].astype(str).str.strip().str.lower()
    else:
        all_products["brand_norm_key"] = ""

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
        for pb in product_branch_keys:
            if key in pb or pb in key:
                return pb
        return None

    # Build schedule_map: (branch_key, date_str) -> set(brands)
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
        try:
            date_str = sched_date.strftime("%d-%m-%Y")
        except Exception:
            date_str = str(sched_date)
        schedule_map.setdefault((bkey, date_str), set()).add(sched_brand)

    if not schedule_map:
        return []

    generated_files: List[Path] = []

    # vba binary path expected in repo
    vba_bin_path = Path(__file__).resolve().parent / "vba" / "vbaProject.bin"

    for (branch_key, date_str), brand_set in schedule_map.items():
        sample_rows = all_products[all_products["branch_norm_key"] == branch_key]
        original_branch = sample_rows["branch_name"].iloc[0] if not sample_rows.empty else branch_key
        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsm"
        out_path = output_dir / filename

        # NOTE: removed 'options' kwarg for ExcelWriter to be compatible with CI pandas version
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book

            # attach vbaProject.bin if exists
            if vba_bin_path.exists():
                try:
                    workbook.add_vba_project(str(vba_bin_path))
                except Exception as e:
                    # if attach fails, continue but warn in stdout
                    print(f"Warning: failed to attach vbaProject.bin: {e}")
            else:
                # Informative warning (not fatal). The generated .xlsm will NOT contain the macro.
                print(f"Warning: vbaProject.bin not found at {vba_bin_path}; generated .xlsm will not include scanner macro.")

            summary_entries = []

            # write brand sheets
            for brand in brand_set:
                brand_norm = str(brand).strip().lower()
                rows = grouped.get((branch_key, brand_norm), [])
                if not rows:
                    candidate_rows = []
                    for (bk, brk), recs in grouped.items():
                        if bk == branch_key and (brand_norm in brk or brk in brand_norm):
                            candidate_rows.extend(recs)
                    rows = candidate_rows
                if not rows:
                    continue

                df = pd.DataFrame(rows)
                df = normalize_columns(df)
                try:
                    df = ensure_category_column(df)
                except Exception:
                    pass
                df = coerce_quantities(df)

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

                # Map internal names to display names
                display_headers = [_DISPLAY_COL_MAP.get(c, c) for c in df_to_write.columns]
                df_to_write.columns = display_headers

                sheet_name = _truncate_sheet_name(str(brand) or "Brand")
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]

                header_cols = list(df_to_write.columns)
                col_map = {}
                for idx, col_name in enumerate(header_cols):
                    col_map[col_name] = {"idx": idx, "letter": _col_idx_to_excel_col(idx)}

                avail_disp = _DISPLAY_COL_MAP.get("available_quantity")
                actual_disp = _DISPLAY_COL_MAP.get("actual_quantity")

                last_idx = len(header_cols) - 1
                if avail_disp not in col_map:
                    last_idx += 1
                    letter = _col_idx_to_excel_col(last_idx)
                    worksheet.write(0, last_idx, avail_disp)
                    col_map[avail_disp] = {"idx": last_idx, "letter": letter}
                if actual_disp not in col_map:
                    last_idx += 1
                    letter = _col_idx_to_excel_col(last_idx)
                    worksheet.write(0, last_idx, actual_disp)
                    col_map[actual_disp] = {"idx": last_idx, "letter": letter}

                diff_idx = max(v["idx"] for v in col_map.values()) + 1
                diff_letter = _col_idx_to_excel_col(diff_idx)
                worksheet.write(0, diff_idx, _DISPLAY_COL_MAP.get("difference", "Difference"))

                avail_letter = col_map[avail_disp]["letter"]
                actual_letter = col_map[actual_disp]["letter"]

                widths = _compute_column_widths(df_to_write, header_cols)
                diff_width = 12
                widths.append(diff_width)

                header_format = workbook.add_format({"bold": True})
                default_format = workbook.add_format({})

                # set header bold only
                worksheet.set_row(0, None, header_format)

                for row_i in range(len(df_to_write)):
                    excel_row = row_i + 2
                    avail_cell = f"{avail_letter}{excel_row}"
                    actual_cell = f"{actual_letter}{excel_row}"
                    formula = f"={actual_cell}-{avail_cell}"
                    worksheet.write_formula(row_i + 1, diff_idx, formula)

                    name_letter = col_map.get(_DISPLAY_COL_MAP.get("name_en"))["letter"] if _DISPLAY_COL_MAP.get("name_en") in col_map else None
                    barcode_letter = col_map.get(_DISPLAY_COL_MAP.get("barcodes"))["letter"] if _DISPLAY_COL_MAP.get("barcodes") in col_map else None
                    name_cell_addr = f"'{sheet_name}'!{name_letter}{excel_row}" if name_letter else None
                    barcode_cell_addr = f"'{sheet_name}'!{barcode_letter}{excel_row}" if barcode_letter else None
                    diff_cell_addr = f"'{sheet_name}'!{diff_letter}{excel_row}"

                    summary_entries.append({
                        "name_cell": name_cell_addr,
                        "barcode_cell": barcode_cell_addr,
                        "diff_cell": diff_cell_addr
                    })

                # set column widths without bolding data rows (only headers bold)
                for idx, col_name in enumerate(header_cols):
                    col_width = widths[idx]
                    worksheet.set_column(idx, idx, col_width, default_format)
                worksheet.set_column(diff_idx, diff_idx, widths[-1], default_format)

            # After brand sheets, add Scanner sheet (appended at end for now)
            scanner_ws = workbook.add_worksheet("Scanner")
            # put instructions and keep A1 blank for scanner input
            scanner_ws.write(0, 0, "Scanner input (A1). Place cursor in A1 or enable macros to auto-focus.")
            scanner_ws.write(2, 0, "How to use: 1) Enable macros. 2) Open Scanner sheet. 3) Ensure cell A1 is selected. 4) Scan barcodes. Macro will increment Actual Quantity.")
            # make A1 a little bigger
            scanner_ws.set_column(0, 0, 30)

            # Create Summary sheet (appended last), then move it to be FIRST sheet
            summary_ws = workbook.add_worksheet("Summary")
            # move Summary to the front
            workbook.worksheets_objs.insert(0, workbook.worksheets_objs.pop())

            header_format = workbook.add_format({"bold": True})
            default_format = workbook.add_format({})

            # Use exact header names expected by tests: 'Product Name', 'Barcode', 'Difference'
            summary_headers = [_DISPLAY_COL_MAP["name_en"], "Barcode", _DISPLAY_COL_MAP["difference"]]
            summary_ws.write_row(0, 0, summary_headers, header_format)

            summary_rows = []
            for ent in summary_entries:
                summary_rows.append({"Product Name": "", "Barcode": "", "Difference": ""})
            summary_df = pd.DataFrame(summary_rows)
            summary_widths = _compute_column_widths(summary_df, summary_headers)
            summary_widths = [max(12, w) for w in summary_widths]

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

            # Apply widths to summary columns; only header row is bold so use default_format for columns
            summary_ws.set_column(0, 0, summary_widths[0], default_format)
            summary_ws.set_column(1, 1, summary_widths[1], default_format)
            summary_ws.set_column(2, 2, summary_widths[2], default_format)

        generated_files.append(out_path)

    return generated_files
