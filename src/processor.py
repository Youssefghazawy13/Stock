from pathlib import Path
import pandas as pd
import zipfile
from src.utils import normalize_columns, validate_product_columns, coerce_quantities, ensure_category_column

def create_zip_from_paths(paths, zip_path):
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

def col_idx_to_excel_col(idx: int) -> str:
    """
    Convert 0-based column index to Excel column letters (0 -> A, 25 -> Z, 26 -> AA).
    """
    letters = ""
    n = idx + 1
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def generate_branch_date_files(products_iter, schedule_df, output_dir: Path):
    """
    Generate one Excel file per (branch, date). Each brand gets its own sheet where:
      - actual_quantity column is created (blank)
      - difference column is an Excel formula: =actual_quantity - available_quantity
    Summary sheet contains formulas referencing each brand sheet so it updates live.
    """
    # Ensure output dir exists
    output_dir.mkdir(parents=True, exist_ok=True)

    schedule = schedule_df.copy()
    # normalizing schedule to help matching (we expect schedule to have columns branch, date, brand)
    schedule["branch_norm"] = schedule["branch"].astype(str).str.strip().str.lower()
    schedule["brand_norm"] = schedule["brand"].astype(str).str.strip().str.lower()
    schedule["date_str"] = schedule["date"].apply(lambda d: d.strftime("%d-%m-%Y"))

    # Build map: (branch_norm, date_str) -> set of brands
    schedule_map = {}
    for _, r in schedule.iterrows():
        key = (r["branch_norm"], r["date_str"])
        schedule_map.setdefault(key, set()).add(r["brand"])

    if not schedule_map:
        return []

    accum = {}  # accum[(branch_norm, date_str)][brand] = list(rows)
    branches_found = set()

    # Aggregate matching rows from products_iter into accum
    for chunk in products_iter:
        chunk = ensure_category_column(chunk)
        chunk = normalize_columns(chunk)
        try:
            validate_product_columns(chunk)
        except Exception:
            # If required columns missing in this chunk, skip it
            continue
        chunk = coerce_quantities(chunk)

        chunk["branch_norm"] = chunk["branch_name"].astype(str).str.strip().str.lower()
        chunk["brand_norm"] = chunk["brand"].astype(str).str.strip().str.lower()
        branches_found.update(chunk["branch_norm"].unique())

        chunk_branches = set(chunk["branch_norm"].unique())

        for (branch_norm, date_str), brand_set in schedule_map.items():
            if branch_norm not in chunk_branches:
                continue
            branch_rows = chunk[chunk["branch_norm"] == branch_norm]
            for brand in brand_set:
                brand_norm = str(brand).strip().lower()
                matched = branch_rows[branch_rows["brand_norm"] == brand_norm]
                if matched.empty:
                    continue
                key = (branch_norm, date_str)
                accum.setdefault(key, {})
                accum[key].setdefault(brand, [])
                accum[key][brand].extend(matched.to_dict(orient="records"))

    generated_files = []

    # For each (branch,date) generate one Excel file
    for (branch_norm, date_str), brand_set in schedule_map.items():
        # recover branch display name from schedule if possible
        original_branch_series = schedule[schedule["branch_norm"] == branch_norm]["branch"]
        original_branch = original_branch_series.iloc[0] if not original_branch_series.empty else branch_norm

        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsx"
        out_path = output_dir / filename

        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book

            # If no accumulated data for this branch/date, produce an ERROR sheet or info sheet
            key = (branch_norm, date_str)
            if key not in accum:
                if branch_norm not in branches_found:
                    pd.DataFrame([{"error": f"No products found for branch '{original_branch}'."}]) \
                        .to_excel(writer, sheet_name="ERROR", index=False)
                    generated_files.append(out_path)
                    continue
                else:
                    pd.DataFrame([{"error": f"No matching products for scheduled brands on {date_str}."}]) \
                        .to_excel(writer, sheet_name="ERROR", index=False)
                    generated_files.append(out_path)
                    continue

            # summary_entries will hold references to product cells for building the formula Summary
            summary_entries = []  # elements: {"sheet": sheet_name, "name_cell": addr, "barcode_cell": addr, "diff_cell": addr}

            # Write brand sheets
            for brand, rows in accum[key].items():
                if not rows:
                    continue
                df = pd.DataFrame(rows)
                df = normalize_columns(df)
                df = coerce_quantities(df)
                df = ensure_category_column(df)

                # Desired columns order for output
                cols_order = [
                    "name_en", "category", "branch_name", "barcodes",
                    "brand", "available_quantity", "actual_quantity", "difference"
                ]

                # Ensure actual_quantity exists (blank if missing)
                if "actual_quantity" not in df.columns:
                    df["actual_quantity"] = ""  # blank for the user to fill later
                else:
                    df["actual_quantity"] = df["actual_quantity"].fillna("")

                # Remove difference if present; we'll write Excel formulas instead
                if "difference" in df.columns:
                    df = df.drop(columns=["difference"])

                present_cols = [c for c in cols_order if c in df.columns]
                df_to_write = df[present_cols].copy()

                sheet_name = _truncate_sheet_name(str(brand))
                # Write dataframe to sheet (this writes headers and values)
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)

                worksheet = writer.sheets[sheet_name]

                # Map header columns to Excel letters
                header_cols = list(df_to_write.columns)
                col_map = {}
                for idx, col_name in enumerate(header_cols):
                    excel_col_letter = col_idx_to_excel_col(idx)  # 0 -> A
                    col_map[col_name] = {"idx": idx, "letter": excel_col_letter}

                # If available_quantity missing (unlikely) ensure it's present
                if "available_quantity" not in col_map:
                    # append available_quantity
                    append_idx = len(header_cols)
                    excel_col_letter = col_idx_to_excel_col(append_idx)
                    worksheet.write(0, append_idx, "available_quantity")
                    for r_idx, val in enumerate(df_to_write.get("available_quantity", []), start=1):
                        worksheet.write(r_idx, append_idx, val)
                    col_map["available_quantity"] = {"idx": append_idx, "letter": excel_col_letter}

                # Ensure actual_quantity in map (we created earlier)
                if "actual_quantity" not in col_map:
                    append_idx = len(header_cols)
                    excel_col_letter = col_idx_to_excel_col(append_idx)
                    worksheet.write(0, append_idx, "actual_quantity")
                    for r_idx in range(1, len(df_to_write) + 1):
                        worksheet.write(r_idx, append_idx, "")
                    col_map["actual_quantity"] = {"idx": append_idx, "letter": excel_col_letter}

                # Place the difference column at the end if not present
                if "difference" not in col_map:
                    diff_col_idx = max([v["idx"] for v in col_map.values()]) + 1
                    diff_col_letter = col_idx_to_excel_col(diff_col_idx)
                    worksheet.write(0, diff_col_idx, "difference")
                else:
                    diff_col_idx = col_map["difference"]["idx"]
                    diff_col_letter = col_map["difference"]["letter"]

                avail_letter = col_map["available_quantity"]["letter"]
                actual_letter = col_map["actual_quantity"]["letter"]

                # Write formula for each data row
                for row_i in range(len(df_to_write)):
                    excel_row = row_i + 2  # Excel data starts at row 2 (1-based)
                    avail_cell = f"{avail_letter}{excel_row}"
                    actual_cell = f"{actual_letter}{excel_row}"
                    formula = f"={actual_cell}-{avail_cell}"
                    worksheet.write_formula(row_i + 1, diff_col_idx, formula)

                    # Prepare summary references
                    name_letter = col_map.get("name_en", {}).get("letter")
                    barcode_letter = col_map.get("barcodes", {}).get("letter")
                    diff_cell_addr = f"'{sheet_name}'!{diff_col_letter}{excel_row}"
                    name_cell_addr = f"'{sheet_name}'!{name_letter}{excel_row}" if name_letter else None
                    barcode_cell_addr = f"'{sheet_name}'!{barcode_letter}{excel_row}" if barcode_letter else None

                    summary_entries.append({
                        "sheet": sheet_name,
                        "name_cell": name_cell_addr,
                        "barcode_cell": barcode_cell_addr,
                        "diff_cell": diff_cell_addr
                    })

            # Now create Summary sheet and write formulas referencing brand sheets
            summary_sheet_name = "Summary"
            # Use workbook.add_worksheet to avoid pandas overwriting later
            worksheet_summary = workbook.add_worksheet(summary_sheet_name)
            # Header
            worksheet_summary.write_row(0, 0, ["Product Name", "Barcode", "Difference"])

            for sr_idx, entry in enumerate(summary_entries, start=1):
                # Product Name
                if entry["name_cell"]:
                    worksheet_summary.write_formula(sr_idx, 0, f"={entry['name_cell']}")
                else:
                    worksheet_summary.write(sr_idx, 0, "")
                # Barcode
                if entry["barcode_cell"]:
                    worksheet_summary.write_formula(sr_idx, 1, f"={entry['barcode_cell']}")
                else:
                    worksheet_summary.write(sr_idx, 1, "")
                # Difference
                worksheet_summary.write_formula(sr_idx, 2, f"={entry['diff_cell']}")

        generated_files.append(out_path)

    return generated_files
