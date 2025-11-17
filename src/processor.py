def generate_branch_date_files(products_iter, schedule_df, output_dir: Path):
    """
    Generate one Excel file per (branch, date). Each brand gets its own sheet where:
      - actual_quantity column is created (blank)
      - difference column is an Excel formula: =actual_quantity - available_quantity
    Summary sheet contains formulas referencing each brand sheet so it updates live.
    """
    import pandas as pd
    import xlsxwriter

    def col_idx_to_excel_col(idx: int) -> str:
        """Convert 0-based column index to Excel column letters (0 -> A)."""
        letters = ""
        n = idx + 1
        while n:
            n, rem = divmod(n - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    output_dir.mkdir(parents=True, exist_ok=True)

    schedule = schedule_df.copy()
    schedule["branch_norm"] = schedule["branch"].astype(str).str.strip().str.lower()
    schedule["brand_norm"] = schedule["brand"].astype(str).str.strip().str.lower()
    schedule["date_str"] = schedule["date"].apply(lambda d: d.strftime("%d-%m-%Y"))

    # Build a map: (branch_norm, date_str) -> set of brands (original spelling)
    schedule_map = {}
    for _, r in schedule.iterrows():
        key = (r["branch_norm"], r["date_str"])
        schedule_map.setdefault(key, set()).add(r["brand"])

    if not schedule_map:
        return []

    accum = {}
    branches_found = set()

    # Aggregate matching rows from products_iter into accum[(branch_norm,date_str)][brand] = list(rows)
    for chunk in products_iter:
        chunk = ensure_category_column(chunk)
        chunk = normalize_columns(chunk)
        try:
            validate_product_columns(chunk)
        except Exception:
            # If required columns missing, skip this chunk
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
        # try to recover original branch name casing from schedule
        original_branch_series = schedule[schedule["branch_norm"] == branch_norm]["branch"]
        original_branch = original_branch_series.iloc[0] if not original_branch_series.empty else branch_norm

        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsx"
        out_path = output_dir / filename

        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book
            summary_rows_count = 0

            # We'll build Summary sheet later using direct formulas; prepare writer first
            # For each brand create its sheet
            key = (branch_norm, date_str)
            if key not in accum:
                # No products matched for this branch/date
                if branch_norm not in branches_found:
                    # branch not found at all in products source
                    pd.DataFrame([{"error": f"No products found for branch '{original_branch}'."}]) \
                      .to_excel(writer, sheet_name="ERROR", index=False)
                    generated_files.append(out_path)
                    continue
                else:
                    pd.DataFrame([{"error": f"No matching products for scheduled brands on {date_str}."}]) \
                      .to_excel(writer, sheet_name="ERROR", index=False)
                    generated_files.append(out_path)
                    continue

            # We'll write each brand sheet and after that build the Summary sheet referencing them.
            # Keep track of where each product's difference cell is so we can reference it from Summary.
            summary_entries = []  # list of dicts: {"sheet": sheet_name, "name_col": 'A', "barcode_col": 'D', "diff_cell": 'H2'}

            for brand, rows in accum[key].items():
                if not rows:
                    continue
                df = pd.DataFrame(rows)
                df = normalize_columns(df)
                df = coerce_quantities(df)
                df = ensure_category_column(df)

                # Ensure columns exist and order them as requested
                cols_order = [
                    "name_en", "category", "branch_name", "barcodes",
                    "brand", "available_quantity", "actual_quantity", "difference"
                ]

                # Ensure 'actual_quantity' exists (create blank column if missing)
                if "actual_quantity" not in df.columns:
                    df["actual_quantity"] = ""  # blank -> user will fill later
                else:
                    # keep actual values if any, but convert to string for blanks
                    df["actual_quantity"] = df["actual_quantity"].fillna("")

                # Remove difference column if present; we'll write formulas
                if "difference" in df.columns:
                    df = df.drop(columns=["difference"])

                # Build present columns in the requested order
                present_cols = [c for c in cols_order if c in df.columns]
                df_to_write = df[present_cols].copy()

                # Write brand sheet
                sheet_name = str(brand)[:31]  # Excel sheet name limit
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)

                worksheet = writer.sheets[sheet_name]

                # Determine columns positions (0-based) in this sheet as written
                # (headers are in row 0; data starts at Excel row 2)
                header_cols = list(df_to_write.columns)
                # map column name -> excel col letter and 0-based index
                col_map = {}
                for idx, col_name in enumerate(header_cols):
                    excel_col_letter = col_idx_to_excel_col(idx)  # 0 -> A
                    col_map[col_name] = {"idx": idx, "letter": excel_col_letter}

                # Ensure available_quantity and actual_quantity are present now
                if "available_quantity" not in col_map:
                    # create an available_quantity column with 0s if missing
                    df_to_write["available_quantity"] = 0
                    col_idx = len(header_cols)
                    excel_col_letter = col_idx_to_excel_col(col_idx)
                    col_map["available_quantity"] = {"idx": col_idx, "letter": excel_col_letter}
                    # write the new column header and values
                    worksheet.write(0, col_idx, "available_quantity")
                    for r_idx, val in enumerate(df_to_write["available_quantity"], start=1):
                        worksheet.write(r_idx, col_idx, val)
                if "actual_quantity" not in col_map:
                    # should not happen because we created earlier, but be safe
                    col_idx = len(header_cols) if "available_quantity" in col_map else len(header_cols)
                    excel_col_letter = col_idx_to_excel_col(col_idx)
                    col_map["actual_quantity"] = {"idx": col_idx, "letter": excel_col_letter}
                    worksheet.write(0, col_idx, "actual_quantity")
                    for r_idx in range(1, len(df_to_write) + 1):
                        worksheet.write(r_idx, col_idx, "")

                # Now compute where to write the difference formula column.
                # If difference column not in header list, place it at the end.
                if "difference" not in col_map:
                    diff_col_idx = max([v["idx"] for v in col_map.values()]) + 1
                    diff_col_letter = col_idx_to_excel_col(diff_col_idx)
                    # write header
                    worksheet.write(0, diff_col_idx, "difference")
                else:
                    diff_col_idx = col_map["difference"]["idx"]
                    diff_col_letter = col_map["difference"]["letter"]

                # Identify available and actual column letters
                avail_letter = col_map["available_quantity"]["letter"]
                actual_letter = col_map["actual_quantity"]["letter"]

                # Write formulas for each data row
                for row_i in range(len(df_to_write)):
                    excel_row = row_i + 2  # Excel rows start at 1; +1 for header => data starts at row 2
                    avail_cell = f"{avail_letter}{excel_row}"
                    actual_cell = f"{actual_letter}{excel_row}"
                    formula = f"={actual_cell}-{avail_cell}"
                    # Write formula into difference cell
                    worksheet.write_formula(row_i + 1, diff_col_idx, formula)

                    # For summary, note the cells for product name, barcode, and difference
                    # name column assumed to exist
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

            # After writing all brand sheets, write Summary sheet that references brand sheets
            summary_sheet = "Summary"
            worksheet_summary = workbook.add_worksheet(summary_sheet)
            # write header
            worksheet_summary.write_row(0, 0, ["Product Name", "Barcode", "Difference"])
            # write formulas referencing brand sheets
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
                # Difference (reference to brand sheet difference cell)
                worksheet_summary.write_formula(sr_idx, 2, f"={entry['diff_cell']}")

        generated_files.append(out_path)

    return generated_files

