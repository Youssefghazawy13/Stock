from pathlib import Path
import pandas as pd
from src.utils import normalize_columns, validate_product_columns, coerce_quantities
import zipfile

def create_zip_from_paths(paths, zip_path):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in paths:
            z.write(p, arcname=p.name)

def _truncate_sheet_name(name: str) -> str:
    if not isinstance(name, str):
        name = str(name)
    return name[:31]

def generate_branch_date_files(products_iter, schedule_df, output_dir: Path):
    """
    products_iter: iterator yielding product DataFrame chunks (or iterator with single DF)
    schedule_df: DataFrame with columns 'branch', 'date' (datetime.date), 'brand'
    output_dir: Path to directory where files will be written
    Returns: list of generated file paths (Path objects)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Normalize schedule and build mapping (branch_norm, date_str) -> set(brands original case)
    schedule = schedule_df.copy()
    schedule["branch_norm"] = schedule["branch"].astype(str).str.strip().str.lower()
    schedule["brand_norm"] = schedule["brand"].astype(str).str.strip().str.lower()
    schedule["date_str"] = schedule["date"].apply(lambda d: d.strftime("%d-%m-%Y"))

    schedule_map = {}
    for _, r in schedule.iterrows():
        key = (r["branch_norm"], r["date_str"])
        schedule_map.setdefault(key, set()).add(r["brand"])

    if not schedule_map:
        return []

    # accum[(branch_norm, date_str)][brand_original] = list of rows(dict)
    accum = {}
    branches_found = set()

    for chunk in products_iter:
        # normalize columns lower-case
        chunk = normalize_columns(chunk)
        # ensure required product columns exist (raises if missing)
        try:
            validate_product_columns(chunk)
        except Exception:
            # If validation fails on a chunk, skip chunk (or you could raise). We raise to alert user.
            raise

        # coerce numeric quantities and compute difference
        chunk = coerce_quantities(chunk)

        # prepare normalized comparison columns
        chunk["branch_norm"] = chunk["branch_name"].astype(str).str.strip().str.lower()
        chunk["brand_norm"] = chunk["brand"].astype(str).str.strip().str.lower()

        branches_found.update(chunk["branch_norm"].unique())

        # for efficiency: iterate only schedule keys with branch present in this chunk
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
                # Convert to dict records (preserve all relevant columns)
                records = matched.to_dict(orient="records")
                accum[key][brand].extend(records)

    generated_files = []

    # For each schedule key, produce an Excel workbook
    for (branch_norm, date_str), brand_set in schedule_map.items():
        # get display branch name from schedule rows (preserve original casing)
        original_branch = schedule[ schedule["branch_norm"] == branch_norm ]["branch"].iloc[0] \
            if any(schedule["branch_norm"] == branch_norm) else branch_norm

        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsx"
        out_path = output_dir / filename

        # create Excel writer
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            # Keep a list to build the Summary sheet
            summary_rows = []

            key = (branch_norm, date_str)
            if key not in accum:
                # If branch not present in products at all -> write ERROR sheet
                if branch_norm not in branches_found:
                    pd.DataFrame([{"error": f"No products found for branch '{original_branch}' in uploaded products file."}]) \
                      .to_excel(writer, sheet_name="ERROR", index=False)
                    generated_files.append(out_path)
                    continue
                else:
                    # branch exists but no matching brand rows
                    pd.DataFrame([{"error": f"No matching products for scheduled brands on {date_str}."}]) \
                      .to_excel(writer, sheet_name="ERROR", index=False)
                    generated_files.append(out_path)
                    continue

            # For each brand that had matches, write sheet
            for brand, rows in accum[key].items():
                if not rows:
                    continue
                df = pd.DataFrame(rows)
                df = normalize_columns(df)
                df = coerce_quantities(df)

                # Ensure columns order for sheet
                cols_order = ["name_en", "barcodes", "available_quantity", "actual_quantity", "difference", "brand", "branch_name"]
                present_cols = [c for c in cols_order if c in df.columns]
                df_to_write = df[present_cols]

                sheet_name = _truncate_sheet_name(brand)
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)

                # Collect summary rows from this sheet
                if "name_en" in df_to_write.columns and "barcodes" in df_to_write.columns and "difference" in df_to_write.columns:
                    # For each row, append a dict for the summary
                    for _, r in df_to_write.iterrows():
                        summary_rows.append({
                            "Product Name": r.get("name_en"),
                            "Barcode": r.get("barcodes"),
                            "Difference": r.get("difference")
                        })

            # Now write the Summary sheet (aggregate of all brand sheets)
            if summary_rows:
                summary_df = pd.DataFrame(summary_rows)
                # Option: dedupe by Barcode keeping latest occurrence; user did not request dedupe explicitly,
                # so we will keep all rows as they were found. If you want deduplication, uncomment the next line:
                # summary_df = summary_df.drop_duplicates(subset=["Barcode", "Product Name"])
                # Write summary sheet
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
            else:
                pd.DataFrame([{"info": "No products included in this report."}]).to_excel(writer, sheet_name="Summary", index=False)

        generated_files.append(out_path)

    return generated_files
