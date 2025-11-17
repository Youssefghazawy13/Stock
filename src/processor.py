from pathlib import Path
import pandas as pd
from src.utils import normalize_columns, validate_product_columns, coerce_quantities
from src.utils import ensure_category_column
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
    schedule_df: DataFrame with columns 'branch' (str), 'date' (datetime.date), 'brand' (str)
    output_dir: Path
    Returns: list of generated file paths
    """
    output_dir.mkdir(parents=True, exist_ok=True)

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

    accum = {}
    branches_found = set()

    # products_iter is an iterator yielding DataFrames (chunks)
    for chunk in products_iter:
        # ensure category exists and normalized columns
        chunk = ensure_category_column(chunk)
        chunk = normalize_columns(chunk)

        # validate required columns exist in this chunk (raises if missing)
        validate_product_columns(chunk)

        # numeric quantities and difference
        chunk = coerce_quantities(chunk)

        # normalize matching columns
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

    for (branch_norm, date_str), brand_set in schedule_map.items():
        # get original branch name (preserve casing) if available
        original_branch = schedule[ schedule["branch_norm"] == branch_norm ]["branch"].iloc[0] \
            if any(schedule["branch_norm"] == branch_norm) else branch_norm

        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsx"
        out_path = output_dir / filename

        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            summary_rows = []

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

            # write each brand sheet
            for brand, rows in accum[key].items():
                if not rows:
                    continue
                df = pd.DataFrame(rows)
                df = normalize_columns(df)
                df = coerce_quantities(df)
                # ensure category column exists (should, from read)
                df = ensure_category_column(df)

                cols_order = [
                    "name_en", "category", "branch_name", "barcodes",
                    "brand", "available_quantity", "actual_quantity", "difference"
                ]
                present_cols = [c for c in cols_order if c in df.columns]
                df_to_write = df[present_cols]

                sheet_name = _truncate_sheet_name(brand)
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)

                # collect summary lines
                if set(["name_en", "barcodes", "difference"]).issubset(df_to_write.columns):
                    for _, r in df_to_write.iterrows():
                        summary_rows.append({
                            "Product Name": r.get("name_en"),
                            "Barcode": r.get("barcodes"),
                            "Difference": r.get("difference")
                        })

            # write Summary
            if summary_rows:
                summary_df = pd.DataFrame(summary_rows)
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
            else:
                pd.DataFrame([{"info": "No products included in this report."}]).to_excel(writer, sheet_name="Summary", index=False)

        generated_files.append(out_path)

    return generated_files
