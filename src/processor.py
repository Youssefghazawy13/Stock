from pathlib import Path
import pandas as pd
from src.utils import normalize_columns, validate_product_columns, coerce_quantity
import zipfile

def create_zip_from_paths(paths, zip_path):
    with zipfile.ZipFile(zip_path, "w") as z:
        for p in paths:
            z.write(p, arcname=p.name)

def generate_branch_date_files(products_iter, schedule_df, output_dir):

    output_dir.mkdir(parents=True, exist_ok=True)

    schedule_df = schedule_df.copy()
    schedule_df["branch_norm"] = schedule_df["branch"].str.strip().str.lower()
    schedule_df["brand_norm"] = schedule_df["brand"].str.strip().str.lower()
    schedule_df["date_str"] = schedule_df["date"].apply(lambda d: d.strftime("%d-%m-%Y"))

    schedule_map = {}
    for _, row in schedule_df.iterrows():
        key = (row["branch_norm"], row["date_str"])
        schedule_map.setdefault(key, set()).add(row["brand"])

    accum = {}

    for chunk in products_iter:
        chunk = normalize_columns(chunk)
        validate_product_columns(chunk)
        chunk = coerce_quantity(chunk)

        chunk["branch_norm"] = chunk["branch_name"].str.strip().str.lower()
        chunk["brand_norm"] = chunk["brand"].str.strip().str.lower()

        for key, brand_set in schedule_map.items():
            branch_norm, _ = key

            branch_rows = chunk[chunk["branch_norm"] == branch_norm]

            for brand in brand_set:
                brand_norm = brand.strip().lower()

                matched = branch_rows[branch_rows["brand_norm"] == brand_norm]
                if matched.empty:
                    continue

                accum.setdefault(key, {})
                accum[key].setdefault(brand, [])
                accum[key][brand].extend(matched.to_dict(orient="records"))

    generated = []

    for (branch_norm, date_str), brand_dict in schedule_map.items():
        filename = f"{branch_norm}_{date_str}.xlsx".replace(" ", "_")
        file_path = Path(output_dir / filename)

        writer = pd.ExcelWriter(file_path, engine="xlsxwriter")

        if (branch_norm, date_str) in accum:
            for brand, rows in accum[(branch_norm, date_str)].items():
                df = pd.DataFrame(rows)
                df = normalize_columns(df)
                df = coerce_quantity(df)

                sheet_name = str(brand)[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            pd.DataFrame([{"error": "No matching data"}]).to_excel(
                writer, sheet_name="ERROR", index=False
            )

        writer.close()
        generated.append(file_path)

    return generated
