import pandas as pd

def allowed_file(uploaded_file):
    name = uploaded_file.name.lower()
    if not any(name.endswith(ext) for ext in ['.csv', '.xls', '.xlsx']):
        return False, "Invalid file type. Only CSV, XLS, and XLSX are supported."
    return True, "OK"

def check_size(uploaded_file, max_mb=200):
    try:
        size = len(uploaded_file.getbuffer())
    except:
        size = getattr(uploaded_file, "size", None)

    if size and size > max_mb * 1024 * 1024:
        return False, f"File size exceeds {max_mb}MB."
    return True, "OK"

def read_products(uploaded_file, preview=True, chunksize=300000):
    name = uploaded_file.name.lower()

    if name.endswith('.csv'):
        if preview:
            return pd.read_csv(uploaded_file, nrows=5)
        return pd.read_csv(uploaded_file, chunksize=chunksize, dtype=str)
    else:
        if preview:
            return pd.read_excel(uploaded_file, nrows=5, engine='openpyxl')
        df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
        return iter([df])

def read_schedule(uploaded_file, preview=True):
    name = uploaded_file.name.lower()

    df = pd.read_csv(uploaded_file, dtype=str) if name.endswith('.csv') \
        else pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)

    df.columns = [str(c).strip().lower() for c in df.columns]

    # Required columns
    required = ["branch", "date", "brand"]
    for col in required:
        if col not in df.columns:
            if preview:
                return df.head(10)
            raise ValueError(f"Missing required column: {col}")

    df = df[["branch", "date", "brand"]]

    # Parse date column
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Split brands
    rows = []
    for _, row in df.iterrows():
        if row["date"] is None:
            continue

        brand_cell = str(row["brand"])
        separators = [';', ',', '/']

        for sep in separators:
            if sep in brand_cell:
                brands = [b.strip() for b in brand_cell.split(sep)]
                break
        else:
            brands = [brand_cell.strip()]

        for b in brands:
            if b:
                rows.append({
                    "branch": row["branch"].strip(),
                    "date": row["date"],
                    "brand": b
                })

    out = pd.DataFrame(rows)
    return out.head(10) if preview else out
