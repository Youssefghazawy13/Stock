import pandas as pd
from typing import Iterator
from src.utils import ensure_category_column

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

def read_products(uploaded_file, preview=True, chunksize=300000) -> Iterator[pd.DataFrame]:
    name = uploaded_file.name.lower()
    if name.endswith('.csv'):
        if preview:
            df = pd.read_csv(uploaded_file, nrows=5, dtype=str)
            df = ensure_category_column(df)
            return df
        iterator = pd.read_csv(uploaded_file, chunksize=chunksize, dtype=str)
        for chunk in iterator:
            chunk = ensure_category_column(chunk)
            yield chunk
    else:
        if preview:
            df = pd.read_excel(uploaded_file, nrows=5, engine='openpyxl', dtype=str)
            df = ensure_category_column(df)
            return df
        df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
        df = ensure_category_column(df)
        yield df

def read_schedule(uploaded_file, preview=True):
    import calendar
    import datetime
    from zoneinfo import ZoneInfo

    name = uploaded_file.name.lower()
    df = pd.read_csv(uploaded_file, dtype=str) if name.endswith('.csv') \
        else pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)

    df.columns = [str(c).strip().lower() for c in df.columns]
    required = ["branch", "date", "brand"]
    for col in required:
        if col not in df.columns:
            if preview:
                return df.head(10)
            raise ValueError(f"Missing required column in schedule: {col}")

    # get current month/year in Africa/Cairo
    try:
        tz = ZoneInfo("Africa/Cairo")
    except Exception:
        tz = datetime.timezone.utc
    now_local = datetime.datetime.now(tz)
    current_year = now_local.year
    current_month = now_local.month
    _, days_in_month = calendar.monthrange(current_year, current_month)

    rows = []
    for _, r in df.iterrows():
        raw_branch = r["branch"]
        raw_date = r["date"]
        raw_brand = r["brand"]
        if pd.isna(raw_branch) or pd.isna(raw_date) or pd.isna(raw_brand):
            continue
        raw_date_str = str(raw_date).strip()
        parsed_date = None
        # try parse full date
        try:
            parsed_ts = pd.to_datetime(raw_date_str, errors="coerce")
            if not pd.isna(parsed_ts):
                parsed_date = parsed_ts.date()
        except Exception:
            parsed_date = None
        # if not full date, check day number
        if parsed_date is None:
            try:
                day_num = int(float(raw_date_str))
                if 1 <= day_num <= days_in_month:
                    parsed_date = datetime.date(current_year, current_month, day_num)
                else:
                    parsed_date = None
            except Exception:
                parsed_date = None
        if parsed_date is None:
            continue
        brand_cell = str(raw_brand)
        separators = [';', ',', '/']
        brands = None
        for sep in separators:
            if sep in brand_cell:
                brands = [b.strip() for b in brand_cell.split(sep) if b.strip()]
                break
        if brands is None:
            brands = [brand_cell.strip()]
        for brand in brands:
            rows.append({
                "branch": str(raw_branch).strip(),
                "date": parsed_date,
                "brand": brand
            })
    out = pd.DataFrame(rows)
    return out.head(10) if preview else out
