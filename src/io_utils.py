# src/io_utils.py
import pandas as pd
from typing import Iterator
from src.utils import ensure_category_column

def allowed_file(uploaded_file):
    """
    Validate file extension.
    """
    name = uploaded_file.name.lower()
    if not any(name.endswith(ext) for ext in ['.csv', '.xls', '.xlsx']):
        return False, "Invalid file type. Only CSV, XLS, and XLSX are supported."
    return True, "OK"

def check_size(uploaded_file, max_mb=200):
    """
    Check uploaded file size (best-effort).
    """
    try:
        size = len(uploaded_file.getbuffer())
    except Exception:
        size = getattr(uploaded_file, "size", None)
    if size and size > max_mb * 1024 * 1024:
        return False, f"File size exceeds {max_mb}MB."
    return True, "OK"

def read_products(uploaded_file, preview=True, chunksize=300000) -> Iterator[pd.DataFrame]:
    """
    Read products file. Returns a small DataFrame for preview (if preview=True)
    or an iterator of DataFrame chunks (for CSV) / single DataFrame iterator (for Excel).
    Ensures the category column exists and is placed after name_en.
    """
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
        # xls / xlsx
        if preview:
            df = pd.read_excel(uploaded_file, nrows=5, engine='openpyxl', dtype=str)
            df = ensure_category_column(df)
            return df
        df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
        df = ensure_category_column(df)
        yield df

def read_schedule(uploaded_file, preview=True):
    """
    Read and normalize the schedule file.

    Accepts CSV or Excel. Normalizes headers (stripped, lowercased) and
    detects common alternative names for branch/date/brand. Parses full dates
    or day numbers (1..31) mapping to current month/year in Africa/Cairo.
    Expands cells with multiple brands separated by ; , or / into multiple rows.

    Returns a DataFrame with columns: branch (str), date (datetime.date), brand (str).
    If preview=True returns head(10) for quick UI preview, otherwise returns full DataFrame.
    """
    import calendar
    import datetime
    from zoneinfo import ZoneInfo

    name = uploaded_file.name.lower()
    df = pd.read_csv(uploaded_file, dtype=str) if name.endswith('.csv') \
        else pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)

    # preserve original headers for helpful error messages
    original_cols = list(df.columns)

    # normalize column names by stripping and lowercasing
    df.columns = [str(c).strip().lower() for c in df.columns]

    # helper to find a column among candidates
    def find_col(candidates):
        for cand in candidates:
            if cand in df.columns:
                return cand
        return None

    # common alternative names
    branch_candidates = ["branch", "branch_name", "store", "location"]
    date_candidates = ["date", "day", "day_number", "daynum", "day_no", "daynumber", "day_of_month"]
    brand_candidates = ["brand", "brands", "brand_name", "vendor", "vendors"]

    branch_col = find_col(branch_candidates)
    date_col = find_col(date_candidates)
    brand_col = find_col(brand_candidates)

    if date_col is None or branch_col is None or brand_col is None:
        # helpful error for the UI
        msg = (
            "Schedule file missing required columns.\n"
            f"Found columns: {original_cols}\n"
            "Required (or acceptable alternatives):\n"
            "- branch (branch_name, store, location)\n"
            "- date (day, day_number, day_of_month)\n"
            "- brand (brands, brand_name, vendor)\n"
            "Please rename your headers or upload a file with these columns."
        )
        if preview:
            # for preview mode raise ValueError so Streamlit shows the message
            raise ValueError(msg)
        else:
            raise ValueError(msg)

    # rename to standard names
    df = df.rename(columns={
        branch_col: "branch",
        date_col: "date",
        brand_col: "brand"
    })

    # compute current month/year in Africa/Cairo
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
        raw_branch = r.get("branch")
        raw_date = r.get("date")
        raw_brand = r.get("brand")
        if pd.isna(raw_branch) or pd.isna(raw_date) or pd.isna(raw_brand):
            continue
        raw_date_str = str(raw_date).strip()
        parsed_date = None

        # try parse full date (many formats)
        parsed_ts = pd.to_datetime(raw_date_str, errors="coerce", dayfirst=False)
        if not pd.isna(parsed_ts):
            parsed_date = parsed_ts.date()

        # if not a full date, check if it's a day number
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
            # skip rows with unparsable date (we could log or collect them if needed)
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
