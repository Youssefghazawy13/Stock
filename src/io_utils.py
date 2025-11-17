# src/io_utils.py
import pandas as pd
from typing import Iterator
from src.utils import ensure_category_column

# -------------------------
# Basic validators
# -------------------------
def allowed_file(uploaded_file):
    name = uploaded_file.name.lower()
    if not any(name.endswith(ext) for ext in ['.csv', '.xls', '.xlsx']):
        return False, "Invalid file type. Only CSV, XLS and XLSX are supported."
    return True, "OK"

def check_size(uploaded_file, max_mb=200):
    try:
        size = len(uploaded_file.getbuffer())
    except Exception:
        size = getattr(uploaded_file, "size", None)
    if size and size > max_mb * 1024 * 1024:
        return False, f"File size exceeds {max_mb}MB."
    return True, "OK"

# -------------------------
# Products reader
# -------------------------
def read_products(uploaded_file, preview=True, chunksize=300000) -> Iterator[pd.DataFrame]:
    """
    Reads a products file. If preview=True returns a small DataFrame for preview.
    If preview=False returns an iterator of DataFrame chunks (CSV) or a single-chunk iterator (Excel).
    The reader will:
      - prefer sheet named 'data' (case-insensitive)
      - otherwise scan sheets and pick the first that contains required product columns
    Required product columns (case-insensitive): name_en, branch_name, barcodes, brand, available_quantity
    The function will call ensure_category_column to produce category.
    """
    name = uploaded_file.name.lower()

    # helper to pick sheet from Excel workbook
    def find_products_sheet_excel(xl):
        # preferred name
        for s in xl.sheet_names:
            if s.strip().lower() == "data":
                return s
        # required cols to detect sheet
        required = {"name_en", "branch_name", "barcodes", "brand", "available_quantity"}
        for s in xl.sheet_names:
            try:
                df_try = xl.parse(s, nrows=3)
                cols = {str(c).strip().lower() for c in df_try.columns}
                if required.issubset(cols):
                    return s
            except Exception:
                continue
        # fallback: return first sheet
        return xl.sheet_names[0] if xl.sheet_names else None

    if name.endswith('.csv'):
        if preview:
            uploaded_file.seek(0)
            # try common separators if comma doesn't work well
            try:
                df = pd.read_csv(uploaded_file, nrows=5, dtype=str)
                df = ensure_category_column(df)
                return df
            except Exception:
                # try semicolon
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, sep=';', nrows=5, dtype=str)
                df = ensure_category_column(df)
                return df
        # streaming large CSVs
        iterator = pd.read_csv(uploaded_file, chunksize=chunksize, dtype=str)
        for chunk in iterator:
            chunk = ensure_category_column(chunk)
            yield chunk
    else:
        # Excel path
        xl = pd.ExcelFile(uploaded_file, engine="openpyxl")
        sheet = find_products_sheet_excel(xl)
        if sheet is None:
            # return empty preview to show helpful message upstream
            if preview:
                return pd.DataFrame()
            else:
                yield pd.DataFrame()
                return
        if preview:
            df = xl.parse(sheet_name=sheet, nrows=5, engine="openpyxl", dtype=str)
            df = ensure_category_column(df)
            return df
        # full read (single chunk)
        df = xl.parse(sheet_name=sheet, engine="openpyxl", dtype=str)
        df = ensure_category_column(df)
        yield df

# -------------------------
# Schedule reader
# -------------------------
def read_schedule(uploaded_file, preview=True):
    """
    Reads a schedule file and returns a DataFrame with columns:
      branch (str), date (datetime.date), brand (str)

    Behavior:
    - Accepts CSV or Excel.
    - For Excel: prefers sheet named 'data' else scans sheets for required columns.
    - Accepts date column as full date or Excel serial number or day number 1-31.
    - Accepts alternate header names for branch/date/brand.
    - For preview=True returns head(10) to show in UI; otherwise returns full DataFrame.
    """
    import calendar
    import datetime
    from zoneinfo import ZoneInfo

    name = uploaded_file.name.lower()

    # helper to detect columns in a DataFrame: normalized set
    def normalized_cols(df):
        return {str(c).strip().lower() for c in df.columns}

    # Load candidates (for CSV we just attempt read; for Excel we scan sheets)
    if name.endswith('.csv'):
        # try comma then semicolon if comma results in single-column
        uploaded_file.seek(0)
        try:
            df_try = pd.read_csv(uploaded_file, nrows=5, dtype=str)
            if df_try.shape[1] == 1 and ";" in df_try.columns[0]:
                # likely semicolon-separated without proper parse
                uploaded_file.seek(0)
                df_try = pd.read_csv(uploaded_file, sep=';', nrows=5, dtype=str)
        except Exception as e:
            # return helpful error in preview
            if preview:
                raise ValueError(f"Unable to read schedule CSV for preview: {e}")
            raise
        candidate_df = df_try  # small sample; we'll re-read full if needed
        chosen_reader = "csv"
    else:
        # Excel: find best sheet
        xl = pd.ExcelFile(uploaded_file, engine="openpyxl")
        chosen_sheet = None
        # prefer 'data'
        for s in xl.sheet_names:
            if s.strip().lower() == "data":
                chosen_sheet = s
                break
        # otherwise scan for sheet containing date/branch/brand candidates
        if chosen_sheet is None:
            for s in xl.sheet_names:
                try:
                    df_try = xl.parse(s, nrows=5, engine="openpyxl", dtype=str)
                except Exception:
                    continue
                cols = normalized_cols(df_try)
                # quick check for at least two of required identifiers
                if {"branch", "brand"}.issubset(cols) or {"date", "brand"}.issubset(cols) or {"branch", "date"}.issubset(cols):
                    chosen_sheet = s
                    break
        if chosen_sheet is None:
            # fallback to first sheet
            chosen_sheet = xl.sheet_names[0] if xl.sheet_names else None

        if chosen_sheet is None:
            if preview:
                return pd.DataFrame()
            else:
                raise ValueError("Schedule file has no sheets.")
        # parse chosen sheet (sample)
        try:
            candidate_df = xl.parse(sheet_name=chosen_sheet, nrows=5, engine="openpyxl", dtype=str)
        except Exception as e:
            if preview:
                raise ValueError(f"Unable to parse schedule Excel sheet '{chosen_sheet}' for preview: {e}")
            raise
        chosen_reader = ("excel", chosen_sheet)

    # Normalize column names for detection
    candidate_df.columns = [str(c).strip() for c in candidate_df.columns]
    norm_cols = normalized_cols(candidate_df)

    # Candidates for mapping
    branch_candidates = ["branch", "branch_name", "store", "location"]
    date_candidates = ["date", "day", "day_number", "daynum", "day_no", "day_of_month"]
    brand_candidates = ["brand", "brands", "brand_name", "vendor", "vendors"]

    def find_col_in_norm(cands):
        for c in cands:
            if c in norm_cols:
                # return the actual candidate as present in candidate_df (case sensitive)
                for real in candidate_df.columns:
                    if str(real).strip().lower() == c:
                        return real
        return None

    branch_col = find_col_in_norm(branch_candidates)
    date_col = find_col_in_norm(date_candidates)
    brand_col = find_col_in_norm(brand_candidates)

    if date_col is None or branch_col is None or brand_col is None:
        # For preview, show detected columns to help user fix file
        if preview:
            raise ValueError(
                "Schedule file missing required columns. Detected columns: "
                f"{list(candidate_df.columns)}. Please include columns for branch, date and brand "
                "(accepted date alternatives: day, day_number; branch alternatives: branch_name; brand alternatives: brands)."
            )
        else:
            raise ValueError(
                "Schedule file missing required columns. Detected columns: "
                f"{list(candidate_df.columns)}."
            )

    # Read the full DataFrame now using the detected sheet/format
    if chosen_reader == "csv":
        uploaded_file.seek(0)
        try:
            df_full = pd.read_csv(uploaded_file, dtype=str)
        except Exception:
            uploaded_file.seek(0)
            df_full = pd.read_csv(uploaded_file, sep=';', dtype=str)
    else:
        # chosen_reader = ("excel", sheet_name)
        sheet = chosen_reader[1]
        uploaded_file.seek(0)
        df_full = pd.read_excel(uploaded_file, sheet_name=sheet, engine="openpyxl", dtype=str)

    # Normalize headers now and rename chosen cols to standard names
    df_full.columns = [str(c).strip() for c in df_full.columns]
    # map actual header names to normalized keys for easier use
    actual_branch_col = None
    actual_date_col = None
    actual_brand_col = None
    for real in df_full.columns:
        low = str(real).strip().lower()
        if low in branch_candidates and actual_branch_col is None:
            actual_branch_col = real
        if low in date_candidates and actual_date_col is None:
            actual_date_col = real
        if low in brand_candidates and actual_brand_col is None:
            actual_brand_col = real

    # rename to standard names
    df_full = df_full.rename(columns={
        actual_branch_col: "branch",
        actual_date_col: "date",
        actual_brand_col: "brand"
    })

    # Convert Excel serial numbers or day numbers to actual dates
    try:
        tz = ZoneInfo("Africa/Cairo")
    except Exception:
        tz = datetime.timezone.utc
    now_local = datetime.datetime.now(tz)
    cur_year = now_local.year
    cur_month = now_local.month
    _, days_in_month = calendar.monthrange(cur_year, cur_month)

    rows = []
    for _, r in df_full.iterrows():
        raw_branch = r.get("branch")
        raw_date = r.get("date")
        raw_brand = r.get("brand")
        if pd.isna(raw_branch) or pd.isna(raw_date) or pd.isna(raw_brand):
            continue
        raw_date_str = str(raw_date).strip()

        parsed_date = None

        # Try parsing as standard date
        parsed_ts = pd.to_datetime(raw_date_str, errors="coerce", dayfirst=False)
        if not pd.isna(parsed_ts):
            parsed_date = parsed_ts.date()

        # If still not parsed, try Excel serial (integer)
        if parsed_date is None:
            try:
                # Excel serials are integers like 45978
                serial = int(float(raw_date_str))
                # pandas.to_datetime can convert from ordinal by specifying unit='D' from '1899-12-30' epoch
                # but here we do manual conversion:
                # Excel's serial day 1 = 1899-12-31 on Windows (but pandas uses 1899-12-30 anchor)
                parsed_date = (pd.Timestamp("1899-12-30") + pd.to_timedelta(serial, unit="D")).date()
            except Exception:
                parsed_date = None

        # If still not parsed, maybe it's a day number (1-31) for current month
        if parsed_date is None:
            try:
                day_num = int(float(raw_date_str))
                if 1 <= day_num <= days_in_month:
                    parsed_date = datetime.date(cur_year, cur_month, day_num)
            except Exception:
                parsed_date = None

        if parsed_date is None:
            # skip unparsable entries
            continue

        # Expand multiple brands in the cell
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
