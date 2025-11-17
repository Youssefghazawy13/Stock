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
    """
    Returns:
      - if preview=True: a small DataFrame for preview (first 5 rows), with computed 'category' column.
      - else: iterator of DataFrame chunks (for csv) or an iterator with a single DataFrame (for excel).
    """
    name = uploaded_file.name.lower()

    if name.endswith('.csv'):
        if preview:
            df = pd.read_csv(uploaded_file, nrows=5, dtype=str)
            df = ensure_category_column(df)
            return df
        # stream in chunks, ensure category in each chunk
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
        # return a single-chunk iterator for compatibility
        yield df
