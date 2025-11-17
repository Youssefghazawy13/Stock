import pandas as pd
from io import BytesIO
import numpy as np
from typing import Iterator, Optional
import streamlit as st
import datetime

def allowed_file(uploaded_file) -> (bool, str):
    name = uploaded_file.name.lower()
    if not any(name.endswith(ext) for ext in ('.csv', '.xls', '.xlsx')):
        return False, "Unsupported file type. Use csv, xls or xlsx."
    return True, "ok"

def check_size(uploaded_file, max_mb=200):
    # Streamlit's UploadedFile may have getbuffer
    try:
        size = len(uploaded_file.getbuffer())
    except Exception:
        # fallback if unavailable
        size = getattr(uploaded_file, "size", None)
    if size is None:
        return True, "size unknown"
    if size > max_mb * 1024 * 1024:
        return False, f"File is larger than {max_mb} MB ({size / (1024*1024):.1f} MB)."
    return True, "ok"

def read_products(uploaded_file, preview: bool = True, chunksize: int = 500_000):
    """
    If preview=True return a small DataFrame (first 5 rows).
    Otherwise return an iterator of DataFrames (for csv) or a single DataFrame (for excel).
    """
    name = uploaded_file.name.lower()
    if name.endswith('.csv'):
        # read small preview or return iterator
        if preview:
            df = pd.read_csv(uploaded_file, nrows=5)
            return df
        else:
            # return iterator of df chunks
            return pd.read_csv(uploaded_file, chunksize=chunksize, dtype=str)
    else:
        # excel formats
        if preview:
            df = pd.read_excel(uploaded_file, nrows=5, engine='openpyxl')
            return df
        else:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
            # return as single iterator with one chunk for compatibility
            return iter([df])

def read_schedule(uploaded_file, preview: bool = True) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith('.csv'):
        df = pd.read_csv(uploaded_file, dtype=str)
    else:
        df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
    # normalize columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    # try to find branch/date/brand columns
    branch_col = next((c for c in df.columns if c in ('branch', 'branch_name', 'branch name')), None)
    date_col = next((c for c in df.columns if c in ('date','day')), None)
    brand_col = next((c for c in df.columns if c in ('brand','brands')), None)
    if not branch_col or not date_col or not brand_col:
        # preview should show what's available
        if preview:
            return df.head(10)
        raise ValueError("Schedule sheet must contain columns named branch, date, and brand (case-insensitive). Found: " + ", ".join(df.columns))
    df = df[[branch_col, date_col, brand_col]].rename(columns={branch_col: 'branch', date_col: 'date', brand_col: 'brand'})
    # parse date column robustly
    def parse_date_cell(x):
        if pd.isna(x):
            return None
        x = str(x).strip()
        # try ISO
        for fmt in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%m/%d/%Y"):
            try:
                return pd.to_datetime(x, format=fmt).date()
            except Exception:
                pass
        # try pandas generic parse
        try:
            return pd.to_datetime(x).date()
        except Exception:
            # maybe Excel serial
            try:
                val = float(x)
                # Excel serial to date: 1899-12-30 start (pandas uses origin)
                return (pd.to_datetime('1899-12-30') + pd.to_timedelta(val, unit='D')).date()
            except Exception:
                return None
    df['date'] = df['date'].apply(parse_date_cell)
    # split brands into multiple rows
    def split_brands(cell):
        if pd.isna(cell):
            return []
        s = str(cell)
        for sep in [';',',','/']:
            if sep in s:
                parts = [p.strip() for p in s.split(sep) if p.strip()]
                if parts:
                    return parts
        # single brand
        return [s.strip()]
    rows = []
    for _, r in df.iterrows():
        date = r['date']
        branch = r['branch']
        brands = split_brands(r['brand'])
        if not branch or not date:
            continue
        for b in brands:
            rows.append({'branch': str(branch).strip(), 'date': date, 'brand': str(b).strip()})
    out = pd.DataFrame(rows)
    if preview:
        return out.head(10)
    return out

