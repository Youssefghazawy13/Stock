# tests/test_processor.py
"""
Bootstrap to make local src importable during tests.
This locates the repository root (folder containing 'src' or 'app.py')
and prepends it and repo_root/src to sys.path before importing project modules.
"""

import sys
import os
from pathlib import Path

# locate repo root by walking up until we find 'src' directory or 'app.py'
here = Path(__file__).resolve()
repo_root = None
for parent in [here] + list(here.parents):
    if (parent / "src").is_dir() or (parent / "app.py").is_file():
        repo_root = parent
        break

if repo_root is None:
    # fallback: two levels up
    repo_root = here.parents[2]

repo_root_str = str(repo_root)
src_dir = str(repo_root / "src")

# Add repo root and src directory to sys.path
if repo_root_str not in sys.path:
    sys.path.insert(0, repo_root_str)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# --- safe to import project modules now ---
import pandas as pd
from src.processor import generate_branch_date_files, create_zip_from_paths
import datetime
from zoneinfo import ZoneInfo

def make_products_df():
    data = {
        "name_en": [f"Product_{i}" for i in range(1, 6)],
        "branch_name": ["Branch_A", "Branch_A", "Branch_B", "Branch_A", "Branch_B"],
        "barcodes": [f"100000000000{i}" for i in range(1, 6)],
        "brand": ["Brand_1", "Brand_2", "Brand_1", "Brand_2", "Brand_3"],
        "available_quantity": [5, 3, 10, 0, 7],
        "actual_quantity": [6, 1, 9, 2, 8],
    }
    return pd.DataFrame(data)

def make_schedule_df_for_today():
    try:
        tz = ZoneInfo("Africa/Cairo")
    except Exception:
        tz = datetime.timezone.utc
    today = datetime.datetime.now(tz).date()
    rows = [
        {"branch": "Branch_A", "date": today, "brand": "Brand_1"},
        {"branch": "Branch_A", "date": today, "brand": "Brand_2"},
        {"branch": "Branch_B", "date": today, "brand": "Brand_3"},
    ]
    return pd.DataFrame(rows)

def test_generate_branch_date_files_and_summary(tmp_path):
    products_df = make_products_df()
    products_iter = iter([products_df])
    schedule_df = make_schedule_df_for_today()

    outdir = tmp_path / "out"
    outdir.mkdir()
    generated = generate_branch_date_files(products_iter, schedule_df, outdir)

    assert len(generated) >= 1
    first = generated[0]
    assert first.exists()
    xls = pd.ExcelFile(first)
    assert "Summary" in xls.sheet_names
    summary = pd.read_excel(first, sheet_name="Summary")
    assert set(["Product Name", "Barcode", "Difference"]).issubset(set(summary.columns))

def test_create_zip_from_paths(tmp_path):
    f1 = tmp_path / "a.xlsx"
    f1.write_text("dummy")
    z = tmp_path / "out.zip"
    create_zip_from_paths([f1], z)
    assert z.exists()
    assert z.stat().st_size > 0
