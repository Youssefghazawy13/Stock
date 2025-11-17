# tests/test_processor.py
# Ensure repo root is on sys.path so `import src` works in CI environments
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import pandas as pd
from src.processor import generate_branch_date_files, create_zip_from_paths
import datetime
from zoneinfo import ZoneInfo

def make_products_df():
    """
    Create an in-memory products DataFrame for testing.
    Uses neutral placeholder values generated at runtime (no committed fixtures).
    """
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
    """
    Create a schedule DataFrame with rows for today's date (Africa/Cairo).
    """
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
    products_iter = iter([products_df])  # single-chunk iterator
    schedule_df = make_schedule_df_for_today()

    outdir = tmp_path / "out"
    outdir.mkdir()

    generated = generate_branch_date_files(products_iter, schedule_df, outdir)

    # Expect at least one generated file
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

    assert z.exists()
    assert z.stat().st_size > 0
