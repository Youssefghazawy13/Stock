# tests/test_processor.py
"""
Robust test file that:
- Locates repository root
- Finds a processor.py file anywhere under the repo (first match)
- Loads processor.py as a module via importlib (no need for src package on PYTHONPATH)
- Uses the functions from the loaded module for testing.
"""

from pathlib import Path
import importlib.util
import sys
import pandas as pd
import datetime
from zoneinfo import ZoneInfo

# --- locate repo root (walk up until we find a sensible boundary) ---
here = Path(__file__).resolve()
repo_root = None
for parent in [here] + list(here.parents):
    # consider this a repo root if it contains .git, app.py, or tests folder sibling
    if (parent / ".git").exists() or (parent / "app.py").exists() or (parent / "tests").exists():
        repo_root = parent
        break
# fallback
if repo_root is None:
    repo_root = here.parents[2]

# --- find processor.py under repo root (search depth is allowed by rglob) ---
processor_path = None
for p in repo_root.rglob("processor.py"):
    # prefer one under a folder named 'src' if found
    if "src" in [part.lower() for part in p.parts]:
        processor_path = p
        break
    if processor_path is None:
        processor_path = p

if processor_path is None:
    raise RuntimeError("Could not find processor.py in repository. Ensure file exists under repo root.")

# --- load the module from file (dynamic import) ---
spec = importlib.util.spec_from_file_location("project_processor", str(processor_path))
if spec is None or spec.loader is None:
    raise RuntimeError(f"Cannot load module from {processor_path}")
module = importlib.util.module_from_spec(spec)
sys.modules["project_processor"] = module
spec.loader.exec_module(module)

# Get functions from loaded module
generate_branch_date_files = getattr(module, "generate_branch_date_files")
create_zip_from_paths = getattr(module, "create_zip_from_paths")

# --- helper test data factories ---
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

# --- tests ---
def test_generate_branch_date_files_and_summary(tmp_path):
    products_df = make_products_df()
    products_iter = iter([products_df])  # single-chunk iterator
    schedule_df = make_schedule_df_for_today()

    outdir = tmp_path / "out"
    outdir.mkdir()

    generated = generate_branch_date_files(products_iter, schedule_df, outdir)

    assert isinstance(generated, list)
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
