import pandas as pd
from pathlib import Path
from src.processor import generate_branch_date_files, create_zip_from_paths
from io import StringIO

def make_products_csv():
    data = """name_ar,name_en,branch_name,barcodes,brand,available_quantity
قميص,T-Shirt,Maadi,123,THE G BASICS,10
بنطلون,Jeans,Maadi,456,THE G STUDIOS,5
حذاء,Shoe,Madinaty,789,OTHER,2
"""
    return StringIO(data)

def test_generate_files(tmp_path):
    products_iter = pd.read_csv(make_products_csv(), chunksize=1000, dtype=str)
    # schedule: Maadi 20/09/2025 contains two brands
    schedule = pd.DataFrame([
        {'branch': 'Maadi', 'date': pd.to_datetime('2025-09-20').date(), 'brand': 'THE G BASICS; THE G STUDIOS'}
    ])
    out = tmp_path / "out"
    out.mkdir()
    generated = generate_branch_date_files(products_iter, schedule, out)
    assert len(generated) == 1
    assert any("Maadi" in p.name for p in generated)

def test_create_zip(tmp_path):
    f1 = tmp_path / "a.txt"
    f1.write_text("hello")
    z = tmp_path / "test.zip"
    create_zip_from_paths([f1], z)
    assert z.exists()

