import streamlit as st
from src.io_utils import read_products, read_schedule, allowed_file, check_size
from src.processor import generate_branch_date_files, create_zip_from_paths
from pathlib import Path
import tempfile

st.set_page_config(page_title="Inventory Application", layout="wide")

st.title("Inventory Application")

st.markdown("""
Upload the **Products** file and the **Counting schedule** file.
The app will create one Excel file per (branch, date). Each brand becomes a sheet inside that file.
""")

uploaded_products = st.file_uploader("Upload Products File (csv, xls, xlsx) - max 200MB", type=['csv','xls','xlsx'])
uploaded_schedule = st.file_uploader("Upload Schedule Sheet (csv, xls, xlsx) - max 200MB", type=['csv','xls','xlsx'])

if uploaded_products:
    ok, msg = allowed_file(uploaded_products)
    if not ok:
        st.error(msg)
    else:
        size_ok, size_msg = check_size(uploaded_products, max_mb=200)
        if not size_ok:
            st.error(size_msg)
        else:
            st.success(f"Products file OK: {uploaded_products.name} ({uploaded_products.size if hasattr(uploaded_products,'size') else 'size unknown'})")
            try:
                # show a preview
                preview_df = read_products(uploaded_products, preview=True)
                st.subheader("Products preview (first 5 rows)")
                st.dataframe(preview_df)
            except Exception as e:
                st.error(f"Error reading products: {e}")

if uploaded_schedule:
    ok, msg = allowed_file(uploaded_schedule)
    if not ok:
        st.error(msg)
    else:
        size_ok, size_msg = check_size(uploaded_schedule, max_mb=200)
        if not size_ok:
            st.error(size_msg)
        else:
            st.success(f"Schedule file OK: {uploaded_schedule.name}")
            try:
                preview_sched = read_schedule(uploaded_schedule, preview=True)
                st.subheader("Schedule preview (first 10 rows)")
                st.dataframe(preview_sched)
            except Exception as e:
                st.error(f"Error reading schedule: {e}")

if st.button("Generate Files"):

    if not uploaded_products or not uploaded_schedule:
        st.error("Please upload both files before generating.")
    else:
        with st.spinner("Processing..."):
            # create temp output folder
            out_dir = Path(tempfile.mkdtemp(prefix="inventory_out_"))
            try:
                # read schedule into expanded dataframe
                schedule_df = read_schedule(uploaded_schedule, preview=False)
                # read products as iterator / generator
                products_iter = read_products(uploaded_products, preview=False, chunksize=500_000)
                # returns list of pathlib.Path objects pointing to generated excel files
                file_paths = generate_branch_date_files(products_iter, schedule_df, out_dir)
                if not file_paths:
                    st.warning("No files generated. Check schedule vs products matching.")
                else:
                    zip_path = out_dir / "inventory_results.zip"
                    create_zip_from_paths(file_paths, zip_path)
                    st.success(f"Generated {len(file_paths)} files. Download the ZIP below.")
                    with open(zip_path, "rb") as f:
                        st.download_button("Download results ZIP", data=f, file_name=zip_path.name)
            except Exception as e:
                st.error(f"Processing failed: {e}")

