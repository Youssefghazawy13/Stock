import streamlit as st
from src.io_utils import read_products, read_schedule, allowed_file, check_size
from src.processor import generate_branch_date_files, create_zip_from_paths
from pathlib import Path
import tempfile

st.set_page_config(page_title="Stock Counting Application", layout="wide")

st.title("Stock Counting Application")

st.markdown("""
Upload the **Products File** and the **Counting Schedule File**.

The app will generate:
- One Excel file per **(Branch, Date)** schedule entry  
- One sheet per **Brand** inside each Excel file  
- A ZIP archive containing all generated Excel files  
""")

uploaded_products = st.file_uploader(
    "Upload Products File (CSV, XLS, XLSX) – max 200MB",
    type=['csv', 'xls', 'xlsx']
)
uploaded_schedule = st.file_uploader(
    "Upload Schedule File (CSV, XLS, XLSX) – max 200MB",
    type=['csv', 'xls', 'xlsx']
)

# -----------------------------------------------------------
# PRODUCTS FILE PREVIEW
# -----------------------------------------------------------

if uploaded_products:
    ok, msg = allowed_file(uploaded_products)
    if not ok:
        st.error(msg)
    else:
        size_ok, size_msg = check_size(uploaded_products, max_mb=200)
        if not size_ok:
            st.error(size_msg)
        else:
            try:
                preview = read_products(uploaded_products, preview=True)
                st.subheader("Products Preview (first 5 rows)")
                st.dataframe(preview)
                st.success("Products file loaded successfully")
            except Exception as e:
                st.error(f"Error reading products file: {e}")

# -----------------------------------------------------------
# SCHEDULE FILE PREVIEW
# -----------------------------------------------------------

if uploaded_schedule:
    ok, msg = allowed_file(uploaded_schedule)
    if not ok:
        st.error(msg)
    else:
        size_ok, size_msg = check_size(uploaded_schedule, max_mb=200)
        if not size_ok:
            st.error(size_msg)
        else:
            try:
                preview = read_schedule(uploaded_schedule, preview=True)
                st.subheader("Schedule Preview (first 10 rows)")
                st.dataframe(preview)
                st.success("Schedule file loaded successfully")
            except Exception as e:
                st.error(f"Error reading schedule file: {e}")

# -----------------------------------------------------------
# GENERATE OUTPUT FILES
# -----------------------------------------------------------

if st.button("Generate Reports"):

    if not uploaded_products or not uploaded_schedule:
        st.error("Please upload BOTH files.")
    else:
        with st.spinner("Generating files... Please wait..."):
            out_dir = Path(tempfile.mkdtemp(prefix="stock_reports_"))

            try:
                schedule_df = read_schedule(uploaded_schedule, preview=False)
                products_iter = read_products(uploaded_products, preview=False)

                generated_files = generate_branch_date_files(
                    products_iter,
                    schedule_df,
                    out_dir
                )

                if not generated_files:
                    st.warning("No reports were generated. Check your files.")
                else:
                    zip_path = out_dir / "Stock_Reports.zip"
                    create_zip_from_paths(generated_files, zip_path)

                    st.success(f"Generated {len(generated_files)} files.")
                    with open(zip_path, "rb") as f:
                        st.download_button(
                            label="Download All Reports (ZIP)",
                            data=f,
                            file_name="Stock_Reports.zip"
                        )

            except Exception as e:
                st.error(f"An error occurred during processing: {e}")
