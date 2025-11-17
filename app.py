import streamlit as st
from pathlib import Path
import tempfile
import datetime
from zoneinfo import ZoneInfo

from src.io_utils import read_products, read_schedule, allowed_file, check_size
from src.processor import generate_branch_date_files, create_zip_from_paths
from src.utils import ensure_category_column

st.set_page_config(page_title="Stock Counting Application", layout="wide")
st.title("Stock Counting Application")
st.markdown(
    "Upload a Products file and a Counting Schedule. The app will generate reports for **today's date** "
    "(Africa/Cairo timezone) and provide a ZIP download of the generated Excel files."
)

uploaded_products = st.file_uploader(
    "Upload Products File (CSV, XLS, XLSX) – max 200MB",
    type=['csv', 'xls', 'xlsx']
)
uploaded_schedule = st.file_uploader(
    "Upload Schedule File (CSV, XLS, XLSX) – max 200MB",
    type=['csv', 'xls', 'xlsx']
)

# show previews
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
                st.subheader("Products preview (first rows)")
                st.dataframe(preview)
                st.success("Products file loaded for preview.")
            except Exception as e:
                st.error(f"Error reading products file: {e}")

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
                st.subheader("Schedule preview (first rows)")
                st.dataframe(preview)
                st.success("Schedule file loaded for preview.")
            except Exception as e:
                st.error(f"Error reading schedule file: {e}")

# Generate today's reports
if st.button("Generate Today's Reports"):

    if not uploaded_products or not uploaded_schedule:
        st.error("Please upload BOTH files before generating reports.")
    else:
        with st.spinner("Preparing today's reports..."):
            out_dir = Path(tempfile.mkdtemp(prefix="stock_reports_"))

            try:
                # read full schedule expanded
                schedule_df = read_schedule(uploaded_schedule, preview=False)

                # compute today's date in Africa/Cairo
                try:
                    tz = ZoneInfo("Africa/Cairo")
                except Exception:
                    tz = datetime.timezone.utc
                today_local = datetime.datetime.now(tz).date()

                # filter schedule to today only
                schedule_for_today = schedule_df[schedule_df["date"] == today_local]

                st.subheader(f"Schedule entries for today: {today_local}")
                if schedule_for_today.empty:
                    st.warning(f"No scheduled entries match today's date: {today_local}. No reports generated.")
                else:
                    st.dataframe(schedule_for_today)

                    # show a small preview of matched products (first chunk)
                    try:
                        products_iter = read_products(uploaded_products, preview=False)
                        first_chunk = next(products_iter)
                        preview_products = ensure_category_column(first_chunk)
                        branches = schedule_for_today["branch"].str.strip().str.lower().unique().tolist()
                        brands = schedule_for_today["brand"].str.strip().str.lower().unique().tolist()
                        preview_matched = preview_products[
                            preview_products["branch_name"].astype(str).str.strip().str.lower().isin(branches) &
                            preview_products["brand"].astype(str).str.strip().str.lower().isin(brands)
                        ]
                        st.subheader("Preview of matched products (up to 100 rows)")
                        st.dataframe(preview_matched.head(100))
                        # rewind: recreate products_iter for full processing (re-open)
                        products_iter = read_products(uploaded_products, preview=False)
                    except StopIteration:
                        st.info("Products file appears empty.")
                        products_iter = read_products(uploaded_products, preview=False)
                    except Exception as e:
                        st.warning(f"Could not preview matched products: {e}")
                        products_iter = read_products(uploaded_products, preview=False)

                    generated_files = generate_branch_date_files(
                        products_iter,
                        schedule_for_today,
                        out_dir
                    )

                    if not generated_files:
                        st.warning("No reports were generated. Check your files and mappings.")
                    else:
                        zip_path = out_dir / f"Stock_Reports_{today_local.strftime('%d-%m-%Y')}.zip"
                        create_zip_from_paths(generated_files, zip_path)

                        st.success(f"Generated {len(generated_files)} file(s) for date {today_local}.")
                        with open(zip_path, "rb") as f:
                            st.download_button(
                                label="Download Today's Reports (ZIP)",
                                data=f,
                                file_name=zip_path.name
                            )

            except Exception as e:
                st.error(f"An error occurred during processing: {e}")
