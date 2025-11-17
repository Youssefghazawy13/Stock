def generate_branch_date_files(products_iter, schedule_df, output_dir: Path):
    """
    Robust version:
    - consumes products_iter into list of chunks (so we can inspect branch list)
    - normalizes branch names (strip/lower/remove non-alnum) for tolerant matching
    - if schedule branch not found exactly, tries to find closest candidate
    - writes Excel files with blank actual_quantity and formula difference as before
    """
    import pandas as pd
    import re
    # helper: normalize branch for comparison
    def normalize_text(s):
        if s is None:
            return ""
        s = str(s).strip().lower()
        # remove non-alphanumeric characters (keep letters and numbers)
        s = re.sub(r'[^0-9a-z]+', '', s)
        return s

    # collect all product chunks into memory (needed to inspect branch names)
    chunks = []
    try:
        for c in products_iter:
            if isinstance(c, pd.DataFrame):
                chunks.append(c.copy())
            else:
                # if something else returned, skip
                continue
    except TypeError:
        # products_iter might be a DataFrame (not iterator)
        if isinstance(products_iter, pd.DataFrame):
            chunks = [products_iter.copy()]
        else:
            # unexpected, treat as empty
            chunks = []

    all_products = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    # Normalize column names to expected form
    all_products.columns = [str(c).strip() for c in all_products.columns]

    # Ensure category and required columns exist (use your helper)
    try:
        all_products = ensure_category_column(all_products)
    except Exception:
        pass

    # Map normalized branch -> list of original branch names found
    all_products['branch_norm_exact'] = all_products.get('branch_name', "").astype(str).str.strip()
    all_products['branch_norm_key'] = all_products['branch_norm_exact'].apply(normalize_text)
    branch_map = {}
    for orig, key in zip(all_products['branch_norm_exact'], all_products['branch_norm_key']):
        branch_map.setdefault(key, set()).add(orig)

    # helper to find best matching original branch name for a schedule branch
    def find_best_branch(orig_branch):
        k = normalize_text(orig_branch)
        if not k:
            return None
        # exact normalized match
        if k in branch_map:
            # return one representative original (choose arbitrary)
            return list(branch_map[k])[0]
        # try substring or startswith matching against keys
        for candidate_key, originals in branch_map.items():
            if candidate_key and (k in candidate_key or candidate_key in k or candidate_key.startswith(k) or k.startswith(candidate_key)):
                return list(originals)[0]
        # no match
        return None

    # Now prepare schedule mapping: prefer normalized matching
    schedule = schedule_df.copy()
    schedule['branch_norm_raw'] = schedule['branch'].astype(str).str.strip()
    schedule['branch_match'] = schedule['branch_norm_raw'].apply(find_best_branch)
    # if any schedule row couldn't be matched, branch_match will be None

    # Build schedule_map using branch_match (original branch names as in products)
    schedule_map = {}
    for _, r in schedule.iterrows():
        matched_branch = r.get('branch_match')
        date_str = r['date'].strftime("%d-%m-%Y") if not pd.isna(r['date']) else None
        if matched_branch is None or date_str is None:
            continue
        key = (matched_branch.strip().lower(), date_str)
        schedule_map.setdefault(key, set()).add(r['brand'])

    # If schedule_map is empty after matching, attempt fallback: use schedule raw branch names
    if not schedule_map:
        for _, r in schedule.iterrows():
            date_str = r['date'].strftime("%d-%m-%Y") if not pd.isna(r['date']) else None
            if date_str is None:
                continue
            key = (str(r['branch']).strip().lower(), date_str)
            schedule_map.setdefault(key, set()).add(r['brand'])

    # proceed as previous implementation but using all_products (not consumed iterator)
    generated_files = []
    output_dir.mkdir(parents=True, exist_ok=True)

    # group products by branch and brand for fast lookup
    all_products['brand_norm_key'] = all_products.get('brand', "").astype(str).str.strip().str.lower()
    all_products['branch_norm_key2'] = all_products.get('branch_name', "").astype(str).str.strip().str.lower()

    # build an index: (branch_norm_key2, brand_norm_key) -> DataFrame slice
    grouped = {}
    for idx, row in all_products.iterrows():
        b = row['branch_norm_key2']
        br = row['brand_norm_key']
        grouped.setdefault((b, br), []).append(row.to_dict())

    # generate files
    for (branch_norm, date_str), brand_set in schedule_map.items():
        # find original branch display name from products
        sample_rows = all_products[all_products['branch_name'].astype(str).str.strip().str.lower() == branch_norm]
        original_branch = sample_rows['branch_name'].iloc[0] if not sample_rows.empty else branch_norm
        safe_branch = str(original_branch).replace(" ", "_")
        filename = f"{safe_branch}_{date_str}.xlsx"
        out_path = output_dir / filename

        # open writer and write sheets similar to previous logic
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book
            summary_entries = []
            for brand in brand_set:
                bnorm = str(brand).strip().lower()
                rows = grouped.get((branch_norm, bnorm), [])
                if not rows:
                    # try approximate brand match among keys
                    candidate_rows = []
                    for (bk, brk), recs in grouped.items():
                        if bk == branch_norm and (bnorm in brk or brk in bnorm):
                            candidate_rows.extend(recs)
                    rows = candidate_rows
                if not rows:
                    continue
                df = pd.DataFrame(rows)
                # ensure required cols exist and ensure category
                df = ensure_category_column(df)
                # create actual_quantity if missing
                if 'actual_quantity' not in df.columns:
                    df['actual_quantity'] = ""
                else:
                    df['actual_quantity'] = df['actual_quantity'].fillna("")
                # select and reorder columns
                cols_order = ["name_en","category","branch_name","barcodes","brand","available_quantity","actual_quantity"]
                present = [c for c in cols_order if c in df.columns]
                df_to_write = df[present].copy()
                sheet_name = str(brand)[:31] if brand else "Brand"
                df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                headers = list(df_to_write.columns)
                # compute column letters
                def letter(i):
                    s=""
                    n=i+1
                    while n:
                        n,rem=divmod(n-1,26)
                        s=chr(65+rem)+s
                    return s
                col_map = {h: letter(i) for i,h in enumerate(headers)}
                diff_col_idx = len(headers)
                diff_col_letter = letter(diff_col_idx)
                worksheet.write(0, diff_col_idx, "difference")
                for r_i in range(len(df_to_write)):
                    excel_row = r_i + 2
                    avail = f"{col_map['available_quantity']}{excel_row}"
                    actual = f"{col_map['actual_quantity']}{excel_row}"
                    formula = f"={actual}-{avail}"
                    worksheet.write_formula(r_i+1, diff_col_idx, formula)
                    name_cell = f"'{sheet_name}'!{col_map.get('name_en','A')}{excel_row}"
                    barcode_cell = f"'{sheet_name}'!{col_map.get('barcodes','B')}{excel_row}"
                    diff_cell = f"'{sheet_name}'!{diff_col_letter}{excel_row}"
                    summary_entries.append((name_cell, barcode_cell, diff_cell))
            # summary
            if summary_entries:
                summary_ws = workbook.add_worksheet("Summary")
                summary_ws.write_row(0,0,["Product Name","Barcode","Difference"])
                for i, (ncell, bcell, dcell) in enumerate(summary_entries, start=1):
                    summary_ws.write_formula(i,0,f"={ncell}")
                    summary_ws.write_formula(i,1,f"={bcell}")
                    summary_ws.write_formula(i,2,f"={dcell}")
        generated_files.append(out_path)

    return generated_files
