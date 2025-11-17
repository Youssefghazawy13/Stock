# Stock — Inventory Counting Application

A Streamlit app that reads a Products file and a Counting Schedule and generates daily Excel reports.

## Behavior (summary)
- Products file must include (case-insensitive):  
  `name_en`, `branch_name`, `barcodes`, `brand`, `available_quantity`, `actual_quantity`  
  (`category` is optional — if missing it will be auto-extracted from `name_en`).

- Schedule file must include (case-insensitive):  
  `branch`, `date`, `brand`  
  `date` may be a full date or a day number (1–31). Day numbers are mapped to the current month/year using Africa/Cairo timezone.

- The app processes **only schedule rows that match today** (Africa/Cairo time).  
- Output: one Excel file per `(branch, today)` named `BranchName_DD-MM-YYYY.xlsx`.
  - **First sheet**: `Summary` (`Product Name`, `Barcode`, `Difference`)
  - Then one sheet per brand with columns:
    `name_en, category, branch_name, barcodes, brand, available_quantity, actual_quantity, difference`

- `difference = actual_quantity - available_quantity`.

## How to run locally
```bash
git clone <repo-url>
cd <repo>
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1

pip install -r requirements.txt
streamlit run app.py

1. Clone repo:https://github.com/youssefghazawy13/stock.git

