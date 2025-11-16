import os, re, io
from datetime import datetime
import pandas as pd
import streamlit as st
import pdfplumber
from pypdf import PdfReader

USER_ROLES = {
    "admin": {"password": "adminpass", "role": "admin"},
    "user": {"password": "userpass", "role": "user"},
}

REQUIRED_BPL_COLUMNS = ['Sl No', 'Consumer No', 'Present Status', 'Effect From', 'Effect To', 'Last Updated On']
REQUIRED_AREAR_COLUMNS = ['Sl.No', 'Consumer No', 'Address', 'Phone', 'Route', 'Last Reading Date', 'Last Pay Date', 'Last Amount Paid', 'Arrears', 'Disconn. Date']
REQUIRED_READER_COLUMNS = ['RouteCode', 'Meter reader']
TEMPLATE_READING_SHEET_COLUMNS = [
    "SL. No.", "Route number", "Consumer No.", "Area code", "Consumer code",
    "category", "Primary key", "Phone No.", "Meter Number",
    "Previous Reading Date", "Previous Reading", "Arrears",
    "Current Reading", "Payable", "Remarks"
]


def _buffer(uploaded):
    if uploaded is None:
        return None
    data = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
    return io.BytesIO(data)


def read_data_file(uploaded, sheet_name=None, **kwargs):
    if uploaded is None:
        return None
    buf = _buffer(uploaded)
    buf.seek(0)
    suffix = os.path.splitext(uploaded.name)[1].lower()
    if suffix in ('.xls', '.xlsx'):
        actual_sheet = 0 if sheet_name is None else sheet_name
        data = pd.read_excel(buf, sheet_name=actual_sheet, engine='openpyxl', **kwargs)
        if isinstance(data, dict):
            return next(iter(data.values()), pd.DataFrame())
        return data
    if suffix == '.csv':
        return pd.read_csv(buf, **kwargs)
    raise ValueError("Unsupported file type. Please upload Excel or CSV files.")


def find_column(df, candidates):
    cleaned_map = {}
    for col in df.columns:
        clean = str(col).replace('\u00A0', ' ').strip().lower()
        clean = re.sub(r'\s+', ' ', clean)
        cleaned_map[clean] = col
    for cand in candidates:
        c = cand.lower()
        if c in cleaned_map:
            return cleaned_map[c]
    for cand in candidates:
        c = cand.lower()
        for clean, orig in cleaned_map.items():
            if c in clean:
                return orig
    return None


def normalize_category(cat):
    if pd.isna(cat):
        return 'Unknown'
    c = str(cat).strip().upper()
    return {
        'D': 'Domestic',
        'DOMESTIC': 'Domestic',
        'N': 'Non Domestic',
        'NON DOMESTIC': 'Non Domestic',
        'I': 'Industrial',
        'INDUSTRIAL': 'Industrial',
        'S': 'Special',
        'SPECIAL': 'Special',
    }.get(c, 'Unknown')


def calc_domestic(usage, billed=True):
    if usage <= 0:
        return 144.0 if billed else 0.0
    if usage <= 10:
        return 144.0
    if usage <= 20:
        return 144.0 + (usage - 10) * 14.41
    if usage <= 30:
        return 288.0 + (usage - 20) * 15.51
    if usage <= 40:
        return usage * 16.62
    if usage <= 50:
        return usage * 17.72
    if usage <= 60:
        return usage * 19.92
    if usage <= 80:
        return usage * 23.23
    if usage <= 100:
        return usage * 25.44
    return 1272.0 + (usage - 100) * 54.10


def calc_nondomestic(usage, billed=True):
    if usage <= 0:
        return 641.0 if billed else 0.0
    if usage <= 30:
        return 641.0
    if usage <= 60:
        return 796.2 + (usage - 30) * 33.15
    if usage <= 100:
        return 1790.7 + (usage - 60) * 40.87
    return 3425.5 + (usage - 100) * 54.10


def calc_industrial(usage, billed=True):
    if usage <= 0:
        return 1413.0 if billed else 0.0
    if usage <= 20:
        return 1413.0
    if usage <= 30:
        return 1413.0 + (usage - 20) * 54.1
    if usage <= 60:
        return 1954.0 + (usage - 30) * 33.15
    if usage <= 100:
        return 2947.0 + (usage - 60) * 40.87
    if usage <= 130:
        return 5741.0 + (usage - 100) * 20.77
    return 7364.0 + (usage - 130) * 54.10


def calculate_water_charge(usage, category, billed=True):
    try:
        u = float(usage) if not pd.isna(usage) else 0.0
    except Exception:
        u = 0.0
    cat = normalize_category(category)
    if cat == 'Non Domestic':
        return calc_nondomestic(u, billed)
    if cat == 'Industrial':
        return calc_industrial(u, billed)
    if cat in ('Domestic', 'Special'):
        return calc_domestic(u, billed)
    return 0.0


def calculate_water_charges(df):
    if df is None or df.empty:
        raise ValueError("Uploaded file is empty")

    usage_col = find_column(df, [
        "usage", "consumption", "reading", "water used", "final reading", "current reading"
    ])
    category_col = find_column(df, [
        "category", "type", "consumer type", "connection type"
    ])
    billed_date_col = find_column(df, [
        "bill date", "billed date", "billing date", "billdate"
    ])

    if usage_col is None:
        raise ValueError("No usage/consumption column detected in the uploaded file")

    working_df = df.copy()
    working_df[usage_col] = pd.to_numeric(working_df[usage_col], errors='coerce').fillna(0.0)

    if category_col:
        working_df[category_col] = working_df[category_col].apply(normalize_category)

    if billed_date_col and billed_date_col in working_df.columns:
        billed_series = working_df[billed_date_col].notna() & (
            working_df[billed_date_col].astype(str).str.strip() != ""
        )
        billed_message = f"Detected Bill Date column: '{billed_date_col}'."
    else:
        billed_series = pd.Series([True] * len(working_df), index=working_df.index)
        billed_message = "No Bill Date column found. Assuming all rows are billed."

    charges = []
    for idx, row in working_df.iterrows():
        cat_value = row[category_col] if category_col else "Domestic"
        charges.append(round(calculate_water_charge(row[usage_col], cat_value, bool(billed_series.loc[idx])), 2))

    result_df = working_df.copy()
    result_df["Water Charge"] = charges
    result_df["_Detected_Billed"] = billed_series.values

    return result_df, usage_col, category_col, billed_message


def extract_consumer_info(consumer_no):
    area_code = pd.NA
    consumer_code = pd.NA
    category = pd.NA
    primary_key = pd.NA
    if isinstance(consumer_no, str) and '/' in consumer_no:
        parts = consumer_no.split('/')
        if len(parts) == 3:
            area_code = parts[0][:3]
            consumer_code = parts[1]
            category = parts[2][-1]
            primary_key = area_code + consumer_code + category
    return area_code, consumer_code, category, primary_key


def transform_arear_list(uploaded, sheet_name):
    data = read_data_file(uploaded, sheet_name=sheet_name, skiprows=4)
    if data is None or data.empty:
        raise ValueError("Unable to read Arear List after skipping header rows")
    column_rename_map = {
        'Sl.No': 'Sl.No', 'Consumer No': 'Consumer No', 'Address': 'Address',
        'Phone': 'Phone', 'Route': 'Route', 'Last Reading Date': 'Last Reading Date',
        'Last Pay Date': 'Last Pay Date', 'Last Amount Paid': 'Last Amount Paid',
        'Disconn. Date': 'Disconn. Date', 'Arrears': 'Arrears'
    }
    data = data.rename(columns=column_rename_map)
    if 'Consumer No' not in data.columns:
        raise ValueError("Transformed Arear List missing 'Consumer No'")
    data[['area code', 'consumer code', 'category', 'Primary key']] = data['Consumer No'].apply(
        lambda x: pd.Series(extract_consumer_info(x))
    )
    if 'Arrears' not in data.columns:
        prev = data['PREVIOUS ARREAR'] if 'PREVIOUS ARREAR' in data.columns else 0
        curr = data['CURRENT ARREAR'] if 'CURRENT ARREAR' in data.columns else 0
        data['Arrears'] = pd.to_numeric(prev, errors='coerce').fillna(0) + pd.to_numeric(curr, errors='coerce').fillna(0)
    desired_header = [
        'Sl.No', 'Consumer No', 'area code', 'consumer code', 'category', 'Primary key',
        'Address', 'Phone', 'Route', 'Last Reading Date', 'Last Pay Date',
        'Last Amount Paid', 'Arrears', 'Disconn. Date'
    ]
    for col in desired_header:
        if col not in data.columns:
            data[col] = pd.NA
    result = data[desired_header].copy()
    result = result[result['Sl.No'].apply(lambda x: str(x).strip().isdigit())]
    return result


def check_columns_exist(df, required_columns):
    if df is None or df.empty:
        return False, required_columns
    missing = [col for col in required_columns if col not in df.columns]
    return len(missing) == 0, missing


def normalize_dates(df, date_col):
    if date_col and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df


def keep_latest_billed_date(df, route_key_col, billed_date_col):
    if billed_date_col not in df.columns or route_key_col not in df.columns:
        return df
    df = normalize_dates(df, billed_date_col)
    if df[billed_date_col].notna().sum() == 0:
        return df
    idx = df.groupby(route_key_col, dropna=False)[billed_date_col].idxmax()
    return df.loc[idx].copy()


def extract_route_num(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    match = re.search(r'\(\s*([0-9]{1,4})\s*\)', text)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            pass
    match = re.search(r'([0-9]+)', text)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            pass
    return None

def clean_text(text):
    return str(text).replace("\u00a0", " ").strip()


def parse_pdf_content(uploaded):
    pdf_bytes = _buffer(uploaded)
    pdf_bytes.seek(0)
    route_no = None
    try:
        reader = PdfReader(pdf_bytes)
        if len(reader.pages) > 0:
            first_page_text = reader.pages[0].extract_text()
            if first_page_text:
                route_match = re.search(r"Route\s*No\s*:\s*(\d+)", first_page_text, re.IGNORECASE)
                if route_match:
                    route_no = route_match.group(1)
    except Exception:
        pass

    # Use pdfplumber to extract tables from the PDF
    pdf_bytes.seek(0)
    dfs = []
    try:
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables or []:
                    if not table:
                        continue
                    # table is a list of rows (lists); convert to DataFrame
                    df = pd.DataFrame(table)
                    # Replace None with empty string and clean each cell
                    df = df.fillna('').astype(str).applymap(clean_text)
                    dfs.append(df)
    except Exception:
        # If pdfplumber extraction fails, return empty dataframe (consistent with prior behavior)
        return pd.DataFrame()

    if not dfs:
        return pd.DataFrame()

    combined_df = pd.DataFrame()
    for df in dfs:
        if df.empty:
            continue
        # Keep the existing header detection and normalization logic
        df.columns = df.columns.astype(str).str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
        expected_headers = ["SL. No.", "Consumer No.", "Phone No.", "Meter Number", "Previous Reading Date", "Previous Reading", "Arrears", "Current Reading", "Amount", "Remarks"]
        column_name_mapping = {
            r"SL\.?\s*No\.?": "SL. No.",
            r"Consumer\s*No\.?": "Consumer No.",
            r"Phone\s*No\.?": "Phone No.",
            r"Meter\s*Number": "Meter Number",
            r"Previous\s*Reading\s*Date": "Previous Reading Date",
            r"Previous\s*Reading|Prev\.?\s*Reading|Last\s*Read": "Previous Reading",
            r"Arrears": "Arrears",
            r"Current\s*Reading": "Current Reading",
            r"Amount|Payable": "Payable",
            r"Remarks": "Remarks",
            r"Bill\s*Issued": "Remarks",
        }
        potential_header_row_index = -1
        cleaned_cols = [' '.join(col.split()).strip() for col in df.columns]
        matched_header_count = sum(1 for exp in expected_headers if any(re.search(exp.replace(' ', r'\s*'), c, re.IGNORECASE) for c in cleaned_cols))
        if matched_header_count >= 3:
            potential_header_row_index = 0
        else:
            for idx, row in df.iterrows():
                row_values_str = ' '.join(row.astype(str).fillna('')).strip()
                row_match = sum(1 for exp in expected_headers if re.search(exp.replace(' ', r'\s*'), row_values_str, re.IGNORECASE))
                if row_match >= 3:
                    potential_header_row_index = idx
                    break
        if potential_header_row_index != -1:
            if potential_header_row_index > 0:
                header_row = df.iloc[potential_header_row_index]
                df = df[potential_header_row_index + 1:].copy()
                df.columns = header_row
            df.columns = df.columns.astype(str).str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
            new_cols = []
            for col in df.columns:
                mapped = False
                for pattern, new_name in column_name_mapping.items():
                    if re.search(pattern, col, re.IGNORECASE):
                        new_cols.append(new_name)
                        mapped = True
                        break
                if not mapped:
                    new_cols.append(col)
            df.columns = new_cols
            df = df.loc[:, ~df.columns.str.contains('^Unnamed:', na=False)]
        if 'Consumer No.' in df.columns:
            df['Consumer No.'] = df['Consumer No.'].astype(str).str.strip().str.replace(r'[\r\n\s]+', '', regex=True)
            split_consumer = df['Consumer No.'].str.split(r'/', expand=True, n=2)
            df['Area code'] = split_consumer[0].fillna('')
            df['Consumer code'] = split_consumer[1].fillna('') if 1 in split_consumer.columns else ''
            df['category'] = split_consumer[2].fillna('') if 2 in split_consumer.columns else ''
            df['Primary key'] = df['Area code'] + df['Consumer code'] + df['category']
        else:
            df['Area code'] = ''
            df['Consumer code'] = ''
            df['category'] = ''
            df['Primary key'] = ''
        df['Route number'] = route_no
        keywords_to_remove = ["meter reader", "meter inspector", "posting clerk", "kerala water authority"]
        mask = df.apply(lambda row: not any(any(keyword in str(cell).lower() for cell in row) for keyword in keywords_to_remove), axis=1)
        df = df[mask].copy()
        for col in TEMPLATE_READING_SHEET_COLUMNS:
            if col not in df.columns:
                df[col] = ''
        df = df[TEMPLATE_READING_SHEET_COLUMNS]
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    return combined_df


def process_bpl_df(bpl_sheet_df):
    bpl_sheet = bpl_sheet_df.dropna(how='all').copy()
    valid, missing = check_columns_exist(bpl_sheet, REQUIRED_BPL_COLUMNS)
    if not valid:
        raise ValueError(f"BPL sheet missing columns: {', '.join(missing)}")
    bpl_sheet['Effect From'] = pd.to_datetime(bpl_sheet['Effect From'], errors='coerce')
    bpl_sheet['Effect To'] = pd.to_datetime(bpl_sheet['Effect To'], errors='coerce')
    bpl_sheet['Last Updated On'] = pd.to_datetime(bpl_sheet['Last Updated On'], errors='coerce')
    bpl_sheet['Year'] = bpl_sheet['Effect From'].dt.year.fillna(-1).astype(int)
    bpl_sheet['Month'] = bpl_sheet['Effect From'].dt.month.fillna(-1).astype(int)
    if 'Consumer No' not in bpl_sheet.columns:
        raise ValueError("'Consumer No' missing in BPL sheet")
    bpl_sheet['Consumer Code'] = bpl_sheet['Consumer No'].astype(str).str.replace('/', '', regex=False).str.strip()
    return bpl_sheet[['Consumer Code', 'Present Status', 'Effect From', 'Effect To', 'Year', 'Month']]


def expand_bpl_years(bpl_df):
    expanded_rows = []
    bpl_df['Effect From'] = pd.to_datetime(bpl_df['Effect From'], errors='coerce')
    bpl_df['Effect To'] = pd.to_datetime(bpl_df['Effect To'], errors='coerce')
    for _, row in bpl_df.iterrows():
        start_year = row['Effect From'].year if pd.notna(row['Effect From']) else None
        end_year = row['Effect To'].year if pd.notna(row['Effect To']) else None
        code = str(row['Consumer Code']) if pd.notna(row['Consumer Code']) else None
        status = row['Present Status'] if pd.notna(row['Present Status']) else 'Unknown'
        if code is None:
            continue
        if start_year is None or end_year is None:
            expanded_rows.append({'Consumer Code': code, 'Year': 'N/A', 'Present Status': status})
            continue
        if not isinstance(start_year, (int, float)) or not isinstance(end_year, (int, float)):
            expanded_rows.append({'Consumer Code': code, 'Year': 'Invalid Date Range', 'Present Status': status})
            continue
        for year in range(int(start_year), int(end_year) + 1):
            expanded_rows.append({'Consumer Code': code, 'Year': year, 'Present Status': status})
    return pd.DataFrame(expanded_rows)


def merge_arear_data(arear_df, reader_df, reading_df, bpl_df):
    if arear_df is None or arear_df.empty:
        raise ValueError("Arear list data is empty")
    if reader_df is None or reader_df.empty:
        raise ValueError("Reader list data is empty")
    if reading_df is None or reading_df.empty:
        raise ValueError("Reading sheet data is empty")
    if bpl_df is None or bpl_df.empty:
        raise ValueError("BPL list data is empty")

    arear = arear_df.dropna(how='all').copy()
    reader = reader_df.dropna(how='all').copy()
    reading = reading_df.dropna(how='all').copy()
    bpl = bpl_df.dropna(how='all').copy()

    valid, missing = check_columns_exist(arear, REQUIRED_AREAR_COLUMNS)
    if not valid:
        raise ValueError(f"Arear list missing columns: {', '.join(missing)}")

    valid, missing = check_columns_exist(reader, REQUIRED_READER_COLUMNS)
    if not valid:
        raise ValueError(f"Reader list missing columns: {', '.join(missing)}")

    required_reading_cols = ['Primary key', 'Previous Reading', 'Previous Reading Date']
    valid, missing = check_columns_exist(reading, required_reading_cols)
    if not valid:
        raise ValueError(f"Reading sheet missing columns: {', '.join(missing)}")

    if 'Primary key' not in arear.columns:
        if 'Consumer No' not in arear.columns:
            raise ValueError("Arear list must contain 'Consumer No'")
        arear[['area code', 'consumer code', 'category', 'Primary key']] = arear['Consumer No'].apply(
            lambda x: pd.Series(extract_consumer_info(x))
        )

    arear['Primary key'] = arear['Primary key'].astype(str).str.strip()
    reading['Primary key'] = reading['Primary key'].astype(str).str.strip()
    arear['Route'] = arear['Route'].astype(str).str.strip()
    reader['RouteCode'] = reader['RouteCode'].astype(str).str.strip()

    if 'Consumer Code' not in bpl.columns:
        raise ValueError("BPL list must include 'Consumer Code'")
    bpl['Consumer Code'] = bpl['Consumer Code'].astype(str).str.strip()

    expanded_bpl = expand_bpl_years(bpl)
    if expanded_bpl.empty:
        bpl_pivot = pd.DataFrame(columns=['Consumer Code'])
    else:
        bpl_pivot = expanded_bpl.pivot_table(
            index='Consumer Code',
            columns='Year',
            values='Present Status',
            aggfunc='first'
        ).reset_index()
        bpl_pivot.columns = ['Consumer Code'] + [f"BPL_{str(col)}" for col in bpl_pivot.columns[1:]]

    merged = arear.merge(
        reader[['RouteCode', 'Meter reader']],
        left_on='Route',
        right_on='RouteCode',
        how='left'
    )

    reading_subset = reading[['Primary key', 'Previous Reading', 'Previous Reading Date', 'Consumer code']].copy()
    merged = merged.merge(
        reading_subset,
        on='Primary key',
        how='left'
    )

    if 'Previous Reading' not in merged.columns:
        raise ValueError("Reading sheet merge did not produce 'Previous Reading'")

    merged = merged.rename(columns={
        'Meter reader': 'Meter Reader Name',
        'Previous Reading': 'Final Reading',
        'Previous Reading Date': 'Last Reading Date PDF'
    })

    if not bpl_pivot.empty:
        merged = merged.merge(bpl_pivot, left_on='Primary key', right_on='Consumer Code', how='left')

    merged.drop(columns=['RouteCode'], errors='ignore', inplace=True)
    merged.drop(columns=['Consumer Code', 'consumer code'], errors='ignore', inplace=True)

    if 'Final Reading' in merged.columns:
        merged['Final Reading'] = pd.to_numeric(merged['Final Reading'], errors='coerce').fillna(0)
        for threshold in [100, 150, 200, 300, 500, 750, 1000]:
            merged[f'Reading > {threshold}'] = merged['Final Reading'] > threshold

    if 'Last Reading Date PDF' in merged.columns:
        merged['Last Reading Date PDF'] = pd.to_datetime(merged['Last Reading Date PDF'], errors='coerce', dayfirst=True)
        merged['Year'] = merged['Last Reading Date PDF'].dt.year.fillna(-1).astype(int)
        merged['Month'] = merged['Last Reading Date PDF'].dt.month.fillna(-1).astype(int)
    else:
        merged['Year'] = -1
        merged['Month'] = -1

    column_order = list(merged.columns)
    bpl_cols = [col for col in column_order if col.startswith('BPL_')]
    if bpl_cols:
        for col in bpl_cols:
            column_order.remove(col)
        insert_idx = column_order.index('Consumer No') + 1 if 'Consumer No' in column_order else len(column_order)
        for col in sorted(bpl_cols, key=lambda x: x.split('_')[1]):
            column_order.insert(insert_idx, col)
            insert_idx += 1
        merged = merged[column_order]

    merged.drop(columns=['Primary key'], errors='ignore', inplace=True)

    return merged


def extract_route_data(df):
    df = df.rename(columns={df.columns[0]: "Route", df.columns[1]: "Billed Date"})
    df = df[df["Route"].astype(str).str.lower() != "route"]
    df["Route"] = df["Route"].apply(clean_text)
    df["Route Number"] = df["Route"].apply(lambda x: re.search(r"\(\s*(\d+)\s*\)", x).group(1).strip() if pd.notna(x) and re.search(r"\(\s*(\d+)\s*\)", x) else "")
    df["Route Name"] = df["Route"].apply(lambda x: re.split(r"\)\s*", x, maxsplit=1)[-1].strip() if ")" in x else x)
    return df[["Route", "Billed Date", "Route Number", "Route Name"]]


def merge_billing_dates(billed_routes_df, consumer_df):
    if billed_routes_df is None or billed_routes_df.empty:
        raise ValueError("Billed route template data is empty")
    if consumer_df is None or consumer_df.empty:
        raise ValueError("Consumer data file is empty")

    billed_df = billed_routes_df.copy()
    sep_df = consumer_df.copy()

    # Ensure billed template has expected columns; try to normalize if missing
    if 'Route Number' not in billed_df.columns or 'Billed Date' not in billed_df.columns:
        try:
            billed_df = extract_route_data(billed_df)
        except Exception as exc:
            raise ValueError("Billed route data must include 'Route Number' and 'Billed Date' columns") from exc

    billed_df["Route Number"] = billed_df["Route Number"].apply(lambda x: int(x) if pd.notna(x) and str(x).strip() != '' else None)
    billed_df = normalize_dates(billed_df, "Billed Date")
    billed_df = keep_latest_billed_date(billed_df, "Route Number", "Billed Date")

    sep_route_number_col = find_column(sep_df, ["route number", "route_no", "route no", "routenumber", "route_number", "route"])
    if sep_route_number_col:
        sep_df["route_key"] = sep_df[sep_route_number_col].apply(extract_route_num)
    else:
        sep_df["route_key"] = None
        for col in sep_df.columns:
            sample = sep_df[col].astype(str).head(10).str.cat(sep=' ')
            if re.search(r'[0-9]{1,4}', sample):
                sep_df["route_key"] = sep_df[col].apply(extract_route_num)
                if sep_df["route_key"].notna().sum() > 0:
                    break
    if sep_df["route_key"].notna().sum() == 0:
        raise ValueError("Could not derive route numbers from consumer data")

    billed_for_merge = billed_df[["Route Number", "Billed Date"]].rename(columns={"Route Number": "route_key"})
    billed_for_merge = billed_for_merge.drop_duplicates(subset=['route_key'], keep='first')

    merged = sep_df.merge(billed_for_merge, on='route_key', how='left', validate='m:1')
    return merged


def calculate_usage_report(previous_df, current_df, save_name):
    if previous_df is None or previous_df.empty:
        raise ValueError("Previous month data is empty")
    if current_df is None or current_df.empty:
        raise ValueError("Current month data is empty")

    august_df = previous_df.copy()
    september_df = current_df.copy()

    august_reading_col = find_column(august_df, ['final reading', 'reading', 'final_reading', 'meter reading'])
    september_reading_col = find_column(september_df, ['final reading', 'reading', 'final_reading', 'meter reading'])
    if not august_reading_col or not september_reading_col:
        raise ValueError("Could not find reading columns in provided files")

    august_consumer_col = find_column(august_df, ['consumer no', 'consumer_no', 'consumerno', 'consumer number'])
    september_consumer_col = find_column(september_df, ['consumer no', 'consumer_no', 'consumerno', 'consumer number'])
    if not august_consumer_col or not september_consumer_col:
        raise ValueError("Could not find consumer number columns in provided files")

    august_data = august_df[[august_consumer_col, august_reading_col]].copy()
    august_data.columns = ['Consumer_No', 'Previous_Reading']

    additional_columns_map = {
        'Address': ['address', 'consumer address', 'location'],
        'Phone': ['phone', 'mobile', 'contact'],
        'Route': ['route'],
        'area code': ['area code', 'area_code', 'areacode'],
        'category': ['category', 'consumer category'],
        'Meter Reader Name': ['meter reader name', 'reader name', 'meter_reader_name', 'reader'],
        'BPL_2024': ['bpl_2024', 'bpl 2024'],
        'BPL_2025': ['bpl_2025', 'bpl 2025'],
        'Last Reading Date': ['last reading date', 'reading date', 'last_reading_date'],
        'Last Pay Date': ['last pay date', 'payment date', 'last_pay_date'],
        'Last Amount Paid': ['last amount paid', 'amount paid', 'last_amount_paid', 'payment amount'],
        'Arrears': ['arrears', 'outstanding', 'due amount'],
        'Disconn. Date': ['disconn. date', 'disconnect date', 'disconnection date', 'disconn_date']
    }

    found_columns = {}
    for desired, candidates in additional_columns_map.items():
        found = find_column(september_df, candidates)
        if found:
            found_columns[desired] = found

    september_columns = [september_consumer_col, september_reading_col] + list(found_columns.values())
    september_columns = [col for col in september_columns if col in september_df.columns]
    september_data = september_df[september_columns].copy()

    rename_map = {september_consumer_col: 'Consumer_No', september_reading_col: 'Current_Reading'}
    rename_map.update({v: k for k, v in found_columns.items()})
    september_data = september_data.rename(columns=rename_map)

    merged = pd.merge(august_data, september_data, on='Consumer_No', how='inner')
    merged['Previous_Reading'] = pd.to_numeric(merged['Previous_Reading'], errors='coerce')
    merged['Current_Reading'] = pd.to_numeric(merged['Current_Reading'], errors='coerce')
    merged['Usage'] = merged['Current_Reading'] - merged['Previous_Reading']
    merged = merged.dropna(subset=['Previous_Reading', 'Current_Reading'])

    column_order = [
        'Consumer_No', 'Address', 'Phone', 'Route', 'area code', 'category', 'Meter Reader Name',
        'Previous_Reading', 'Current_Reading', 'Usage', 'BPL_2024', 'BPL_2025',
        'Last Reading Date', 'Last Pay Date', 'Last Amount Paid', 'Arrears', 'Disconn. Date'
    ]
    final_columns = [col for col in column_order if col in merged.columns]
    result = merged[final_columns]

    total_usage = result['Usage'].sum()
    avg_usage = result['Usage'].mean()
    max_usage = result['Usage'].max()
    min_usage = result['Usage'].min()

    arrears_stats = {}
    if 'Arrears' in result.columns:
        result['Arrears'] = pd.to_numeric(result['Arrears'], errors='coerce')
        arrears_stats = {
            'Total Arrears': result['Arrears'].sum(),
            'Average Arrears': result['Arrears'].mean(),
            'Consumers with Arrears': len(result[result['Arrears'] > 0])
        }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{save_name}_{timestamp}.xlsx"
    output_buffer = io.BytesIO()

    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
        result.to_excel(writer, sheet_name='Usage_Calculation', index=False)
        summary_metrics = ['Total Records', 'Total Usage', 'Average Usage', 'Maximum Usage', 'Minimum Usage']
        summary_values = [len(result), total_usage, avg_usage, max_usage, min_usage]
        if arrears_stats:
            summary_metrics.extend(arrears_stats.keys())
            summary_values.extend(arrears_stats.values())
        summary_df = pd.DataFrame({'Metric': summary_metrics, 'Value': summary_values})
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        usage_categories = pd.DataFrame({
            'Category': ['0-100 units', '101-200 units', '201-500 units', '501+ units'],
            'Count': [
                len(result[(result['Usage'] >= 0) & (result['Usage'] <= 100)]),
                len(result[(result['Usage'] > 100) & (result['Usage'] <= 200)]),
                len(result[(result['Usage'] > 200) & (result['Usage'] <= 500)]),
                len(result[result['Usage'] > 500])
            ]
        })
        usage_categories.to_excel(writer, sheet_name='Usage_Categories', index=False)
        high_usage = result[result['Usage'] > 500].copy()
        if not high_usage.empty:
            high_usage.sort_values('Usage', ascending=False).to_excel(writer, sheet_name='High_Usage_Consumers', index=False)

    output_buffer.seek(0)

    summary = {
        'records': len(result),
        'total_usage': total_usage,
        'average_usage': avg_usage,
        'max_usage': max_usage,
        'min_usage': min_usage,
        'arrears_stats': arrears_stats,
        'columns': final_columns,
        'file': output_buffer,
        'filename': filename
    }
    return result, summary


def create_billed_route_template():
    template_df = pd.DataFrame({
        "Route": ["( 1 ) Njarakkadu-Thazhamel", "( 2 ) AnanthaBhavan School to Njarakkadu"],
        "Reading Date": ["2025-08-12", "2025-08-12"],
        "Bill From": [8619, 8619],
        "Bill To": [8725, 8727]
    })
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        template_df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer


def download_excel(dataframe, filename):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer, filename


def create_template(columns):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(columns=columns).to_excel(writer, index=False)
    buffer.seek(0)
    return buffer


def main():
    st.set_page_config(page_title="Water Revenue Analysis", layout="wide")

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.role = None

    if not st.session_state.authenticated:
        st.title("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            user = USER_ROLES.get(username)
            if user and password == user["password"]:
                st.session_state.authenticated = True
                st.session_state.role = user["role"]
                st.rerun()
            else:
                st.error("Invalid credentials")
        st.stop()

    st.title("Water Revenue Analysis Dashboard")
    st.sidebar.title("Navigation")
    selected_page = st.sidebar.radio(
        "Go to",
        [
            "Dashboard",
            "Usage Comparison",
            "Water Charge Calculator"
        ],
        key="navigation_selector"
    )

    if selected_page == "Dashboard":
        tabs = st.tabs([
            "Arear List Merge",
            "Billed routes data"
        ])

        with tabs[0]:
            st.header("Total Consumer data")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.download_button(
                    "Download Arear List Template",
                    data=create_template(REQUIRED_AREAR_COLUMNS),
                    file_name="arear_list_template.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="arear_template_download"
                )
            with col2:
                st.download_button(
                    "Download Reader List Template",
                    data=create_template(REQUIRED_READER_COLUMNS),
                    file_name="reader_list_template.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="reader_template_download"
                )
            with col3:
                st.download_button(
                    "Download BPL List Template",
                    data=create_template(REQUIRED_BPL_COLUMNS),
                    file_name="bpl_list_template.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="bpl_template_download"
                )
            arear_file = st.file_uploader("Upload Arear List", type=["xlsx", "xls", "csv"], key="arear_file")
            reader_file = st.file_uploader("Upload Reader List", type=["xlsx", "xls", "csv"], key="reader_file")
            reading_files = st.file_uploader("Upload Reading Sheets (PDF)", type=["pdf"], accept_multiple_files=True, key="reading_files")
            bpl_file = st.file_uploader("Upload BPL List", type=["xlsx", "xls", "csv"], key="bpl_file")

            if st.button("Process Arear Merge"):
                progress_container = st.empty()
                status_placeholder = st.empty()
                merged_df = None
                try:
                    progress_bar = progress_container.progress(0)

                    def update_progress(message, value, status_type="info"):
                        progress = int(min(max(value, 0), 100))
                        progress_bar.progress(progress)
                        if status_type == "info":
                            status_placeholder.info(f"{message} ({progress}%)")
                        elif status_type == "warning":
                            status_placeholder.warning(f"{message} ({progress}%)")
                        elif status_type == "success":
                            status_placeholder.success(f"{message} ({progress}%)")
                        else:
                            status_placeholder.error(f"{message} ({progress}%)")

                    update_progress("Validating inputs", 10)
                    if arear_file is None or reader_file is None or not reading_files or bpl_file is None:
                        raise ValueError("Please upload Arear list, Reader list, at least one Reading sheet PDF, and BPL list")

                    update_progress("Reading Arear list", 25)
                    arear_df = read_data_file(arear_file)

                    update_progress("Reading Reader list", 40)
                    reader_df = read_data_file(reader_file)

                    reading_dfs = []
                    total_pdfs = len(reading_files)
                    if total_pdfs:
                        increment = 20 / total_pdfs
                        progress_value = 40
                        for idx, pdf in enumerate(reading_files, start=1):
                            try:
                                reading_dfs.append(parse_pdf_content(pdf))
                            except Exception as e:
                                st.warning(f"Failed to parse one of the reading PDFs: {e}")
                            progress_value += increment
                            update_progress(f"Parsing reading sheets ({idx}/{total_pdfs})", progress_value)
                    else:
                        update_progress("Parsing reading sheets", 60)

                    reading_df = pd.concat(reading_dfs, ignore_index=True) if reading_dfs else pd.DataFrame()

                    update_progress("Processing BPL list", 75)
                    bpl_raw_df = read_data_file(bpl_file)
                    bpl_df = process_bpl_df(bpl_raw_df)
                    if bpl_df.empty:
                        st.warning("Processed BPL list is empty; merge results may be incomplete.")
                        update_progress("Processed BPL list is empty", 80, status_type="warning")

                    update_progress("Merging datasets", 95)
                    merged_df = merge_arear_data(arear_df, reader_df, reading_df, bpl_df)
                    update_progress("Merge completed", 100, status_type="success")
                except Exception as exc:
                    progress_container.empty()
                    status_placeholder.error(str(exc))
                    st.error(str(exc))
                else:
                    if merged_df is None or merged_df.empty:
                        st.warning("Merge completed but resulted in no rows.")
                    else:
                        st.success("Arear merge completed")
                        st.dataframe(merged_df.head(20))
                        buffer, filename = download_excel(merged_df, f"arear_merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                        st.download_button("Download Merged Arear Data", data=buffer, file_name=filename)
                finally:
                    if merged_df is None:
                        progress_container.empty()

        with tabs[1]:
            st.header("Billed routes data (Eabacus>Supervisory function>monitoring> billed routes>month Wise)")
            template_col, upload_col = st.columns([1, 3])
            with template_col:
                st.download_button(
                    "Download Billed Route Template",
                    data=create_billed_route_template(),
                    file_name="billed_routes_template.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="billed_route_template_download"
                )
            with upload_col:
                billed_route_file = st.file_uploader("Upload billed route template (Supervisory function>monitoring> billed routes)", type=["xlsx", "xls", "csv"], key="billed_file")
                consumer_file = st.file_uploader("Upload consumer data file", type=["xlsx", "xls", "csv"], key="consumer_file")
            if st.button("Merge Billing Dates"):
                try:
                    if billed_route_file is None or consumer_file is None:
                        raise ValueError("Please upload both billed route template and consumer data file")
                    billed_df = read_data_file(billed_route_file)
                    consumer_df = read_data_file(consumer_file)
                    merged_df = merge_billing_dates(billed_df, consumer_df)
                    if merged_df.empty:
                        st.warning("No matching records found after merging")
                        st.dataframe(merged_df.head(20))
                    else:
                        st.success("Billing dates merged")
                        st.dataframe(merged_df.head(20))
                        buffer, filename = download_excel(merged_df, f"billing_merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                        st.download_button("Download Billing Merge", data=buffer, file_name=filename)
                except Exception as exc:
                    st.error(str(exc))

    elif selected_page == "Usage Comparison":
        st.header("Usage Comparison Report")
        prev_month = st.file_uploader("Upload previous month report", type=["xlsx", "xls", "csv"], key="prev_month")
        curr_month = st.file_uploader("Upload current month report", type=["xlsx", "xls", "csv"], key="curr_month")
        save_name = st.text_input("Output filename prefix", value="usage_report")
        if st.button("Generate Usage Comparison"):
            try:
                if prev_month is None or curr_month is None:
                    raise ValueError("Please upload both previous and current month reports")
                prev_df = read_data_file(prev_month)
                curr_df = read_data_file(curr_month)
                result_df, summary = calculate_usage_report(prev_df, curr_df, save_name)
                st.success("Usage comparison generated")
                st.metric("Total Usage", f"{summary['total_usage']:.2f}")
                st.metric("Average Usage", f"{summary['average_usage']:.2f}")
                st.metric("Max Usage", f"{summary['max_usage']:.2f}")

                if summary['arrears_stats']:
                    st.metric("Total Arrears", f"{summary['arrears_stats']['Total Arrears']:.2f}")
                    st.metric("Average Arrears", f"{summary['arrears_stats']['Average Arrears']:.2f}")

                st.dataframe(result_df.head(20))
                st.download_button(
                    "Download Usage Report",
                    data=summary['file'],
                    file_name=summary['filename']
                )
            except Exception as exc:
                st.error(str(exc))

    elif selected_page == "Water Charge Calculator":
        st.header("Water Charge Calculator")
        uploaded_file = st.file_uploader(
            "Upload consumer Excel/CSV file",
            type=["xlsx", "xls", "csv"],
            key="water_charge_file",
        )
        if st.button("Calculate Water Charges"):
            progress_container = st.empty()
            status_placeholder = st.empty()
            try:
                progress_bar = progress_container.progress(0)

                def update_status(message, value, status_type="info"):
                    pct = int(min(max(value, 0), 100))
                    progress_bar.progress(pct)
                    text = f"{message} ({pct}%)"
                    getattr(status_placeholder, status_type)(text)

                update_status("Validating input file", 10)
                if uploaded_file is None:
                    raise ValueError("Please upload a consumer data file before calculating water charges")

                update_status("Reading consumer data", 35)
                df = read_data_file(uploaded_file)

                update_status("Calculating water charges", 75)
                result_df, usage_col, category_col, billed_message = calculate_water_charges(df)

                update_status("Preparing results", 95, status_type="success")
                st.success("Water charges calculated successfully")
                st.write(billed_message)
                st.dataframe(result_df.head(20))
                buffer, filename = download_excel(result_df, f"water_charges_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                st.download_button("Download Results", data=buffer, file_name=filename)
                progress_bar.progress(100)
            except Exception as exc:
                progress_container.empty()
                status_placeholder.error(str(exc))
                st.error(str(exc))

if st.sidebar.button("Logout"):
    st.session_state.authenticated = False
    st.session_state.role = None
    st.rerun()


if __name__ == "__main__":
    main()