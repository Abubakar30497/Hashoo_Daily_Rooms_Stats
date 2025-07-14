import base64
import io
import pandas as pd
import gspread
from datetime import datetime
from dash import Dash, dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
from oauth2client.service_account import ServiceAccountCredentials

# ========== Google Sheets Setup ==========
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("sheets-381015-db3e2c9a4767.json", scope)
client = gspread.authorize(creds)
sheet_id = "1bRhI66zl254CzLNFSRO6IgS0ngElRJEvnXk_wQmUeYo"
spreadsheet = client.open_by_key(sheet_id)
ws_actual = spreadsheet.worksheet("Actual_25-26")
ws_budget = spreadsheet.worksheet("Budget_25-26")

# ========== File Processor ==========
def process_file(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    df = pd.read_excel(io.BytesIO(decoded), sheet_name="Sheet2", skiprows=2)

    # Handle inconsistent column names
    col_map = {
        'Total Occ.': 'Total Occ',
        'Avg Rate': 'Avg Rate',
        'Avg.Rate': 'Avg Rate',
        'Revenue': 'Revenue',
        'Room Rev': 'Revenue'
    }
    df.rename(columns={col: col_map[col] for col in df.columns if col in col_map}, inplace=True)

    required_cols = ['Date', 'Total Occ']
    for col in ['Avg Rate', 'Revenue']:
        if col not in df.columns:
            df[col] = None  # Fill with NaNs if missing

    forecast_start_index = df[df['Date'].astype(str).str.contains("Forecast", case=False)].index
    forecast_start_index = forecast_start_index[0] if len(forecast_start_index) > 0 else None

    df['Label'] = ['History' if i < forecast_start_index else 'Forecast' for i in df.index] \
        if forecast_start_index is not None else 'History'

    df = df[df['Date'].astype(str).str.contains("JUL-2025")]
    df.reset_index(drop=True, inplace=True)

    hotel_name = filename.split()[1].split('.')[0]
    df['Property'] = hotel_name
    df['Date'] = pd.to_datetime(df['Date'].astype(str).str.extract(r'(\d{2}-[A-Z]{3}-\d{4})')[0], format='%d-%b-%Y')
    df['Month-Year'] = df['Date'].dt.strftime('%b-%Y')

    return df[['Property', 'Date', 'Total Occ', 'Avg Rate', 'Revenue', 'Label', 'Month-Year']]

# ========== Update Google Sheet ==========
def update_google_sheet(processed_df, worksheet):
    EXPECTED_COLUMNS = ['Property', 'Date', 'Total Occ', 'Avg Rate', 'Revenue', 'Label', 'Month-Year']
    
    # Ensure all expected columns are present
    for col in EXPECTED_COLUMNS:
        if col not in processed_df.columns:
            processed_df[col] = None
    
    # Reorder columns
    processed_df = processed_df[EXPECTED_COLUMNS]

    # Get existing data from sheet
    existing_data = worksheet.get_all_values()
    existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0]) if existing_data else pd.DataFrame()
    if existing_df.empty:
        existing_df = pd.DataFrame(columns=EXPECTED_COLUMNS)

    # Ensure types are consistent for comparison and merging
    processed_df['Date'] = pd.to_datetime(processed_df['Date'])
    existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
    processed_df['Month-Year'] = processed_df['Month-Year'].astype(str)
    existing_df['Month-Year'] = existing_df['Month-Year'].astype(str)

    # Remove overlapping records
    for (prop, month) in processed_df[['Property', 'Month-Year']].drop_duplicates().values:
        existing_df = existing_df[~((existing_df["Property"] == prop) & (existing_df["Month-Year"] == month))]

    # Append and sort
    updated_df = pd.concat([existing_df, processed_df], ignore_index=True)
    updated_df = updated_df.sort_values(by=["Property", "Date"])

    # Write back to Google Sheet
    worksheet.clear()
    worksheet.update([updated_df.columns.tolist()] + updated_df.astype(str).values.tolist())

    return f"✅ {len(processed_df)} rows updated for {processed_df['Property'].nunique()} property(ies)."
# ========== Create Colored Table ==========
def make_table(data):
    # Compute total row with safe calculations
    total_row = {
        'Day': 'Total',
        'Month': '',
        'Actual Occ': data['Actual Occ'].sum(skipna=True) if 'Actual Occ' in data.columns else 0,
        'Budget Occ': data['Budget Occ'].sum(skipna=True) if 'Budget Occ' in data.columns else 0,
        'Actual Rate': round(data['Actual Rate'].mean(skipna=True), 2) if 'Actual Rate' in data.columns and data['Actual Rate'].notna().any() else 0,
        'Budget Rate': round(data['Budget Rate'].mean(skipna=True), 2) if 'Budget Rate' in data.columns and data['Budget Rate'].notna().any() else 0,
        'Actual Revenue': data['Actual Revenue'].sum(skipna=True) if 'Actual Revenue' in data.columns else 0,
        'Budget Revenue': data['Budget Revenue'].sum(skipna=True) if 'Budget Revenue' in data.columns else 0,
        'Label': ''
    }

    data = pd.concat([data, pd.DataFrame([total_row])], ignore_index=True)

    return dash_table.DataTable(
        columns=[{"name": col, "id": col} for col in data.columns],
        data=data.to_dict('records'),
        style_table={'overflowY': 'auto', 'height': '600px'},
        style_cell={'textAlign': 'center'},
        style_header={'fontWeight': 'bold'},
        style_data_conditional=[
            {'if': {'filter_query': '{Label} = "History"'}, 'backgroundColor': '#e6f2ff'},
            {'if': {'filter_query': '{Day} = "Total"'}, 'fontWeight': 'bold', 'backgroundColor': '#f1f1f1'}
        ]
    )    
# ========== App Layout ==========
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = dbc.Container([
    html.H3("Hashoo Hotels Rooms Daily Stats (Auto Refresh + Upload)", className="my-3"),
    dcc.Upload(
        id='upload-data',
        children=html.Button("Upload Excel Files (Actual)", style={"fontSize": "16px"}),
        multiple=True
    ),
    html.Div(id='upload-status', className="my-2"),
    dcc.Interval(id='interval-component', interval=5*60*1000, n_intervals=0),
    html.Div(id='tabs-container')
])

# ========== Upload Callback ==========
@app.callback(
    Output('upload-status', 'children'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def handle_upload(contents, filenames):
    if contents and filenames:
        all_dfs = []
        for content, name in zip(contents, filenames):
            try:
                df = process_file(content, name)
                if not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                return html.Div(f"❌ Error processing {name}: {str(e)}")

        if not all_dfs:
            return html.Div("⚠️ No valid data found.")
        
        final_df = pd.concat(all_dfs, ignore_index=True)
        msg = update_google_sheet(final_df, ws_actual)
        return html.Div(msg)
    return html.Div("No files uploaded.")

# ========== Auto Refresh Dashboard (Debug Version) ==========
@app.callback(
    Output('tabs-container', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_tabs(n):
    try:
        actual_data = ws_actual.get_all_values()
        budget_data = ws_budget.get_all_values()

        if not actual_data or not budget_data:
            return html.Div("❌ Failed to fetch data from Google Sheets.")

        actual_df = pd.DataFrame(actual_data[1:], columns=actual_data[0])
        budget_df = pd.DataFrame(budget_data[1:], columns=budget_data[0])
        
        # DEBUG: Print what we got
        print("=== DEBUG INFO ===")
        print(f"Actual columns: {actual_df.columns.tolist()}")
        print(f"Budget columns: {budget_df.columns.tolist()}")
        print(f"Actual shape: {actual_df.shape}")
        print(f"Budget shape: {budget_df.shape}")
        print(f"Actual data sample:\n{actual_df.head()}")
        print(f"Budget data sample:\n{budget_df.head()}")
        
        # Clean and standardize actual data
        if 'Month-Year' in actual_df.columns:
            actual_df = actual_df.drop('Month-Year', axis=1)
        
        # Ensure all required columns exist
        required_cols = ['Property', 'Date', 'Total Occ', 'Avg Rate', 'Revenue', 'Label']
        for col in required_cols:
            if col not in actual_df.columns:
                actual_df[col] = None
                print(f"Added missing column to actual_df: {col}")
            if col not in budget_df.columns:
                budget_df[col] = None
                print(f"Added missing column to budget_df: {col}")
        
        # Standardize columns
        actual_df = actual_df[required_cols]
        budget_df = budget_df[required_cols]
        
        print(f"After column standardization:")
        print(f"Actual df shape: {actual_df.shape}")
        print(f"Budget df shape: {budget_df.shape}")
        
        # Parse dates
        actual_df['Date'] = pd.to_datetime(actual_df['Date'], errors='coerce')
        budget_df['Date'] = pd.to_datetime(budget_df['Date'], format='%d-%b-%y', errors='coerce')
        
        print(f"After date parsing:")
        print(f"Actual dates sample: {actual_df['Date'].head()}")
        print(f"Budget dates sample: {budget_df['Date'].head()}")
        print(f"Actual null dates: {actual_df['Date'].isnull().sum()}")
        print(f"Budget null dates: {budget_df['Date'].isnull().sum()}")
        
        # Convert numeric columns
        for col in ['Total Occ', 'Avg Rate', 'Revenue']:
            actual_df[col] = actual_df[col].astype(str).str.replace(',', '', regex=False)
            budget_df[col] = budget_df[col].astype(str).str.replace(',', '', regex=False)
            
            actual_df[col] = pd.to_numeric(actual_df[col], errors='coerce')
            budget_df[col] = pd.to_numeric(budget_df[col], errors='coerce')

        # Create a comprehensive merge
        # First, get unique combinations of Property and Date from both datasets
        actual_df_clean = actual_df.dropna(subset=['Date'])
        budget_df_clean = budget_df.dropna(subset=['Date'])
        
        print(f"After cleaning:")
        print(f"Clean actual df shape: {actual_df_clean.shape}")
        print(f"Clean budget df shape: {budget_df_clean.shape}")
        
        if actual_df_clean.empty:
            return html.Div("❌ No valid actual data found after cleaning.")
        if budget_df_clean.empty:
            return html.Div("❌ No valid budget data found after cleaning.")
        
        # Merge on Property and Date
        merged_df = pd.merge(
            actual_df_clean, 
            budget_df_clean, 
            on=['Property', 'Date'], 
            how='outer', 
            suffixes=('_Actual', '_Budget')
        )
        
        print(f"After merge:")
        print(f"Merged df shape: {merged_df.shape}")
        print(f"Merged df columns: {merged_df.columns.tolist()}")
        
        if merged_df.empty:
            return html.Div("❌ No data after merge - check if Property and Date values match between sheets.")
        
        # Create final columns
        merged_df['Day'] = merged_df['Date'].dt.strftime('%d-%b')
        merged_df['Month'] = merged_df['Date'].dt.strftime('%B')
        
        # Rename columns for clarity
        merged_df.rename(columns={
            'Total Occ_Actual': 'Actual Occ',
            'Total Occ_Budget': 'Budget Occ',
            'Avg Rate_Actual': 'Actual Rate',
            'Avg Rate_Budget': 'Budget Rate',
            'Revenue_Actual': 'Actual Revenue',
            'Revenue_Budget': 'Budget Revenue',
            'Label_Actual': 'Label'
        }, inplace=True)
        
        # Fill missing Label with Budget Label if needed
        merged_df['Label'] = merged_df['Label'].fillna(merged_df.get('Label_Budget', ''))
        
        # Select final columns
        final_columns = ['Property', 'Date', 'Day', 'Month', 'Actual Occ', 'Budget Occ', 
                        'Actual Rate', 'Budget Rate', 'Actual Revenue', 'Budget Revenue', 'Label']
        
        # Ensure all columns exist
        for col in final_columns:
            if col not in merged_df.columns:
                merged_df[col] = None
        
        pivot_df = merged_df[final_columns]
        
        print(f"Final pivot_df shape: {pivot_df.shape}")
        print(f"Pivot_df sample:\n{pivot_df.head()}")
        
        # Keep only rows where we have actual data
        pivot_df = pivot_df[pd.notnull(pivot_df['Actual Occ'])]
        pivot_df = pivot_df.sort_values(by=['Property', 'Date']).reset_index(drop=True)
        
        print(f"After filtering for actual data: {pivot_df.shape}")
        
        if pivot_df.empty:
            return html.Div("❌ No rows with actual occupancy data found.")
        
        # Generate tabs
        tabs = []
        for prop, group in pivot_df.groupby("Property"):
            if not group.empty:
                print(f"Creating tab for property: {prop} with {len(group)} rows")
                tabs.append(
                    dcc.Tab(label=prop, children=[
                        make_table(group[['Day', 'Month', 'Actual Occ', 'Budget Occ', 
                                        'Actual Rate', 'Budget Rate', 'Actual Revenue', 'Budget Revenue', 'Label']])
                    ])
                )
        
        print(f"Created {len(tabs)} tabs")
        return dcc.Tabs(tabs)
    
    except Exception as e:
        print(f"Error in update_tabs: {str(e)}")
        import traceback
        traceback.print_exc()
        return html.Div(f"❌ Error: {str(e)}")

# ========== Run App ==========
if __name__ == '__main__':
    app.run_server(debug=True)
