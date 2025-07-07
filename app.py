import base64
import io
import os
import pandas as pd
import gspread
from datetime import datetime
from dash import Dash, dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
from oauth2client.service_account import ServiceAccountCredentials

# Decode base64 and save credentials.json at runtime
if not os.path.exists("credentials.json"):
    encoded = os.environ.get("GOOGLE_CREDENTIALS_B64")
    if encoded:
        with open("credentials.json", "wb") as f:
            f.write(base64.b64decode(encoded))
    else:
        raise Exception("GOOGLE_CREDENTIALS_B64 environment variable not set.")

# ========== Google Sheets Setup ==========
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
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

    df = df[['Date', 'Total Occ']].copy()
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

    return df[['Property', 'Date', 'Total Occ', 'Label', 'Month-Year']]

# ========== Update Google Sheet ==========
def update_google_sheet(processed_df, worksheet):
    existing_data = worksheet.get_all_values()
    existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0]) if existing_data else pd.DataFrame()
    if existing_df.empty:
        existing_df = pd.DataFrame(columns=['Property', 'Date', 'Total Occ', 'Label', 'Month-Year'])

    processed_df['Date'] = pd.to_datetime(processed_df['Date'])
    existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
    processed_df["Month-Year"] = processed_df["Month-Year"].astype(str)
    existing_df["Month-Year"] = existing_df["Month-Year"].astype(str)

    for (prop, month) in processed_df[['Property', 'Month-Year']].drop_duplicates().values:
        existing_df = existing_df[~((existing_df["Property"] == prop) & (existing_df["Month-Year"] == month))]

    updated_df = pd.concat([existing_df, processed_df], ignore_index=True)
    updated_df = updated_df.sort_values(by=["Property", "Date"])
    worksheet.clear()
    worksheet.update([updated_df.columns.tolist()] + updated_df.astype(str).values.tolist())

    return f"✅ {len(processed_df)} rows updated for {processed_df['Property'].nunique()} property(ies)."

# ========== Create Colored Table ==========
def make_table(data):
    return dash_table.DataTable(
        columns=[{"name": col, "id": col} for col in ['Day', 'Month', 'Actual', 'Budget', 'Label']],
        data=data.to_dict('records'),
        style_table={'overflowY': 'auto', 'height': '600px'},
        style_cell={'textAlign': 'center'},
        style_header={'fontWeight': 'bold'},
        style_data_conditional=[
            {'if': {'filter_query': '{Label} = "History"'}, 'backgroundColor': '#e6f2ff'},
            {'if': {'filter_query': '{Actual} > {Budget} && {Label} = "Forecast"', 'column_id': 'Actual'}, 'backgroundColor': '#d4edda'},
            {'if': {'filter_query': '{Actual} < {Budget} && {Label} = "Forecast"', 'column_id': 'Actual'}, 'backgroundColor': '#f8d7da'}
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

# ========== Auto Refresh Dashboard ==========
@app.callback(
    Output('tabs-container', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_tabs(n):
    actual_data = ws_actual.get_all_values()
    budget_data = ws_budget.get_all_values()

    if not actual_data or not budget_data:
        return html.Div("❌ Failed to fetch data from Google Sheets.")

    actual_df = pd.DataFrame(actual_data[1:], columns=actual_data[0])
    budget_df = pd.DataFrame(budget_data[1:], columns=budget_data[0])
    
    actual_df['Type'] = 'Actual'
    budget_df['Type'] = 'Budget'
    actual_df = actual_df.drop('Month-Year',axis=1)
    
    # Standardize columns
    actual_df = actual_df[['Property', 'Date', 'Total Occ', 'Label', 'Type']]
    budget_df = budget_df[['Property', 'Date', 'Total Occ', 'Label', 'Type']]

    # Standardize columns
    actual_df = actual_df[['Property', 'Date', 'Total Occ', 'Label', 'Type']]
    budget_df = budget_df[['Property', 'Date', 'Total Occ', 'Label', 'Type']]
    
    actual_df['Date'] = pd.to_datetime(actual_df['Date'], errors='coerce')
    
    # Fix for budget: parse from format like '1-Jul-25'
    budget_df['Date'] = pd.to_datetime(budget_df['Date'], format='%d-%b-%y', errors='coerce')

    # Merge vertically
    df = pd.concat([actual_df, budget_df], ignore_index=True)
    # Clean types
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Total Occ'] = pd.to_numeric(df['Total Occ'], errors='coerce')
    
    # Pivot so Actual and Budget become columns
    pivot_df = df.pivot_table(
        index=['Property', 'Date', 'Label'],
        columns='Type',
        values='Total Occ',
        aggfunc='first'  # or sum if multiple rows per date
    ).reset_index()

    pivot_df['Day'] = pivot_df['Date'].dt.strftime('%d-%b')
    pivot_df['Month'] = pivot_df['Date'].dt.strftime('%B')
    
    pivot_df = pivot_df[['Property', 'Date', 'Day', 'Month', 'Actual', 'Budget', 'Label']]
    pivot_df = pivot_df.sort_values(by=['Property', 'Date'])
    # Keep only rows where Actual is present (not NaN)
    pivot_df = pivot_df[pd.notnull(pivot_df['Actual'])]
    
    # Optional: reset index if needed
    pivot_df = pivot_df.sort_values(by='Date').reset_index(drop=True)
    # Generate tabs
    tabs = [
        dcc.Tab(label=prop, children=[
            make_table(group[['Day', 'Month', 'Actual', 'Budget', 'Label']])
        ])
        for prop, group in pivot_df.groupby("Property")
    ]

    return dcc.Tabs(tabs)


# ========== Run App ==========
if __name__ == '__main__':
    app.run_server(debug=True)
