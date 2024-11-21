import tkinter as tk
from tkinter import ttk
import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
import Shared
# Global variables or Shared configurations
from helper_functions import get_asset_path
from tkcalendar import DateEntry

tbl = f"client_data.{Shared.project}"
# inbound_datatbl = f"client_data.{Shared.project_inbound}"
sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')
inbound_datatbl = 'client_data.INBOUND_STD_SAMPLE'

# Helper function to format numbers with commas and three decimal places
def format_number(value):
    # Format the number with three decimal places and convert to string
    value = f"{value:,.3f}"
    # Remove the trailing zeros after the decimal point, if any
    if value.endswith('.000'):
        return value[:-4]  # Remove the .000
    elif value.endswith('0'):
        return value[:-1]  # Remove the last 0
    return value  # Return the formatted value



# Function to connect to the database with optional filters
def connect_to_database_inbound(dc_name=None, start_date=None, end_date=None):
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
    inbound_datatbl = f'client_data.INBOUND_STD_SAMPLE'
    if not Shared.project_inbound:
        return None

    connection = pymysql.connect(
        host='10.216.252.8',
        user=Shared.userid,
        passwd=Shared.password,
        db='client_data',
        port=3306,
        ssl_ca=sslca,
        ssl_key=sslkey,
        ssl_cert=sslcert
    )

    try:
        cursor = connection.cursor()

        # Modify SQL query to include filters
        select_query = f"""SELECT 
            DATEDIFF(MAX(Received_Date), MIN(Received_Date)) AS Days_of_Data,
            COUNT(DISTINCT Load_Number) AS Total_IB_Loads,
            COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)) AS Total_Orders,
            COUNT(*) AS Total_Lines,
            SUM(Qty) AS Total_Units,
            COUNT(DISTINCT SKU) AS SKUs_with_Movement,
            COALESCE(SUM(Qty) / NULLIF(COUNT(*), 0), 0) AS IB_Units_Per_Line,
            COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)), 0), 0) AS IB_Lines_Per_Order,
            COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)), 0), 0) AS IB_Units_Per_Order
        FROM {inbound_datatbl}
        WHERE 1=1"""

        # Add DC Name filter if specified
        if dc_name:
            select_query += " AND Destination_DC = %s"

        # Add date filters if specified
        if start_date and end_date:
            select_query += " AND Received_Date BETWEEN %s AND %s"

        # Execute query with parameters
        params = [param for param in [dc_name, start_date, end_date] if param is not None]
        cursor.execute(select_query, params)

        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        if not rows:
            print("No data found for inbound table.")
            return None

        df_inbound = pd.DataFrame(rows, columns=columns)
        return df_inbound

    finally:
        connection.close()


# Function to get distinct DC names
def get_dc_names():
    inbound_datatbl = f'client_data.INBOUND_STD_SAMPLE'
    query = f"SELECT DISTINCT Destination_DC FROM {inbound_datatbl}"
    try:
        connection = pymysql.connect(
            host='10.216.252.8',
            user=Shared.userid,
            passwd=Shared.password,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )
        with connection.cursor() as cursor:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()


def get_date_range():
    inbound_datatbl = f'client_data.INBOUND_STD_SAMPLE'
    query = f"SELECT MIN(Received_Date), MAX(Received_Date) FROM {inbound_datatbl}"
    try:
        connection = pymysql.connect(
            host='10.216.252.8',
            user=Shared.userid,
            passwd=Shared.password,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0].strftime("%Y-%m-%d"), result[1].strftime("%Y-%m-%d")
    finally:
        connection.close()


# Display function for inbound summary metrics
def display_inbound_summary(inbound_metrics):
    print("Inbound Metrics:", inbound_metrics)  # Verify inbound metrics dictionary
    for widget in card_frame_inbound.winfo_children():
        widget.destroy()

    def create_card(parent, metric, value, row, col):
        frame = tk.Frame(parent, borderwidth=1, padx=5, pady=5, bg='white')
        frame.config(highlightbackground="red", highlightcolor="red", highlightthickness=2)
        frame.grid(row=row, column=col, padx=5, pady=5, ipadx=5, ipady=5)
        label_title = tk.Label(frame, text=metric, font=('Arial', 10, 'bold'), bg='white')
        label_title.pack(side='top', anchor='center')
        label_value = tk.Label(frame, text=str(value), font=('Arial', 10), bg='white')
        label_value.pack(side='top', anchor='center')

    row, col = 0, 0
    for metric, value in inbound_metrics.items():
        create_card(card_frame_inbound, metric, value, row, col)
        col += 1


# Event handler for analyze button click
def on_analyze_click():
    dc_name = dc_name_var.get()
    start_date = start_date_var.get()
    end_date = end_date_var.get()

    df_inbound = connect_to_database_inbound(dc_name, start_date, end_date)
    if df_inbound is not None:
        print(df_inbound)  # Add this line to verify the fetched data
        inbound_metrics = df_inbound.iloc[0].to_dict()
        display_inbound_summary(inbound_metrics)
    else:
        print("No data returned from the database.")



# Main GUI setup
root = tk.Tk()
root.title("Inbound Data Analyzer")

# DC Name filter
dc_name_var = tk.StringVar()
dc_name_combo = ttk.Combobox(root, textvariable=dc_name_var, values=get_dc_names())
dc_name_combo.grid(row=0, column=1)
tk.Label(root, text="Select DC Name").grid(row=0, column=0)

from datetime import datetime
from tkcalendar import DateEntry

# Fetch the date range and ensure they are datetime.date objects
min_date_str, max_date_str = get_date_range()
min_date = datetime.strptime(min_date_str, "%Y-%m-%d").date()
max_date = datetime.strptime(max_date_str, "%Y-%m-%d").date()

# Start Date filter with DateEntry
start_date_var = tk.StringVar(value=min_date_str)
start_date_entry = DateEntry(root, textvariable=start_date_var, date_pattern='yyyy-mm-dd',
                             mindate=min_date, maxdate=max_date)
start_date_entry.grid(row=1, column=1)
tk.Label(root, text="Start Date").grid(row=1, column=0)

# End Date filter with DateEntry
end_date_var = tk.StringVar(value=max_date_str)
end_date_entry = DateEntry(root, textvariable=end_date_var, date_pattern='yyyy-mm-dd',
                           mindate=min_date, maxdate=max_date)
end_date_entry.grid(row=2, column=1)
tk.Label(root, text="End Date").grid(row=2, column=0)
# Analyze button
analyze_button = tk.Button(root, text="Analyze Inbound", command=on_analyze_click)
analyze_button.grid(row=3, column=0, columnspan=2)

# Output frame for inbound summary
card_frame_inbound = tk.Frame(root)
card_frame_inbound.grid(row=4, column=0, columnspan=2)

# Run the application
root.mainloop()
