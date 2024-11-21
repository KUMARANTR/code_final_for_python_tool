import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import pandas as pd
import pymysql
import Shared
from helper_functions import get_asset_path

# Assuming Shared.project and Shared.project_inbound are defined and configured
tbl = f"client_data.{Shared.project}"
inbound_datatbl = f"client_data.{Shared.project_inbound}"
sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')

# Connect to database and fetch metrics
def connect_to_database_inbound(start_date=None, end_date=None, dc_name=None):
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
        query = f"""
        SELECT 
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
        WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
        AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """

        cursor.execute(query)
        result = cursor.fetchone()

        metrics = {
            "Days of Data": result[0],
            "Total IB Loads": result[1],
            "Total Orders": result[2],
            "Total Lines": result[3],
            "Total Units": result[4],
            "SKUs with Movement": result[5],
            "IB Units Per Line": result[6],
            "IB Lines Per Order": result[7],
            "IB Units Per Order": result[8]
        }

        po_label = "PO" if result[2] > 0 else "Receipt"
        return metrics, po_label

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None
    finally:
        connection.close()

# Display metrics in the frame
def display_inbound_summary(inbound_metrics, po_label):
    if 'card_frame_inbound' not in globals() or card_frame_inbound is None:
        return

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

    col = 0
    for metric, value in inbound_metrics.items():
        display_metric = metric.replace("PO", po_label) if "PO" in metric else metric
        create_card(card_frame_inbound, display_metric, value, 1, col)
        col += 1

# Fetch and display metrics
def fetch_and_display_metrics():
    start_date = start_date_entry.get_date()
    end_date = end_date_entry.get_date()
    dc_name = dc_name_entry.get()

    metrics, po_label = connect_to_database_inbound(start_date, end_date, dc_name)
    if metrics:
        display_inbound_summary(metrics, po_label)
    else:
        messagebox.showerror("Error", "Failed to fetch metrics from the database.")

# GUI Setup
root = tk.Tk()
root.title("Inbound Data Summary")
root.geometry("800x600")

# Date range selection
tk.Label(root, text="Start Date").grid(row=0, column=0, padx=10, pady=5)
start_date_entry = DateEntry(root)
start_date_entry.grid(row=0, column=1, padx=10, pady=5)

tk.Label(root, text="End Date").grid(row=0, column=2, padx=10, pady=5)
end_date_entry = DateEntry(root)
end_date_entry.grid(row=0, column=3, padx=10, pady=5)

# Destination DC selection
tk.Label(root, text="Destination DC").grid(row=1, column=0, padx=10, pady=5)
dc_name_entry = ttk.Entry(root)
dc_name_entry.grid(row=1, column=1, padx=10, pady=5)
dc_name_entry.insert(0, "All")

# Fetch data button
fetch_button = tk.Button(root, text="Fetch Metrics", command=fetch_and_display_metrics)
fetch_button.grid(row=1, column=2, columnspan=2, padx=10, pady=5)

# Frame to hold metric cards
card_frame_inbound = tk.Frame(root, bg='lightgray')
card_frame_inbound.grid(row=2, column=0, columnspan=4, padx=10, pady=10, sticky="nsew")

root.mainloop()
