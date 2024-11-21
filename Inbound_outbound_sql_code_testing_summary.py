import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import pandas as pd
import pymysql
import Shared
from tkinter import filedialog
from tkinter import messagebox
# tbl = f"client_data.{Shared.project}"
# inbound_datatbl = f"client_data.{Shared.project_inbound}"
# sslca = 'server-ca.pem'
# sslkey = 'client-key.pem'
# sslcert = 'client-cert.pem'
import Shared
from helper_functions import get_asset_path

tbl = f"client_data.{Shared.project}"
inbound_datatbl = f"client_data.{Shared.project_inbound}"
sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')

# Helper function to format numbers with commas and three decimal places
# def format_number(value):
#     value = f"{value:,.3f}"
#     if value.endswith('.000'):
#         return value[:-4]  # Remove the .000
#     elif value.endswith('0'):
#         return value[:-1]  # Remove the last 0
#     return value
def format_number(value):
    # Check if value is None, and if so, return a default value or a placeholder
    if value is None:
        return "N/A"  # You can change "N/A" to any default value you prefer, such as "0" or "Not Available"

    # Otherwise, format the number
    formatted_value = f"{value:,.3f}"

    # Remove trailing .000 or trailing zeroes in decimals
    if formatted_value.endswith('.000'):
        return formatted_value[:-4]  # Remove the .000
    elif formatted_value.endswith('0'):
        return formatted_value[:-1]  # Remove the last 0

    return formatted_value


def connect_to_database_inbound(dc_name, start_date, end_date):
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
    if not Shared.project_inbound:
        return None
    select_query_inbound = f"""
    SELECT 
        DATEDIFF(MAX(Received_Date), MIN(Received_Date)) AS Days_of_Data,
        COUNT(DISTINCT Load_Number) AS Total_IB_Loads,
        COUNT(DISTINCT PO_Number) AS Total_PO,
        COUNT(DISTINCT Receipt_Number) AS Total_Receipt,
        COUNT(*) AS Total_Lines,
        SUM(Qty) AS Total_Units,
        COUNT(DISTINCT SKU) AS SKUs_with_Movement,
        COALESCE(SUM(Qty) / NULLIF(COUNT(*), 0), 0) AS IB_Units_Per_Line,
        COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT PO_Number), 0), 0) AS IB_Lines_Per_PO,
        COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT PO_Number), 0), 0) AS IB_Units_Per_PO,
        COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT Receipt_Number), 0), 0) AS IB_Lines_Per_Receipt,
        COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT Receipt_Number), 0), 0) AS IB_Units_Per_Receipt
    FROM {inbound_datatbl}
    WHERE Received_Date BETWEEN %s AND %s
    """
    if dc_name != "ALL":
        select_query_inbound += " AND Destination_DC = %s"
        params = (start_date, end_date, dc_name)
    else:
        params = (start_date, end_date)

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

        cursor = connection.cursor()
        cursor.execute(select_query_inbound, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        if not rows:
            print("No data found for inbound table.")
            return None

        df_inbound = pd.DataFrame(rows, columns=columns)
        return df_inbound

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()
# import threading
# def show_no_data_popup():
#     # Show a pop-up message if inbound data is not available
#     # Ensure the messagebox is called after the root is fully initialized
#     root.after(0, lambda: messagebox.showinfo("Inbound data is not available."))

def display_inbound_summary(inbound_data):
    # print(inbound_data)  # Debugging line to check if inbound_data is None
    # Check if inbound data is None and show the message if needed
    if inbound_data is None:
        # If you have a label in the GUI, update it with the message
        print("No inbound data available.")  # Debugging line
        global no_data_label
        if 'no_data_label' not in globals() or no_data_label is None:
            no_data_label = tk.Label(card_frame_inbound_label, text="Inbound data is not available.",
                                     font=('Arial', 12, 'bold'), fg='red')
            no_data_label.grid(row=0, column=0, columnspan=6, pady=10, sticky='w')
        return  # Exit the function if no data is available

    # Determine if PO_Number or Receipt_Number is available
    use_po = 'Total_PO' in inbound_data.columns and inbound_data['Total_PO'][0] > 0

    # Set label names dynamically based on available data
    po_or_receipt_label = "Total PO" if use_po else "Total Receipt"
    ib_lines_label = "IB Lines Per PO" if use_po else "IB Lines Per Receipt"
    ib_units_label = "IB Units Per PO" if use_po else "IB Units Per Receipt"

    # Set up metrics with dynamic labels
    inbound_metrics = {
        "Days of Data": format_number(inbound_data['Days_of_Data'][0]),
        "Total IB Loads": format_number(inbound_data['Total_IB_Loads'][0]),
        po_or_receipt_label: format_number(inbound_data['Total_PO'][0] if use_po else inbound_data['Total_Receipt'][0]),
        "Total Lines": format_number(inbound_data['Total_Lines'][0]),
        "Total Units": format_number(inbound_data['Total_Units'][0]),
        "SKUs with Movement": format_number(inbound_data['SKUs_with_Movement'][0]),
    }

    # Set up order profile metrics with dynamic labels
    inbound_order_profile_metrics = {
        "IB Units Per Line": format_number(inbound_data['IB_Units_Per_Line'][0]),
        ib_lines_label: format_number(
            inbound_data['IB_Lines_Per_PO'][0] if use_po else inbound_data['IB_Lines_Per_Receipt'][0]
        ),
        ib_units_label: format_number(
            inbound_data['IB_Units_Per_PO'][0] if use_po else inbound_data['IB_Units_Per_Receipt'][0]
        ),
    }

    # Clear the UI frame if it exists
    if 'card_frame_inbound' not in globals() or card_frame_inbound is None:
        return

    # Clear existing widgets
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

    # Add section labels
    tk.Label(card_frame_inbound, text="Inbound Volumes", font=('Arial', 14, 'bold'), anchor='w').grid(row=0, column=0, columnspan=6, pady=10, sticky='w')
    tk.Label(card_frame_inbound, text="Inbound Order Profile", font=('Arial', 14, 'bold'), anchor='w').grid(row=0, column=6, columnspan=3, pady=10, sticky='w')

    # Create cards for inbound metrics
    row, col, max_columns = 1, 0, 6
    for metric, value in inbound_metrics.items():
        create_card(card_frame_inbound, metric, value, row, col)
        col += 1
        if col >= max_columns:
            col = 0
            row += 1

    # Create cards for inbound order profile metrics
    row, col, max_columns = 1, 6, 3
    for metric, value in inbound_order_profile_metrics.items():
        create_card(card_frame_inbound, metric, value, row, col)
        col += 1
        if col >= max_columns + 6:
            col = 6
            row += 1

def get_distinct_dc_names():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
    # Check if inbound table is set
    if not Shared.project_inbound:
        # print("Inbound table name is missing. Skipping distinct DC names query.")
        return None
    try:
        conn = pymysql.connect(
            host='10.216.252.8',
            user=Shared.userid,
            passwd=Shared.password,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )
        cursor = conn.cursor()
        query = f"SELECT DISTINCT Destination_DC FROM {inbound_datatbl}"
        cursor.execute(query)
        dc_names = cursor.fetchall()
        cursor.close()
        conn.close()
        return ["ALL"] + [dc[0] for dc in dc_names]

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["ALL"]


def fetch_min_max_dates_inbound_og():
    global min_date_sql_inbound, max_date_sql_inbound
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"

    # Check if inbound table is set
    if not Shared.project_inbound:
        # print("Inbound table name is missing. Skipping date query for inbound data.")
        min_date_sql_inbound, max_date_sql_inbound = None, None
        return None
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

        cursor = connection.cursor()
        select_query_min_ib = f"""SELECT MIN(Received_Date), MAX(Received_Date) FROM {inbound_datatbl}"""
        cursor.execute(select_query_min_ib)
        min_date_sql_inbound, max_date_sql_inbound = cursor.fetchone()


        if min_date_sql_inbound and max_date_sql_inbound:
            min_date_sql_inbound = pd.Timestamp(min_date_sql_inbound)
            max_date_sql_inbound = pd.Timestamp(max_date_sql_inbound)
        else:
            print("No valid date data fetched!")
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
    finally:
        connection.close()



def fetch_min_max_dates_inbound():
    global min_date_sql_inbound, max_date_sql_inbound
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"

    # Check if inbound table is set
    if not Shared.project_inbound:
        # If no inbound table is defined, return None values for both dates
        #print("Inbound table name is missing. Skipping date query for inbound data.")
        min_date_sql_inbound, max_date_sql_inbound = None, None
        return None, None  # Return a tuple with None for both dates

    try:
        # Establish database connection
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

        cursor = connection.cursor()
        # Query to fetch min and max dates
        select_query_min_ib = f"""SELECT MIN(Received_Date), MAX(Received_Date) FROM {inbound_datatbl}"""
        cursor.execute(select_query_min_ib)

        # Fetch the result
        result = cursor.fetchone()

        if result:  # If there is any result from the query
            min_date_sql_inbound, max_date_sql_inbound = result
            if min_date_sql_inbound and max_date_sql_inbound:
                # Convert to pandas Timestamps for consistency
                min_date_sql_inbound = pd.Timestamp(min_date_sql_inbound)
                max_date_sql_inbound = pd.Timestamp(max_date_sql_inbound)
            else:
                print("No valid date data fetched!")
                min_date_sql_inbound, max_date_sql_inbound = None, None
        else:
            print("No data returned from the query!")
            min_date_sql_inbound, max_date_sql_inbound = None, None
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        min_date_sql_inbound, max_date_sql_inbound = None, None
    finally:
        # Ensure connection is closed even if an error occurs
        if connection:
            connection.close()

    # Return the dates (or None if there was an issue)
    return min_date_sql_inbound, max_date_sql_inbound


from tkinter import messagebox
# # Create a new frame for the filter options label
# filter_frame_inbound = tk.Frame(notebook)  # 'root' should be your main tkinter window or another parent container
# filter_frame_inbound.pack(side='top', fill='x')  # Adjust positioning as needed

# Define a placeholder for the inbound filter options label globally so it can be updated
filter_options_label_inbound = None  # Declare globally

# Function to display the filter options label
def update_filter_options_label_inbound(dc_name, start_date, end_date):
    global filter_options_label_inbound  # Declare it global within the function
    # Clear previous label if it exists
    if filter_options_label_inbound:
        filter_options_label_inbound.pack_forget()

    # Define the filter options text
    filter_text = f"DC Name: {dc_name}, Start Date: {start_date.strftime('%Y-%m-%d')}, End Date: {end_date.strftime('%Y-%m-%d')}"

    # Create and display the label with filter options
    filter_options_label_inbound = tk.Label(card_frame_inbound_label, text=filter_text, font=('Arial', 10, 'bold'))
    # filter_options_label_inbound.pack(pady=(10, 0))
    # Use grid instead of pack
    filter_options_label_inbound.grid(row=0, column=10, columnspan=3, pady=10, sticky='w')
    # Place the label without affecting the grid layout of other widgets
    # filter_options_label_inbound.place(x=10, y=10)  # Adjust x and y as needed for positioning


# Update the on_analyze_inbound_click function to call the filter options label update
def on_analyze_inbound_click():
    if not Shared.project_inbound:
        messagebox.showwarning("Warning", "Inbound table name is missing. Cannot analyze inbound data.")
        return None

    global dc_filter_inbound  # Ensure it's declared as global here too
    dc_name = dc_filter_inbound.get()
    start_date = order_start_date_entry.get_date()
    end_date = order_end_date_entry.get_date()

    # Ensure that the start date is earlier than the end date
    if start_date > end_date:
        messagebox.showerror("Error", "Start date cannot be later than end date.")
        return

    # Attempt to fetch inbound data
    inbound_data = connect_to_database_inbound(dc_name, start_date, end_date)

    # Check if inbound data is available
    if inbound_data is not None and not inbound_data.empty:
        display_inbound_summary(inbound_data)

        # Update the filter options label with selected filters
        update_filter_options_label_inbound(dc_name, start_date, end_date)
    else:
        messagebox.showinfo("No Data", "Inbound data is not available for the selected criteria.")


# def on_analyze_inbound_click():
#     if not Shared.project_inbound:
#         messagebox.showwarning("Warning", "Inbound table name is missing. Cannot analyze inbound data.")
#         return None
#
#     global dc_filter_inbound  # Ensure it's declared as global here too
#     dc_name = dc_filter_inbound.get()
#     start_date = order_start_date_entry.get_date()
#     end_date = order_end_date_entry.get_date()
#
#     # Ensure that the start date is earlier than the end date
#     if start_date > end_date:
#         messagebox.showerror("Error", "Start date cannot be later than end date.")
#         return
#
#     # Attempt to fetch inbound data
#     inbound_data = connect_to_database_inbound(dc_name, start_date, end_date)
#
#     # Check if inbound data is available
#     if inbound_data is not None and not inbound_data.empty:
#         display_inbound_summary(inbound_data)
#     else:
#         messagebox.showinfo("No Data", "Inbound data is not available for the selected criteria.")


# def on_analyze_inbound_click():
#     if not Shared.project_inbound:
#         # print("Inbound table name is missing. Skipping distinct DC names query.")
#         return None
#     global dc_filter_inbound  # Ensure it's declared as global here too
#     dc_name = dc_filter_inbound.get()
#     start_date = order_start_date_entry.get_date()
#     end_date = order_end_date_entry.get_date()
#
#     # Ensure that the start date is earlier than the end date
#     if start_date > end_date:
#         messagebox.showerror("Error", "Start date cannot be later than end date.")
#         return
#
#     inbound_data = connect_to_database_inbound(dc_name, start_date, end_date)
#     if inbound_data is not None:
#         display_inbound_summary(inbound_data)

def export_inbound_data():
    # Prompt user to select file name and location for saving the export
    file_path = filedialog.asksaveasfilename(
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        title="Save Inbound Data As"
    )

    # If the user cancels the save dialog, file_path will be empty
    if not file_path:
        messagebox.showinfo("Export Canceled", "Export was canceled.")
        return

    try:
        # Assuming `inbound_data` is your DataFrame to export
        inbound_data = connect_to_database_inbound(dc_filter_inbound.get(),
                                                   order_start_date_entry.get_date(),
                                                   order_end_date_entry.get_date())
        if inbound_data is not None:
            inbound_data.to_csv(file_path, index=False)
            messagebox.showinfo("Export Successful", f"Inbound data saved to {file_path}")
        else:
            messagebox.showerror("No Data", "No data available to export.")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred while exporting data: {str(e)}")


# Function to create a scrollable frame
def create_scrollable_frame(parent):
    canvas = tk.Canvas(parent)
    scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
    scrollable_frame = ttk.Frame(canvas)

    scrollable_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(
            scrollregion=canvas.bbox("all")
        )
    )

    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    return scrollable_frame

def connect_to_database_outbound(dc_name, bu_name, channel_name, start_date_outbound, end_date_outbound):
    tbl = f"client_data.{Shared.project}" if Shared.project else None
    if tbl is None:
        print("Error: Project name is not defined.")
        return None

    # SQL query modified to filter by dc_name, bu_name, channel_name, start_date, and end_date
    select_query_outbound = f"""
    SELECT 
        DATEDIFF(MAX(Order_Date), MIN(Order_Date)) AS Days_of_Data,
        COUNT(DISTINCT Order_Number) AS Total_Orders,
        COUNT(*) AS Total_Lines,
        SUM(Qty) AS Total_Units,
        COUNT(DISTINCT SKU) AS SKUs_with_Movement,
        COALESCE(SUM(Qty) / NULLIF(COUNT(*), 0), 0) AS OB_Units_Per_Line,
        COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT Order_Number), 0), 0) AS OB_Lines_Per_Order,
        COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT Order_Number), 0), 0) AS OB_Units_Per_Order
    FROM {tbl}
    WHERE Order_Date BETWEEN %s AND %s
    """

    # Initialize params with date range
    params = [start_date_outbound, end_date_outbound]

    # Add condition for a specific DC if dc_name is not "All"
    if dc_name != "ALL":
        select_query_outbound += " AND DC_Name = %s"
        params.append(dc_name)

    # Add condition for a specific BU if bu_name is not "All"
    if bu_name != "ALL":
        select_query_outbound += " AND Business_Unit = %s"
        params.append(bu_name)

    # Add condition for a specific Channel if channel_name is not "All"
    if channel_name != "ALL":
        select_query_outbound += " AND Order_Type = %s"
        params.append(channel_name)

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

        cursor = connection.cursor()
        cursor.execute(select_query_outbound, tuple(params))  # Convert params to tuple here
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        if not rows:
            print("No data found for outbound table.")
            return None

        df_outbound = pd.DataFrame(rows, columns=columns)
        return df_outbound

    except Exception as e:
        # print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()




def display_outbound_summary(outbound_data):


    # Dynamically adjust metrics based on PO or Receipt
    outbound_metrics = {
        "Days_of_Data": outbound_data['Days_of_Data'][0],
        "Total_Orders": format_number(outbound_data['Total_Orders'][0]),
        "Total_Lines": format_number(outbound_data['Total_Lines'][0]),
        "Total_Units": format_number(outbound_data['Total_Units'][0]),
        "SKUs_with_Movement": format_number(outbound_data['SKUs_with_Movement'][0]),
    }

    outbound_order_profile_metrics = {
        "OB_Units_Per_Line": format_number(outbound_data['OB_Units_Per_Line'][0]),
        "OB_Lines_Per_Order" : format_number(outbound_data['OB_Lines_Per_Order'][0]),
        "OB_Units_Per_Order" : format_number(outbound_data['OB_Units_Per_Order'][0])
    }

    # Debug: Print each metric and value to confirm correctness
    # print("Outbound Volumes Metrics:")
    # for metric, value in outbound_metrics.items():
    #     print(f"{metric}: {value}")
    #
    # print("Outbound Order Profile Metrics:")
    # for metric, value in outbound_order_profile_metrics.items():
    #     print(f"{metric}: {value}")
    # Clear existing widgets
    for widget in card_frame_outbound.winfo_children():
        widget.destroy()

    def create_card(parent, metric, value, row, col):
        frame = tk.Frame(parent, borderwidth=1, padx=5, pady=5, bg='white')
        frame.config(highlightbackground="red", highlightcolor="red", highlightthickness=2)
        frame.grid(row=row, column=col, padx=5, pady=5, ipadx=5, ipady=5)
        label_title = tk.Label(frame, text=metric, font=('Arial', 10, 'bold'), bg='white')
        label_title.pack(side='top', anchor='center')

        label_value = tk.Label(frame, text=str(value), font=('Arial', 10), bg='white')
        label_value.pack(side='top', anchor='center')

    outbound_volumes_label = tk.Label(card_frame_outbound, text="Outbound Volumes", font=('Arial', 14, 'bold'), anchor='w')
    outbound_volumes_label.grid(row=0, column=0, columnspan=6, pady=10, sticky='w')

    outbound_order_profile_label = tk.Label(card_frame_outbound, text="Outbound Order Profile", font=('Arial', 14, 'bold'),
                                           anchor='w')
    outbound_order_profile_label.grid(row=0, column=6, columnspan=3, pady=10, sticky='w')

    row = 1
    col = 0
    max_columns = 6
    for metric, value in outbound_metrics.items():
        create_card(card_frame_outbound, metric, value, row, col)
        col += 1
        if col >= max_columns:
            col = 0
            row += 1

    row = 1
    col = 6
    max_columns = 3
    for metric, value in outbound_order_profile_metrics.items():
        create_card(card_frame_outbound, metric, value, row, col)
        col += 1
        if col >= max_columns + 6:
            col = 6
            row += 1

def get_distinct_dc_names_outbound():
    tbl = f"client_data.{Shared.project}"
    try:
        # Set up your database connection (replace with your actual connection parameters)
        conn = pymysql.connect(
            host='10.216.252.8',  #
            user=Shared.userid,
            passwd=Shared.password,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )

        # Create a cursor to execute the query
        cursor = conn.cursor()

        # SQL query to get distinct DC_Name
        query = f"SELECT DISTINCT DC_Name FROM {tbl}"
        cursor.execute(query)

        # Fetch all distinct DC_Name outbound values from the result
        dc_names_outbound = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Extract the DC names outbound from the result tuples and add "All" at the beginning
        dc_values_outbound = ["ALL"] + [dc[0] for dc in dc_names_outbound]
        return dc_values_outbound

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["ALL"]  # Return "All" in case of error

def get_distinct_bu_filter_outbound():
    tbl = f"client_data.{Shared.project}"
    try:
        # Set up your database connection (replace with your actual connection parameters)
        conn = pymysql.connect(
            host='10.216.252.8',  #
            user=Shared.userid,
            passwd=Shared.password,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )

        # Create a cursor to execute the query
        cursor = conn.cursor()

        # SQL query to get distinct BU_NAME
        query = f"SELECT DISTINCT Business_Unit FROM {tbl}"
        cursor.execute(query)

        # Fetch all distinct Business_Unit outbound values from the result
        bu_names_outbound = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Extract the Business_Unit names outbound from the result tuples and add "All" at the beginning
        bu_values = ["ALL"] + [dc[0] for dc in bu_names_outbound]
        return bu_values

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["ALL"]  # Return "All" in case of error


def get_distinct_channel_filter_outbound():
    tbl = f"client_data.{Shared.project}"
    try:
        # Set up your database connection (replace with your actual connection parameters)
        conn = pymysql.connect(
            host='10.216.252.8',  #
            user=Shared.userid,
            passwd=Shared.password,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )

        # Create a cursor to execute the query
        cursor = conn.cursor()

        # SQL query to get distinct channel_NAME
        query = f"SELECT DISTINCT Order_Type FROM {tbl}"
        cursor.execute(query)

        # Fetch all distinct Order_Type outbound values from the result
        channel_names_outbound = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Extract the channel names outbound from the result tuples and add "All" at the beginning
        channel_values = ["ALL"] + [dc[0] for dc in channel_names_outbound]
        return channel_values

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["ALL"]  # Return "All" in case of error

def get_distinct_all_filter():
    tbl = f"client_data.{Shared.project}"
    try:
        # Set up your database connection (replace with your actual connection parameters)
        conn = pymysql.connect(
            host='10.216.252.8',  #
            user=Shared.userid,
            passwd=Shared.password,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )

        # Create a cursor to execute the query
        cursor = conn.cursor()

        # SQL query to get distinct channel_NAME
        query = f"SELECT DISTINCT DC_Name, Order_Type FROM {tbl}"
        cursor.execute(query)

        # Fetch all distinct Order_Type outbound values from the result
        channel_names_outbound = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Extract the channel names outbound from the result tuples and add "All" at the beginning
        channel_values = ["ALL"] + [dc[0] for dc in channel_names_outbound]
        return channel_values

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["ALL"]  # Return "All" in case of error


# Function to fetch min and max dates from the database////Minimum /maximum dates OUTBOUND dates
def fetch_min_max_dates_outbound():
    global min_date_sql_outbound, max_date_sql_outbound
    tbl = f"client_data.{Shared.project}"

    # print("Fetched min outbound date:", min_date_sql_outbound)
    # print("Fetched max outbound date:", max_date_sql_outbound)
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
        select_query_ob = f"""SELECT MIN(Order_Date), MAX(Order_Date) FROM {tbl}"""
        cursor.execute(select_query_ob)
        min_date_sql_outbound, max_date_sql_outbound = cursor.fetchone()
        # if min_date_sql_inbound and max_date_sql_inbound:
        #     min_date_sql_inbound = pd.Timestamp(min_date_sql_inbound)
        #     max_date_sql_inbound = pd.Timestamp(max_date_sql_inbound)
        # Validate data
        if min_date_sql_outbound and max_date_sql_outbound:
            min_date_sql_outbound = pd.Timestamp(min_date_sql_outbound)
            max_date_sql_outbound = pd.Timestamp(max_date_sql_outbound)
            # print(f"Data fetched successfully! Min date: {min_date_sql_outbound}, Max date: {max_date_sql_outbound}")
        else:
            print("No valid date data fetched!")
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
    finally:
        connection.close()

    return min_date_sql_outbound, max_date_sql_outbound

# Define a placeholder for the inbound filter options label globally so it can be updated
filter_options_label_outbound = None  # Declare globally

# Function to display the filter options label
def update_filter_options_label_outbound(dc_name,bu_name,channel_name, start_date_outbound,end_date_outbound):
    global filter_options_label_outbound  # Declare it global within the function
    # Clear previous label if it exists
    if filter_options_label_outbound:
        filter_options_label_outbound.pack_forget()

    # Define the filter options text
    filter_text = f"DC Name: {dc_name},Business Unit: {bu_name},Order Type: {channel_name}, Start Date: {start_date_outbound.strftime('%Y-%m-%d')}, End Date: {end_date_outbound.strftime('%Y-%m-%d')}"

    # Create and display the label with filter options
    filter_options_label_outbound = tk.Label(card_frame_outbound_label, text=filter_text, font=('Arial', 10, 'bold'))
    # filter_options_label_inbound.pack(pady=(10, 0))
    # Use grid instead of pack
    filter_options_label_outbound.grid(row=0, column=10, columnspan=3, pady=10, sticky='w')
 # Place the label without affecting the grid layout of other widgets
 #    filter_options_label_outbound.place(x=10, y=10)  # Adjust x and y as needed for positioning


def on_analyze_outbound_click():
    global dc_filter, bu_filter, channel_filter,dc_name,bu_name,channel_name,start_date_outbound,end_date_outbound
    start_date_outbound = order_start_date_entry_outbound.get_date()
    end_date_outbound = order_end_date_entry_outbound.get_date()
    dc_name = dc_filter.get()
    bu_name = bu_filter.get()
    channel_name = channel_filter.get()

    if start_date_outbound > end_date_outbound:
        messagebox.showerror("Error", "Start date cannot be later than end date.")
        return

    outbound_data = connect_to_database_outbound(dc_name, bu_name, channel_name, start_date_outbound, end_date_outbound)
    if outbound_data is not None:
        # print("Fetched Outbound Data:")
        # print(outbound_data)  # Check the data fetched from the database
        display_outbound_summary(outbound_data)
        # Update the filter options label with selected filters
        update_filter_options_label_outbound(dc_name, bu_name, channel_name, start_date_outbound, end_date_outbound)
    else:
        print("Outbound data is None")




# import pandas as pd
# from tkinter import filedialog, messagebox

# Define the export function
def export_outbound_data():
    # Prompt user to select the save location and filename
    file_path = filedialog.asksaveasfilename(
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv")],
        title="Save outbound data as"
    )

    # If the user cancels, return from the function
    if not file_path:
        messagebox.showinfo("Export Cancelled", "Export operation was cancelled.")
        return

    # Get filtered outbound data
    start_date_outbound = order_start_date_entry_outbound.get_date()
    end_date_outbound = order_end_date_entry_outbound.get_date()
    dc_name = dc_filter.get()
    bu_name = bu_filter.get()
    channel_name = channel_filter.get()

    outbound_data = connect_to_database_outbound(dc_name, bu_name, channel_name, start_date_outbound, end_date_outbound)

    # Check if outbound_data has been retrieved
    if outbound_data is not None:
        try:
            # Add filter criteria as new columns
            outbound_data['DC_Name'] = dc_name
            outbound_data['Business_Unit'] = bu_name
            outbound_data['Order_Type'] = channel_name  # Assuming 'Order_Type' maps to 'channel_name'
            outbound_data['Order_Date'] = f"{start_date_outbound} to {end_date_outbound}"

            # Define the columns to export
            export_columns = ["DC_Name", "Business_Unit", "Order_Type", "Order_Date"] + \
                             ["Days_of_Data", "Total_Orders", "Total_Lines", "Total_Units", "SKUs_with_Movement",
                              "OB_Units_Per_Line", "OB_Lines_Per_Order", "OB_Units_Per_Order"]

            # Check if all export_columns are present in outbound_data
            missing_columns = [col for col in export_columns if col not in outbound_data.columns]
            if missing_columns:
                messagebox.showerror("Missing Columns", f"The following columns are missing in the data: {', '.join(missing_columns)}")
                return

            # Filter outbound_data to include only the export_columns
            outbound_data_to_export = outbound_data[export_columns]

            # Save to CSV
            outbound_data_to_export.to_csv(file_path, index=False)
            messagebox.showinfo("Export Successful", f"Data exported successfully to {file_path}.")
        except Exception as e:
            messagebox.showerror("Export Error", f"An error occurred during export: {str(e)}")
    else:
        messagebox.showerror("No Data", "No data available to export with the current filters.")


# Need to define these variables outside the function so other files can access them
channel_names_outbound = None
bu_names_outbound = None
dc_names_outbound = None
min_date_sql_outbound = None
max_date_sql_outbound = None
min_date_sql_inbound = None
max_date_sql_inbound = None

def create_notebook_page_inbound_outbound_new(notebook):
    global card_frame_inbound, card_frame_outbound, inbound_data, outbound_data
    global card_frame_inbound, filter_frame_inbound, card_frame_outbound, filter_frame_outbound, root, chart_frame_inbound
    global selected_start_date, selected_end_date, selected_start_date_outbound, selected_end_date_outbound
    global inbound_data, outbound_data, order_start_date_entry, order_end_date_entry, min_date_sql_inbound, max_date_sql_inbound, min_date_sql_outbound, max_date_sql_outbound, order_start_date_entry_outbound, order_end_date_entry_outbound
    global export_button_frame_inbound, export_button_frame_outbound, order_start_date_entry_label, order_end_date_entry_label
    global dc_filter_inbound  # Declare dc_filter_inbound as global here
    global dc_filter, bu_filter, channel_filter, channel_names_outbound, bu_names_outbound, dc_names_outbound, min_date_sql_outbound, max_date_sql_outbound
    global card_frame_inbound_label,card_frame_outbound_label

    # # Date Filter Widgets
    # if inbound_data is not None:
    #fetch_min_max_dates_inbound()  # Fetch the min and max dates from the inbound table
    #
    #fetch_min_max_dates_outbound()  # Fetch the min and max dates from the outbound table
    #
    # Fetch DC Names INBOUND
    #dc_names = get_distinct_dc_names()
    dc_names = Shared.dc_names
    # Create Inbound and Outbound Summary Tab
    summary_frame = ttk.Frame(notebook, width=800, height=1000)
    summary_frame.grid_columnconfigure(1, weight=1)

    notebook.add(summary_frame, text="Summary")
    # Create a frame for filter widgets to align them horizontally
    scrollable_frame_inbound = create_scrollable_frame(summary_frame)
    filter_frame = tk.Frame(scrollable_frame_inbound)
    filter_frame.pack(pady=10)
    # Create a label for the "Inbound Data Summary" heading
    heading_label = ttk.Label(filter_frame, text="Inbound Data Summary", font=('Arial', 14, 'bold'),
                              background="#FF3B00", foreground="white")
    heading_label.pack(side="top", pady=(10, 0))  # Add padding at the top for spacing

    # Add a blank line (optional for extra spacing)
    spacer_label = ttk.Label(filter_frame, text="")
    spacer_label.pack(side="top", pady=(5, 5))  # Add vertical padding to create space
    dc_label_inbound = ttk.Label(filter_frame, text="DC Name:", font=('Arial', 10))
    dc_label_inbound.pack(side="left", padx=10)
    # DC Name Filter
    dc_filter_inbound = ttk.Combobox(filter_frame, values=dc_names, state="readonly")
    dc_filter_inbound.set("ALL")
    dc_filter_inbound.pack(side="left", padx=10)

    # # Date Filter Widgets INBOUND
    # fetch_min_max_dates_inbound()  # Fetch the min and max dates from the inbound table
    # Function to validate the selected dates
    def validate_dates_inbound(event=None):
        start_date = order_start_date_entry.get_date()
        end_date = order_end_date_entry.get_date()
        # Check if min_date_sql_inbound and max_date_sql_inbound are not None
        if min_date_sql_inbound is None or max_date_sql_inbound is None:
            messagebox.showerror("Invalid Date", "Date range from the database is not available.")
            return

        # Convert min_date_sql_inbound and max_date_sql_inbound to date for comparison if they are not None
        min_date_sql_inbound_date = Shared.min_date_sql_inbound.date() if Shared.min_date_sql_inbound else None
        max_date_sql_inbound_date = Shared.max_date_sql_inbound.date() if Shared.max_date_sql_inbound else None

        # # Convert min_date_sql_inbound and max_date_sql_inbound to date for comparison
        # min_date_sql_inbound_date = min_date_sql_inbound.date()
        # max_date_sql_inbound_date = max_date_sql_inbound.date()

        # Check if start date is earlier than min date
        if start_date < min_date_sql_inbound_date:
            messagebox.showerror("Invalid Date", "Selected start date is earlier than available data start date.")
            order_start_date_entry.set_date(min_date_sql_inbound_date)  # Reset to the min date
            return

        # Check if end date is later than max date
        if end_date > max_date_sql_inbound_date:
            messagebox.showerror("Invalid Date", "Selected end date is later than available data end date.")
            order_end_date_entry.set_date(max_date_sql_inbound_date)  # Reset to the max date
            return

        # Check if start date is after end date
        if start_date > end_date:
            messagebox.showerror("Invalid Date", "Start date cannot be later than end date.")
            order_start_date_entry.set_date(min_date_sql_inbound_date)  # Reset start date to the min date
            order_end_date_entry.set_date(max_date_sql_inbound_date)  # Reset end date to the max date

    # Order Start Date
    order_start_date_entry_label = tk.Label(filter_frame, text="Start Date:")
    order_start_date_entry_label.pack(side='left', padx=10)
    order_start_date_entry = DateEntry(filter_frame, width=12, background='darkblue', foreground='white', borderwidth=2)
    order_start_date_entry.set_date(Shared.min_date_sql_inbound)  # Default to the min date
    order_start_date_entry.pack(side='left', padx=10)
    order_start_date_entry.bind("<<DateEntrySelected>>",validate_dates_inbound)  # Bind validation to date selection
    # Order End Date
    order_end_date_entry_label = tk.Label(filter_frame, text="End Date:")
    order_end_date_entry_label.pack(side='left', padx=10)
    order_end_date_entry = DateEntry(filter_frame, width=12, background='darkblue', foreground='white', borderwidth=2)
    order_end_date_entry.set_date(Shared.max_date_sql_inbound)  # Default to the max date
    order_end_date_entry.pack(side='left', padx=10)
    order_end_date_entry.bind("<<DateEntrySelected>>", validate_dates_inbound)  # Bind validation to date selection
    # Analyze Button
    analyze_button_inbound = tk.Button(filter_frame, text="Analyze Inbound", command=on_analyze_inbound_click)
    analyze_button_inbound.pack(side='left', padx=10)


    # Add Export Button
    export_button = tk.Button(filter_frame, text="Export Inbound", command=export_inbound_data)
    export_button.pack(pady=10)

    # Frame for displaying metrics
    card_frame_inbound = tk.Frame(scrollable_frame_inbound)
    card_frame_inbound.pack(pady=20)
    card_frame_inbound_label = tk.Frame(scrollable_frame_inbound)
    card_frame_inbound_label.pack(pady=20)
    # Fetch DC Names
    #dc_names_outbound = get_distinct_dc_names_outbound()
    dc_names_outbound = Shared.dc_names_outbound
    # Create a frame for filter widgets to align them horizontally
    filter_frame_outbound = tk.Frame(scrollable_frame_inbound)
    filter_frame_outbound.pack(pady=10)
    heading_label_outbound = ttk.Label(filter_frame_outbound, text="Outbound Data Summary", font=('Arial', 14, 'bold'),
                                       background="#FF3B00", foreground="white")
    heading_label_outbound.pack(side="top", pady=(10, 0))  # Add padding at the top for spacing

    # Add a blank line (optional for extra spacing)
    spacer_label_outbound = ttk.Label(filter_frame_outbound, text="")
    spacer_label_outbound.pack(side="top", pady=(5, 5))  # Add vertical padding to create space
    # DC Name Filter
    dc_label_outbound = ttk.Label(filter_frame_outbound, text="DC Name:", font=('Arial', 10))
    dc_label_outbound.pack(side="left", padx=10)
    dc_filter = ttk.Combobox(filter_frame_outbound, values=dc_names_outbound, state='readonly', font=('Arial', 10))
    dc_filter.set("ALL")  # Set the default value to "All"
    dc_filter.pack(side="left", padx=10)
    # Fetch BU Names
    #bu_names_outbound = get_distinct_bu_filter_outbound()
    bu_names_outbound = Shared.bu_names_outbound
    # BU Name Filter
    bu_label = ttk.Label(filter_frame_outbound, text="Business Unit:", font=('Arial', 10))
    bu_label.pack(side="left", padx=10)
    bu_filter = ttk.Combobox(filter_frame_outbound, values=bu_names_outbound, state='readonly', font=('Arial', 10))
    bu_filter.set("ALL")  # Set default value
    bu_filter.pack(side="left", padx=10)
    # Fetch channel Names
    #channel_names_outbound = get_distinct_channel_filter_outbound()
    channel_names_outbound = Shared.channel_names_outbound
    # Channel Filter
    channel_label = ttk.Label(filter_frame_outbound, text="Order Type:", font=('Arial', 10))
    channel_label.pack(side="left", padx=10)
    channel_filter = ttk.Combobox(filter_frame_outbound, values=channel_names_outbound, state='readonly', font=('Arial', 10))
    channel_filter.set("ALL")  # Set default value
    channel_filter.pack(side="left", padx=10)
    # Date Filter Widgets
    # fetch_min_max_dates_outbound()  # Fetch the min and max dates from the outbound table
    # Function to validate the selected dates
    def validate_dates_outbound(event=None):
        start_date_outbound = order_start_date_entry_outbound.get_date()
        end_date_outbound = order_end_date_entry_outbound.get_date()

        # Convert min_date_sql_outbound and max_date_sql_outbound to date for comparison
        min_date_sql_outbound_date = Shared.min_date_sql_outbound.date()
        max_date_sql_outbound_date = Shared.max_date_sql_outbound.date()

        # Check if start date is earlier than min date
        if start_date_outbound < min_date_sql_outbound_date:
            messagebox.showerror("Invalid Date", "Selected start date is earlier than available data start date.")
            order_start_date_entry_outbound.set_date(min_date_sql_outbound_date)  # Reset to the min date
            return

        # Check if end date is later than max date
        if end_date_outbound > max_date_sql_outbound_date:
            messagebox.showerror("Invalid Date", "Selected end date is later than available data end date.")
            order_end_date_entry_outbound.set_date(max_date_sql_outbound_date)  # Reset to the max date
            return

        # Check if start date is after end date
        if start_date_outbound > end_date_outbound:
            messagebox.showerror("Invalid Date", "Start date cannot be later than end date.")
            order_start_date_entry_outbound.set_date(min_date_sql_outbound_date)  # Reset start date to the min date
            order_end_date_entry_outbound.set_date(max_date_sql_outbound_date)  # Reset end date to the max date

    # Order Start Date
    order_start_date_entry_outbound_label = tk.Label(filter_frame_outbound,
                                                     text=f"Start Date:")
    order_start_date_entry_outbound_label.pack(side='left', padx=10)
    order_start_date_entry_outbound = DateEntry(filter_frame_outbound, width=12, background='darkblue', foreground='white',
                                                borderwidth=2)
    order_start_date_entry_outbound.set_date(Shared.min_date_sql_outbound)  # Default to the min date
    order_start_date_entry_outbound.pack(side='left', padx=10)
    order_start_date_entry_outbound.bind("<<DateEntrySelected>>", validate_dates_outbound)  # Bind validation to date selection
    # Order End Date
    order_end_date_entry_outbound_label = tk.Label(filter_frame_outbound,
                                                   text=f"End Date:")
    order_end_date_entry_outbound_label.pack(side='left', padx=10)
    order_end_date_entry_outbound = DateEntry(filter_frame_outbound, width=12, background='darkblue', foreground='white',
                                              borderwidth=2)
    order_end_date_entry_outbound.set_date(Shared.max_date_sql_outbound)  # Default to the max date
    order_end_date_entry_outbound.pack(side='left', padx=10)
    order_end_date_entry_outbound.bind("<<DateEntrySelected>>", validate_dates_outbound)  # Bind validation to date selection
    # Analyze Button
    analyze_button_outbound = tk.Button(filter_frame_outbound, text="Analyze Outbound", command=on_analyze_outbound_click)
    analyze_button_outbound.pack(side='left', padx=10)

    # Export button to export data
    export_button = tk.Button(filter_frame_outbound, text="Export Outbound ", command=export_outbound_data)
    export_button.pack(pady=10)
    # Frame for displaying metrics
    card_frame_outbound = tk.Frame(scrollable_frame_inbound)
    card_frame_outbound.pack(pady=20)
    card_frame_outbound_label = tk.Frame(scrollable_frame_inbound)
    card_frame_outbound_label.pack(pady=20)
    # Export Button
    style = ttk.Style()
    style.configure('Export.TButton', background='white', foreground='#FF3B00')

    # root.mainloop()
def main():
    global dc_filter_inbound  # Declare dc_filter_inbound as global here as well
    global dc_filter,bu_filter,channel_filter
    # Initialize root window
    root = tk.Tk()
    root.title("Summary Page")
    root.geometry("1200x600")
    # Fetch the min and max dates for inbound before creating the notebook page
    #fetch_min_max_dates_inbound()  # Ensure dates are fetched before the page is created
    #fetch_min_max_dates_outbound()  # Fetch the min and max dates from the outbound table

    # # Create a Notebook to hold both tabs
    notebook = ttk.Notebook(root)
    notebook.pack(side='top', fill='both', expand=True)
    create_notebook_page_inbound_outbound_new(notebook)
    # SeasonalityApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
