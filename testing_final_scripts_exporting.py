import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import pandas as pd
import pymysql
import Shared
from helper_functions import get_asset_path

tbl = f"client_data.{Shared.project}"
inbound_datatbl = f"client_data.{Shared.project_inbound}"
sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')


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


def connect_to_database_inbound():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
    # Check if inbound table is set
    if not Shared.project_inbound:
        # print("Inbound table name is missing. Skipping inbound data query.")
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
        select_query = f"""SELECT 
            DATEDIFF(MAX(Received_Date), MIN(Received_Date)) AS Days_of_Data,
            COUNT(DISTINCT Load_Number) AS Total_IB_Loads,
            COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)) AS Total_Orders,
            COUNT(*) AS Total_Lines,
            SUM(Qty) AS Total_Units,
            COUNT(DISTINCT SKU) AS SKUs_with_Movement,
            COALESCE(SUM(Qty) / NULLIF(COUNT(*), 0), 0) AS IB_Units_Per_Line,
            COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)), 0), 0) AS IB_Lines_Per_Order,
            COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)), 0), 0) AS IB_Units_Per_Order,
            
        FROM {inbound_datatbl}"""
        # print(f"Inbound Data Query: {select_query}")  # Debugging: Print query
        cursor.execute(select_query)

        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        # If no rows are returned, return None
        if not rows:
            print("No data found for inbound table.")
            return None

        df_inbound = pd.DataFrame(rows, columns=columns)

        # Check if PO_Number is blank and replace it with Receipt_Number if so
        if df_inbound['PO_Number'].isnull().all():
            df_inbound['PO_Number'] = df_inbound['Receipt_Number']
        # print("Data fetched successfully!")
        return df_inbound

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()



def inbound_volumes(inbound_data, start_date=None, end_date=None, dc_name=None):
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"

    # Check if inbound table is set
    if not Shared.project_inbound:
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

        # Determine po_label based on the presence of PO_Number
        cursor.execute(f"""
            SELECT COUNT(PO_Number)
            FROM {inbound_datatbl}
            WHERE PO_Number IS NOT NULL AND PO_Number != ''
            AND (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        po_count_result = cursor.fetchone()
        po_count = po_count_result[0] if po_count_result else 0
        po_label = "PO" if po_count > 0 else "Receipt"

        # Execute each SQL query and fetch the result
        cursor.execute(f"""
            SELECT DATEDIFF(MAX(Received_Date), MIN(Received_Date)) AS Days_of_Data
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        days_of_data_result = cursor.fetchone()
        days_of_data = days_of_data_result[0] if days_of_data_result else 0

        cursor.execute(f"""
            SELECT COUNT(DISTINCT Load_Number) AS Total_IB_Loads
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        total_ib_loads_result = cursor.fetchone()
        total_ib_loads = total_ib_loads_result[0] if total_ib_loads_result else 0

        cursor.execute(f"""
            SELECT COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)) AS Total_Orders
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        total_orders_result = cursor.fetchone()
        total_orders = total_orders_result[0] if total_orders_result else 0

        cursor.execute(f"""
            SELECT COUNT(*) AS Total_Lines
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        total_lines_result = cursor.fetchone()
        total_lines = total_lines_result[0] if total_lines_result else 0

        cursor.execute(f"""
            SELECT SUM(Qty) AS Total_Units
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        total_units_result = cursor.fetchone()
        total_units = total_units_result[0] if total_units_result else 0

        cursor.execute(f"""
            SELECT COUNT(DISTINCT SKU) AS SKUs_With_Movement
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        skus_with_movement_result = cursor.fetchone()
        skus_with_movement = skus_with_movement_result[0] if skus_with_movement_result else 0

        metrics = {
            "Days of Data": format_number(days_of_data),
            "Total IB Loads": format_number(total_ib_loads),
            f"Total {po_label}": format_number(total_orders),
            "Total Lines": format_number(total_lines),
            "Total Units": format_number(total_units),
            "SKUs with Movement": format_number(skus_with_movement)
        }
        return metrics, po_label

    finally:
        connection.close()

def inbound_order_profile(inbound_data, start_date=None, end_date=None, dc_name=None):
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
    # Check if inbound table is set
    if not Shared.project_inbound:
        # print("Inbound table name is missing. Skipping inbound data query.")
        return None

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

        # Determine po_label based on the presence of PO_Number
        cursor.execute(f"""
            SELECT COUNT(PO_Number)
            FROM {inbound_datatbl}
            WHERE PO_Number IS NOT NULL AND PO_Number != ''
            AND (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        po_count_result = cursor.fetchone()
        po_count = po_count_result[0] if po_count_result else 0
        po_label = "PO" if po_count > 0 else "Receipt"

        # Execute each SQL query and fetch the result
        cursor.execute(f"""
            SELECT COALESCE(SUM(Qty) / NULLIF(COUNT(*), 0), 0) AS IB_Units_Per_Line
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        ib_units_per_line_result = cursor.fetchone()
        ib_units_per_line = ib_units_per_line_result[0] if ib_units_per_line_result else 0

        cursor.execute(f"""
            SELECT COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)), 0), 0) AS IB_Lines_Per_{po_label}
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        ib_lines_per_po_result = cursor.fetchone()
        ib_lines_per_po = ib_lines_per_po_result[0] if ib_lines_per_po_result else 0

        cursor.execute(f"""
            SELECT COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT COALESCE(PO_Number, Receipt_Number)), 0), 0) AS IB_Units_Per_{po_label}
            FROM {inbound_datatbl}
            WHERE (Received_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (Destination_DC = '{dc_name}' OR '{dc_name}' = 'All');
        """)
        ib_units_per_po_result = cursor.fetchone()
        ib_units_per_po = ib_units_per_po_result[0] if ib_units_per_po_result else 0

        # Define the metrics with the dynamic po_label
        metrics = {
            "IB Units Per Line": ib_units_per_line,
            f"IB Lines Per {po_label}": ib_lines_per_po,  # Dynamically set label
            f"IB Units Per {po_label}": ib_units_per_po
        }
        return metrics, po_label

    finally:
        connection.close()

def display_inbound_summary(inbound_metrics, inbound_order_profile_metrics, inbound_data, start_date, end_date, dc_name,
                            po_label):
    # Ensure card_frame_inbound is defined
    if 'card_frame_inbound' not in globals() or card_frame_inbound is None:
        # print("Error: card_frame_inbound is not defined.")
        return
    if not inbound_metrics or not inbound_order_profile_metrics:
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

    inbound_volumes_label = tk.Label(card_frame_inbound, text="Inbound Volumes", font=('Arial', 14, 'bold'), anchor='w')
    inbound_volumes_label.grid(row=0, column=0, columnspan=6, pady=10, sticky='w')

    inbound_order_profile_label = tk.Label(card_frame_inbound, text="Inbound Order Profile", font=('Arial', 14, 'bold'),
                                           anchor='w')
    inbound_order_profile_label.grid(row=0, column=6, columnspan=3, pady=10, sticky='w')

    # Dynamically update the labels with `po_label`
    col = 0
    for metric, value in inbound_metrics.items():
        # Update the metric label if it contains "PO" to include `po_label`
        display_metric = metric.replace("PO", po_label) if "PO" in metric else metric
        create_card(card_frame_inbound, display_metric, value, 1, col)
        col += 1

    col = 6
    for metric, value in inbound_order_profile_metrics.items():
        display_metric = metric.replace("PO", po_label) if "PO" in metric else metric
        create_card(card_frame_inbound, display_metric, value, 1, col)
        col += 1

def update_metrics_on_filter_change(inbound_data, start_date, end_date, dc_name):
    if inbound_data is None:
        # Set default "None" metrics when inbound data is not available
        inbound_metrics = {key: "None" for key in
                           ["Days of Data", "Total IB Loads", "Total PO", "Total Lines", "Total Units",
                            "SKUs with Movement"]}
        inbound_order_profile_metrics = {key: "None" for key in
                                         ["IB Units Per Line", "IB Lines Per PO", "IB Units Per PO"]}
        po_label = "PO"
    else:
        inbound_metrics, po_label = inbound_volumes(inbound_data, start_date, end_date, dc_name)
        inbound_order_profile_metrics, _ = inbound_order_profile(inbound_data, start_date, end_date, dc_name)

    display_inbound_summary(inbound_metrics, inbound_order_profile_metrics, inbound_data, start_date, end_date, dc_name,
                            po_label)


def handle_export(inbound_data, outbound_data, start_date, end_date, start_date_outbound, end_date_outbound,
                  inbound_dc_name, bu_name, channel_name, outbound_dc_name):
    try:
        # Initialize variables with None to ensure they exist
        inbound_metrics = None
        inbound_po_label = None
        inbound_order_profile_metrics = None
        outbound_metrics = None
        outbound_order_profile_metrics = None

        # Step 1: Calculate inbound metrics and order profile
        if inbound_data is not None:
            inbound_metrics, inbound_po_label = inbound_volumes(inbound_data, start_date, end_date, inbound_dc_name)
            inbound_order_profile_metrics, _ = inbound_order_profile(inbound_data, start_date, end_date,
                                                                     inbound_dc_name)
        else:
            messagebox.showinfo("Info", "No inbound data available for export. Exporting outbound data only.")

        # Step 2: Calculate outbound metrics and order profile
        if outbound_data is not None:
            outbound_metrics = outbound_volumes(outbound_data, start_date_outbound, end_date_outbound, outbound_dc_name,
                                                bu_name, channel_name)
            outbound_order_profile_metrics = outbound_order_profile(outbound_data, start_date_outbound,
                                                                    end_date_outbound,
                                                                    outbound_dc_name, bu_name, channel_name)
        else:
            messagebox.showinfo("Info", "No outbound data available for export. Exporting inbound data only.")

        # Step 3: Call the export function based on available data
        if inbound_metrics is not None or outbound_metrics is not None:
            export_inbound_outbound_data(
                inbound_metrics,
                inbound_order_profile_metrics,
                outbound_metrics,
                outbound_order_profile_metrics,
                inbound_dc_name,
                outbound_dc_name,
                bu_name,
                channel_name,
                start_date,
                end_date,
                start_date_outbound,
                end_date_outbound,
                inbound_po_label
            )
        else:
            messagebox.showinfo("Info", "No data available for export.")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to export data: {str(e)}")




#############new function inbound onchange ################
def on_date_change_inbound(event=None):
    start_date = order_start_date_entry.get_date()
    end_date = order_end_date_entry.get_date()  # Corrected to fetch from order_end_date_entry

    # Ensure both dates are valid
    if start_date and end_date:
        update_metrics_on_filter_change(inbound_data, start_date, end_date, dc_filter_inbound.get())


#########new function inbound onchange ################


# Function to fetch distinct DC names Inbound from the database
def get_distinct_dc_names():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"

    # Check if inbound table is set
    if not Shared.project_inbound:
        # print("Inbound table name is missing. Skipping distinct DC names query.")
        return None

    try:
        # Set up your database connection (replace with your actual connection parameters)
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

        # Create a cursor to execute the query
        cursor = conn.cursor()

        # SQL query to get distinct Destination_DC
        query = f"SELECT DISTINCT Destination_DC FROM {inbound_datatbl}"
        cursor.execute(query)

        # Fetch all distinct Destination_DC values from the result
        dc_names = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Extract the DC names from the result tuples and add "All" at the beginning
        dc_values = ["All"] + [dc[0] for dc in dc_names]
        return dc_values

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["All"]  # Return "All" in case of error


# Function to initialize the filter for DC name Inbound
def create_dc_filter(parent_frame):
    global dc_filter_inbound
    # dc_values = ["All"] + list(inbound_data['Destination_DC'].unique())  # Add "All" to the unique DC names
    # Create a label for the "Inbound Data Summary" heading
    heading_label = ttk.Label(parent_frame, text="Inbound Data Summary", font=('Arial', 14, 'bold'),
                              background="#FF3B00", foreground="white")
    heading_label.pack(side="top", pady=(10, 0))  # Add padding at the top for spacing

    # Add a blank line (optional for extra spacing)
    spacer_label = ttk.Label(parent_frame, text="")
    spacer_label.pack(side="top", pady=(5, 5))  # Add vertical padding to create space
    dc_label_inbound = ttk.Label(parent_frame, text="DC Name:", font=('Arial', 10))
    dc_label_inbound.pack(side="left", padx=10)
    dc_values = get_distinct_dc_names()
    dc_filter_inbound = ttk.Combobox(parent_frame, values=dc_values, state='readonly', font=('Arial', 10))
    dc_filter_inbound.set("All")  # Set the default value to "All"
    # dc_filter.pack(side="left", padx=10)
    # Position DC filter in the grid after inbound order profile
    # dc_filter_inbound.grid(row=0, column=10, columnspan=1, pady=10, sticky='w')
    dc_filter_inbound.pack(side="left", padx=10)
    # Bind event to update the metrics on DC selection change
    dc_filter_inbound.bind("<<ComboboxSelected>>", lambda event: update_metrics_on_filter_change(inbound_data,
                                                                                                 order_start_date_entry.get_date(),
                                                                                                 order_end_date_entry.get_date(),
                                                                                                 dc_filter_inbound.get()))


# Function to fetch min and max dates from the database
def fetch_min_max_dates_inbound():
    global min_date_sql_inbound, max_date_sql_inbound
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"

    # Check if inbound table is set
    if not Shared.project_inbound:
        print("Inbound table name is missing. Skipping date query for inbound data.")
        min_date_sql_inbound, max_date_sql_inbound = None, None
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
        select_query = f"""SELECT MIN(Received_Date), MAX(Received_Date) FROM {inbound_datatbl}"""
        cursor.execute(select_query)
        min_date_sql_inbound, max_date_sql_inbound = cursor.fetchone()

        # Validate data
        if min_date_sql_inbound and max_date_sql_inbound:
            min_date_sql_inbound = pd.Timestamp(min_date_sql_inbound)
            max_date_sql_inbound = pd.Timestamp(max_date_sql_inbound)
            # print(f"Data fetched successfully! Min date: {min_date_sql_inbound}, Max date: {max_date_sql_inbound}")
        else:
            print("No valid date data fetched!")
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
    finally:
        connection.close()


# Function to fetch data from the MySQL table
def connect_to_database_outbound():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
    connection = pymysql.connect(
        host='10.216.252.8',  #
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
        select_query = f"""
            SELECT 
                DATEDIFF(MAX(Order_Date), MIN(Order_Date)) AS days_of_data,
                COUNT(DISTINCT Order_Number) AS total_orders,
                COUNT(*) AS total_lines,
                SUM(Qty) AS total_units,
                COUNT(DISTINCT SKU) AS skus_with_movement,
                COALESCE(SUM(Qty) / NULLIF(COUNT(*), 0), 0) AS ob_units_per_line,
                COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT Order_Number), 0), 0) AS ob_lines_per_order,
                COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT Order_Number), 0), 0) AS ob_units_per_order
            FROM {tbl} """
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df_outbound = pd.DataFrame(rows, columns=columns)

        # print("Data fetched successfully!")
        return df_outbound

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()

import pymysql
from tkinter import messagebox


def outbound_volumes(outbound_data, start_date=None, end_date=None, dc_name=None, bu_name=None, channel_name=None):
    tbl = f"client_data.{Shared.project}"


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


        # Execute each SQL query and fetch the result
        cursor.execute(f"""
            SELECT DATEDIFF(MAX(Order_Date), MIN(Order_Date)) AS days_of_data
            FROM {tbl}
            WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
            AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
            AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
            AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All')
        """)
        days_of_data_result = cursor.fetchone()
        days_of_data = days_of_data_result[0] if days_of_data_result else 0

        cursor.execute(f"""
                SELECT COUNT(DISTINCT Order_Number) AS total_orders
                FROM {tbl}
                WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
                AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
                AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
                AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All')
            """)
        total_orders_result = cursor.fetchone()
        total_orders = total_orders_result[0] if total_orders_result else 0

        cursor.execute(f"""
                SELECT COUNT(*) AS total_lines
                FROM {tbl}
                WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
                AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
                AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
                AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All')
            """)
        total_lines_result = cursor.fetchone()
        total_lines = total_lines_result[0] if total_lines_result else 0

        cursor.execute(f"""
                SELECT SUM(Qty) AS total_units
                FROM {tbl}
                WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
                AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
                AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
                AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All');
            """)
        total_units_result = cursor.fetchone()
        total_units = total_units_result[0] if total_units_result else 0

        cursor.execute(f"""
                SELECT COUNT(DISTINCT SKU) AS skus_with_movement
                FROM {tbl}
                WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
                AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
                AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
                AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All')
            """)
        skus_with_movement_result = cursor.fetchone()
        skus_with_movement = skus_with_movement_result[0] if skus_with_movement_result else 0

        metrics = {
            "Days of Data": format_number(days_of_data),
            f"Total Orders": format_number(total_orders),
            "Total Lines": format_number(total_lines),
            "Total Units": format_number(total_units),
            "SKUs with Movement": format_number(skus_with_movement)
        }
        return metrics

    finally:
        connection.close()




# Outbound Order Profile Calculation
def outbound_order_profile(outbound_data, start_date=None, end_date=None, dc_name=None, bu_name=None, channel_name=None):
    tbl = f"client_data.{Shared.project}"
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

        # Execute each SQL query and fetch the result
        cursor.execute(f"""
           SELECT COALESCE(SUM(Qty) / NULLIF(COUNT(*), 0), 0) AS ob_units_per_line
                FROM {tbl}
                WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
                AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
                AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
                AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All')
        """)
        ob_units_per_line_result = cursor.fetchone()
        ob_units_per_line = ob_units_per_line_result[0] if ob_units_per_line_result else 0

        cursor.execute(f"""
            SELECT COALESCE(COUNT(*) / NULLIF(COUNT(DISTINCT COALESCE(Order_Number)), 0), 0) AS ob_lines_per_po
                FROM {tbl}
                WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
                AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
                AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
                AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All')
        """)
        ob_lines_per_po_result = cursor.fetchone()
        ob_lines_per_po = ob_lines_per_po_result[0] if ob_lines_per_po_result else 0

        cursor.execute(f"""
            SELECT COALESCE(SUM(Qty) / NULLIF(COUNT(DISTINCT COALESCE(Order_Number)), 0), 0) AS ob_units_per_po
                FROM {tbl}
                WHERE (Order_Date BETWEEN '{start_date}' AND '{end_date}')
                AND (DC_Name = '{dc_name}' OR '{dc_name}' = 'All')
                AND (Business_Unit = '{bu_name}' OR '{bu_name}' = 'All')
                AND (Order_Type = '{channel_name}' OR '{channel_name}' = 'All');
        """)
        ob_units_per_po_result = cursor.fetchone()
        ob_units_per_po = ob_units_per_po_result[0] if ob_units_per_po_result else 0

        # Define the metrics with the dynamic po_label
        metrics = {
            "OB Units Per Line": ob_units_per_line,
            f"OB Lines Per Orders": ob_lines_per_po,  # Dynamically set label
            f"OB Units Per Orders": ob_units_per_po
        }
        return metrics

    finally:
        connection.close()



# Display function for outbound data and outbound order profile
def display_outbound_summary(outbound_metrics, outbound_order_profile_metrics, outbound_data, start_date, end_date,
                             dc_name, bu_name, channel_name):
    if not outbound_metrics or not outbound_order_profile_metrics:
        return

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

    outbound_volumes_label = tk.Label(card_frame_outbound, text="Outbound Volumes", font=('Arial', 14, 'bold'),
                                      anchor='w')
    outbound_volumes_label.grid(row=0, column=0, columnspan=6, pady=10, sticky='w')

    outbound_order_profile_label = tk.Label(card_frame_outbound, text="Outbound Order Profile",
                                            font=('Arial', 14, 'bold'), anchor='w')
    outbound_order_profile_label.grid(row=0, column=6, columnspan=3, pady=10, sticky='w')

    col = 0
    for metric, value in outbound_metrics.items():
        create_card(card_frame_outbound, metric, value, 1, col)
        col += 1

    col = 6
    for metric, value in outbound_order_profile_metrics.items():
        create_card(card_frame_outbound, metric, value, 1, col)
        col += 1


# Function to update metrics based on outbound data filters
def update_outbound_metrics_on_filter_change(outbound_data, start_date, end_date, dc_name, bu_name, channel_name):
    outbound_metrics = outbound_volumes(outbound_data, start_date, end_date, dc_name, bu_name, channel_name)
    outbound_order_profile_metrics = outbound_order_profile(outbound_data, start_date, end_date, dc_name, bu_name,
                                                            channel_name)
    display_outbound_summary(outbound_metrics, outbound_order_profile_metrics, outbound_data, start_date, end_date,
                             dc_name, bu_name, channel_name)


# Function to fetch distinct DC names outbound from the database
def get_distinct_dc_names_outbound():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
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
        dc_values_outbound = ["All"] + [dc[0] for dc in dc_names_outbound]
        return dc_values_outbound

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["All"]  # Return "All" in case of error


# Function to initialize the filter for DC OUTBOUND
def create_dc_filter_outbound(parent_frame):
    global dc_filter
    # dc_values = ["All"] + list(outbound_data['DC_Name'].unique())  # Add "All" to the unique DC names
    # Create a label for the "Inbound Data Summary" heading
    heading_label_outbound = ttk.Label(parent_frame, text="Outbound Data Summary", font=('Arial', 14, 'bold'),
                                       background="#FF3B00", foreground="white")
    heading_label_outbound.pack(side="top", pady=(10, 0))  # Add padding at the top for spacing

    # Add a blank line (optional for extra spacing)
    spacer_label_outbound = ttk.Label(parent_frame, text="")
    spacer_label_outbound.pack(side="top", pady=(5, 5))  # Add vertical padding to create space
    # Create a label for DC NAME
    dc_label_outbound = ttk.Label(parent_frame, text="DC Name:", font=('Arial', 10))
    dc_label_outbound.pack(side="left", padx=10)
    dc_values_outbound = get_distinct_dc_names_outbound()
    dc_filter = ttk.Combobox(parent_frame, values=dc_values_outbound, state='readonly', font=('Arial', 10))
    dc_filter.set("All")  # Set the default value to "All"
    dc_filter.pack(side="left", padx=10)

    # Bind event to update the metrics on DC selection change
    dc_filter.bind("<<ComboboxSelected>>", lambda event: update_outbound_metrics_on_filter_change(outbound_data,
                                                                                                  order_start_date_entry_outbound.get_date(),
                                                                                                  order_end_date_entry_outbound.get_date(),
                                                                                                  dc_filter.get(),
                                                                                                  bu_filter.get(),
                                                                                                  channel_filter.get()))


def get_distinct_bu_filter_outbound():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
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
        bu_values = ["All"] + [dc[0] for dc in bu_names_outbound]
        return bu_values

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["All"]  # Return "All" in case of error


# Outbound filter for BU
def create_bu_filter(parent_frame):
    global bu_filter
    # bu_values = ["All"] + list(outbound_data['Business_Unit'].unique())
    bu_label = ttk.Label(parent_frame, text="Business Unit:", font=('Arial', 10))
    bu_label.pack(side="left", padx=10)
    bu_values = get_distinct_bu_filter_outbound()
    bu_filter = ttk.Combobox(parent_frame, values=bu_values, state='readonly', font=('Arial', 10))
    bu_filter.set("All")  # Set default value
    bu_filter.pack(side="left", padx=10)

    # Bind event to update the metrics on BU selection change
    bu_filter.bind("<<ComboboxSelected>>", lambda event: update_outbound_metrics_on_filter_change(outbound_data,
                                                                                                  order_start_date_entry_outbound.get_date(),
                                                                                                  order_end_date_entry_outbound.get_date(),
                                                                                                  dc_filter.get(),
                                                                                                  bu_filter.get(),
                                                                                                  channel_filter.get()))


def get_distinct_channel_filter_outbound():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
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
        channel_values = ["All"] + [dc[0] for dc in channel_names_outbound]
        return channel_values

    except pymysql.Error as e:
        print(f"Error: {e}")
        return ["All"]  # Return "All" in case of error


# Outbound filter for Channel
def create_channel_filter(parent_frame):
    global channel_filter
    # channel_values = ["All"] + list(outbound_data['Order_Type'].unique())
    # Create a label for DC NAME
    channel_label = ttk.Label(parent_frame, text="Order Type:", font=('Arial', 10))
    channel_label.pack(side="left", padx=10)
    channel_values = get_distinct_channel_filter_outbound()
    channel_filter = ttk.Combobox(parent_frame, values=channel_values, state='readonly', font=('Arial', 10))
    channel_filter.set("All")  # Set default value
    channel_filter.pack(side="left", padx=10)

    # Bind event to update the metrics on Channel selection change
    channel_filter.bind("<<ComboboxSelected>>", lambda event: update_outbound_metrics_on_filter_change(outbound_data,
                                                                                                       order_start_date_entry_outbound.get_date(),
                                                                                                       order_end_date_entry_outbound.get_date(),
                                                                                                       dc_filter.get(),
                                                                                                       bu_filter.get(),
                                                                                                       channel_filter.get()))


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


##########new function OUTBOUND onchange ################
# Function to fetch min and max dates from the database////Minimum /maximum dates OUTBOUND dates
def fetch_min_max_dates_outbound():
    tbl = f"client_data.{Shared.project}"
    inbound_datatbl = f"client_data.{Shared.project_inbound}"
    global min_date_sql_outbound, max_date_sql_outbound
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
        select_query = f"""SELECT MIN(Order_Date), MAX(Order_Date) FROM {tbl}"""
        cursor.execute(select_query)
        min_date_sql_outbound, max_date_sql_outbound = cursor.fetchone()

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


##############new function inbound onchange ################
def on_date_change_outbound(event=None):
    start_date_outbound = order_start_date_entry_outbound.get_date()
    end_date_outbound = order_end_date_entry_outbound.get_date()  # Corrected to fetch from order_end_date_entry

    # Ensure both dates are valid
    if start_date_outbound and end_date_outbound:
        # update_metrics_on_filter_change(inbound_data, start_date, end_date, dc_filter_inbound.get())
        update_outbound_metrics_on_filter_change(outbound_data, start_date_outbound, end_date_outbound, dc_filter.get(),
                                                 bu_filter.get(), channel_filter.get())



######################LAST eXPORT
from tkinter import filedialog, messagebox


def export_inbound_outbound_data(
        inbound_metrics, inbound_order_profile_metrics, outbound_metrics, outbound_order_profile_metrics,
        inbound_dc_name, outbound_dc_name, bu_name, channel_name, start_date, end_date, start_date_outbound,
        end_date_outbound, po_label
):
    try:
        # Flags to track export status
        inbound_saved = False
        outbound_saved = False

        # Export Inbound Data if available
        if inbound_metrics and inbound_order_profile_metrics:
            inbound_header_data = {
                "DC Name": [inbound_dc_name],
                "From Date": [start_date],
                "To Date": [end_date]
            }
            inbound_summary = pd.DataFrame(inbound_header_data)

            inbound_data_to_export = {
                "Days of Data": [inbound_metrics.get("Days of Data", "N/A")],
                "Total IB Loads": [inbound_metrics.get("Total IB Loads", "N/A")],
                f"Total {po_label}": [inbound_metrics.get(f"Total {po_label}", "N/A")],
                "Total Lines": [inbound_metrics.get("Total Lines", "N/A")],
                "Total Units": [inbound_metrics.get("Total Units", "N/A")],
                "SKUs with Movement": [inbound_metrics.get("SKUs with Movement", "N/A")],
                "IB Units Per Line": [inbound_order_profile_metrics.get("IB Units Per Line", "N/A")],
                f"IB Lines Per {po_label}": [inbound_order_profile_metrics.get(f"IB Lines Per {po_label}", "N/A")],
                f"IB Units Per {po_label}": [inbound_order_profile_metrics.get(f"IB Units Per {po_label}", "N/A")]
            }
            inbound_metrics_df = pd.DataFrame(inbound_data_to_export)
            inbound_summary = pd.concat([inbound_summary, inbound_metrics_df], axis=1)

            # Save Inbound Summary to Excel
            inbound_file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                             filetypes=[("Excel Files", "*.xlsx")])
            if inbound_file_path:
                inbound_summary.to_excel(inbound_file_path, index=False)
                inbound_saved = True  # Mark as saved

        # Export Outbound Data if available
        if outbound_metrics and outbound_order_profile_metrics:
            outbound_header_data = {
                "DC Name": [outbound_dc_name],
                "Business Unit": [bu_name],
                "Channel Name": [channel_name],
                "From Date": [start_date_outbound],
                "To Date": [end_date_outbound]
            }
            outbound_summary = pd.DataFrame(outbound_header_data)

            outbound_data_to_export = {
                "Days of Data": [outbound_metrics.get("Days of Data", "N/A")],
                "Total Orders": [outbound_metrics.get("Total Orders", "N/A")],
                "Total Lines": [outbound_metrics.get("Total Lines", "N/A")],
                "Total Units": [outbound_metrics.get("Total Units", "N/A")],
                "SKUs with Movement": [outbound_metrics.get("SKUs with Movement", "N/A")],
                "OB Units Per Line": [outbound_order_profile_metrics.get("OB Units Per Line", "N/A")],
                "OB Lines Per Orders": [outbound_order_profile_metrics.get("OB Lines Per Orders", "N/A")],
                "OB Units Per Orders": [outbound_order_profile_metrics.get("OB Units Per Orders", "N/A")]
            }
            outbound_metrics_df = pd.DataFrame(outbound_data_to_export)
            outbound_summary = pd.concat([outbound_summary, outbound_metrics_df], axis=1)

            # Save Outbound Summary to Excel
            outbound_file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                              filetypes=[("Excel Files", "*.xlsx")])
            if outbound_file_path:
                outbound_summary.to_excel(outbound_file_path, index=False)
                outbound_saved = True  # Mark as saved

        # Display appropriate message based on the export results
        if inbound_saved and outbound_saved:
            messagebox.showinfo("Export Successful", "Inbound and outbound data exported successfully.")
        elif inbound_saved:
            messagebox.showinfo("Export Successful",
                                "Inbound data exported successfully. Outbound data export was canceled.")
        elif outbound_saved:
            messagebox.showinfo("Export Successful",
                                "Outbound data exported successfully. Inbound data export was canceled.")
        else:
            messagebox.showinfo("Export Canceled", "Export was canceled.")

    except Exception as e:
        messagebox.showerror("Error", f"An error occurred during export: {str(e)}")

def create_notebook_page_inbound_outbound(notebook):
    global card_frame_inbound, card_frame_outbound, inbound_data, outbound_data
    global card_frame_inbound, filter_frame_inbound, card_frame_outbound, filter_frame_outbound, root, chart_frame_inbound
    global selected_start_date, selected_end_date, selected_start_date_outbound, selected_end_date_outbound
    global inbound_data, outbound_data, order_start_date_entry, order_end_date_entry, min_date_sql_inbound, max_date_sql_inbound, min_date_sql_outbound, max_date_sql_outbound, order_start_date_entry_outbound, order_end_date_entry_outbound
    global export_button_frame_inbound, export_button_frame_outbound, order_start_date_entry_label, order_end_date_entry_label

    # Initialize selected_start_date and selected_end_date to avoid undefined error
    selected_start_date = 'No dates'
    selected_end_date = 'No dates'

    # Create Inbound and Outbound Summary Tab
    summary_frame = ttk.Frame(notebook, width=800, height=1000)
    summary_frame.grid_columnconfigure(1, weight=1)

    notebook.add(summary_frame, text="Summary")

    # Export Button
    style = ttk.Style()
    style.configure('Export.TButton', background='white', foreground='#FF3B00')

    export_button = ttk.Button(summary_frame, text="Export Data", style='Export.TButton',
                               command=lambda: handle_export(
                                   inbound_data,
                                   outbound_data,
                                   order_start_date_entry.get_date(),
                                   order_end_date_entry.get_date(),
                                   order_start_date_entry_outbound.get_date(),
                                   order_end_date_entry_outbound.get_date(),
                                   dc_filter_inbound.get(),
                                   bu_filter.get(),
                                   channel_filter.get(),
                                   dc_filter.get()
                               ))
    export_button.pack(side='bottom', pady=20)

    # Fetch inbound data
    inbound_data = connect_to_database_inbound()

    if inbound_data is not None:
        # Fetch the min and max dates only after data is fetched
        fetch_min_max_dates_inbound()

        # Set the default dates if necessary
        if min_date_sql_inbound is None or max_date_sql_inbound is None:
            print("No valid date data fetched. Using default dates.")
            selected_start_date = 'No dates'  # Placeholder if no dates are fetched
            selected_end_date = 'No dates'  # Placeholder if no dates are fetched
        else:
            selected_start_date = min_date_sql_inbound
            selected_end_date = max_date_sql_inbound

        # Now that selected_start_date and selected_end_date are set, call the function
        update_metrics_on_filter_change(inbound_data, selected_start_date, selected_end_date, "All")
    # else:
        # print("No inbound data retrieved. Check database connection and table settings.")

    # Scrollable Frame and Filters
    scrollable_frame_inbound = create_scrollable_frame(summary_frame)
    filter_frame_inbound = ttk.Frame(scrollable_frame_inbound)
    filter_frame_inbound.pack(fill='x')
    card_frame_inbound = ttk.Frame(scrollable_frame_inbound)
    card_frame_inbound.pack(fill='x', pady=10)

    create_dc_filter(filter_frame_inbound)

    # If dates are 'No dates', set placeholders for the date picker fields
    if selected_start_date == 'No dates' or selected_end_date == 'No dates':
        order_start_date_entry = DateEntry(filter_frame_inbound, selectmode='day', width=12,
                                           background='darkblue', foreground='white', borderwidth=2)
        order_start_date_entry.set_date(None)  # Set None if no dates are available
        order_end_date_entry = DateEntry(filter_frame_inbound, selectmode='day', width=12,
                                         background='darkblue', foreground='white', borderwidth=2)
        order_end_date_entry.set_date(None)  # Set None if no dates are available

        # Optional placeholder labels for dates
        order_start_date_entry_label = tk.Label(filter_frame_inbound, text="From Date: No dates")
        order_end_date_entry_label = tk.Label(filter_frame_inbound, text="To Date: No dates")
    else:
        # Set valid dates if fetched from the database
        order_start_date_entry = DateEntry(filter_frame_inbound, selectmode='day', year=min_date_sql_inbound.year,
                                           month=min_date_sql_inbound.month, day=min_date_sql_inbound.day,
                                           mindate=selected_start_date, maxdate=selected_end_date, width=12,
                                           background='darkblue', foreground='white', borderwidth=2)
        order_start_date_entry.set_date(min_date_sql_inbound)

        order_end_date_entry = DateEntry(filter_frame_inbound, selectmode='day', year=max_date_sql_inbound.year,
                                         month=max_date_sql_inbound.month, day=max_date_sql_inbound.day,
                                         mindate=selected_start_date, maxdate=selected_end_date, width=12,
                                         background='darkblue', foreground='white', borderwidth=2)
        order_end_date_entry.set_date(max_date_sql_inbound)

    order_start_date_entry_label = tk.Label(filter_frame_inbound, text="From Date:")
    order_start_date_entry_label.pack(side='left', padx=10)
    order_start_date_entry.pack(side='left', padx=10)
    order_start_date_entry.bind("<<DateEntrySelected>>", on_date_change_inbound)

    order_end_date_entry_label = tk.Label(filter_frame_inbound, text="To Date:")
    order_end_date_entry_label.pack(side='left', padx=10)
    order_end_date_entry.pack(side='left', padx=10)
    order_end_date_entry.bind("<<DateEntrySelected>>", on_date_change_inbound)

    # Update metrics again if necessary
    update_metrics_on_filter_change(inbound_data, selected_start_date, selected_end_date, "All")

    # Load data from the outbound database
    outbound_data = connect_to_database_outbound()  # Make sure this function is defined

    # fetch minimum outbound and maximum outbound dates
    fetch_min_max_dates_outbound()  # Fetch outbound min dates and max dates from the database
    # Ensure min_date_sql_outbound and max_date_sql_outbound are available
    if min_date_sql_outbound and max_date_sql_outbound:
        selected_start_date_outbound = min_date_sql_outbound
        selected_end_date_outbound = max_date_sql_outbound
    else:
        # Handle fallback in case no data is fetched
        selected_start_date_outbound = pd.Timestamp('2000-01-01')
        selected_end_date_outbound = pd.Timestamp('2100-01-01')

    filter_frame_outbound = ttk.Frame(scrollable_frame_inbound)
    filter_frame_outbound.pack(fill='x')
    card_frame_outbound = ttk.Frame(scrollable_frame_inbound)
    card_frame_outbound.pack(fill='x', pady=10)
    # Set up filters and date pickers for outbound data
    create_dc_filter_outbound(filter_frame_outbound)  # Assuming this function is defined
    create_bu_filter(filter_frame_outbound)  # Assuming this function is defined
    create_channel_filter(filter_frame_outbound)  # Assuming this function is defined

    ####dates min and max dates inbound
    # Create date entries for selecting date range
    order_start_date_entry_outbound = DateEntry(filter_frame_outbound, selectmode='day',
                                                year=min_date_sql_outbound.year, month=min_date_sql_outbound.month,
                                                day=min_date_sql_outbound.day, mindate=selected_start_date_outbound,
                                                maxdate=selected_end_date_outbound, width=12, background='darkblue',
                                                foreground='white', borderwidth=2)
    order_start_date_entry_outbound.set_date(min_date_sql_outbound)
    order_start_date_entry_outbound_label = tk.Label(filter_frame_outbound,
                                                     text=f"From Date:")
    order_start_date_entry_outbound_label.pack(side='left', padx=10)
    order_start_date_entry_outbound.pack(side='left', padx=10)
    order_start_date_entry_outbound.bind("<<DateEntrySelected>>", on_date_change_outbound)
    order_end_date_entry_outbound = DateEntry(filter_frame_outbound, selectmode='day', year=max_date_sql_outbound.year,
                                              month=max_date_sql_outbound.month, day=max_date_sql_outbound.day,
                                              mindate=selected_start_date_outbound, maxdate=selected_end_date_outbound,
                                              width=12, background='darkblue', foreground='white', borderwidth=2)
    order_end_date_entry_outbound.set_date(max_date_sql_outbound)
    order_end_date_entry_outbound_label = tk.Label(filter_frame_outbound,
                                                   text=f"To Date:")
    order_end_date_entry_outbound_label.pack(side='left', padx=10)
    order_end_date_entry_outbound.pack(side='left', padx=10)
    order_end_date_entry_outbound.bind("<<DateEntrySelected>>", on_date_change_outbound)

    # Update outbound metrics
    update_outbound_metrics_on_filter_change(outbound_data, selected_start_date_outbound, selected_end_date_outbound,
                                             "All", "All", "All")


def main():
    # Initialize root window
    root = tk.Tk()
    root.title("Summary Page")
    root.geometry("1200x600")
    # # Create a Notebook to hold both tabs
    notebook = ttk.Notebook(root)
    notebook.pack(side='top', fill='both', expand=True)
    create_notebook_page_inbound_outbound(notebook)
    root.mainloop()


if __name__ == "__main__":
    main()
