import pandas as pd
# import mysql.connector
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
from tkinter import filedialog
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pymysql
import Shared
from helper_functions import get_asset_path
from Inbound_outbound_sql_code_testing_summary import get_distinct_dc_names_outbound,get_distinct_bu_filter_outbound,get_distinct_channel_filter_outbound,fetch_min_max_dates_outbound
sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')


# tbl= f"client_data.{Shared.project}"
# Function to connect to the database with SSL
def connect_to_db():
    try:
        # Create the connection object
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
        return connection
    except pymysql.Error as err:
        messagebox.showerror("Connection Error", f"Error: {err}")
        return None


# Function to update all filters based on the selected value
def update_filters(event=None):
    tbl = f"client_data.{Shared.project}"

    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()

    selected_order_type = order_type_var.get()
    selected_business_unit = business_unit_var.get()
    selected_dc_name = dc_name_var.get()

    try:
        # Construct query based on selected filters
        query = f"""
        SELECT DISTINCT Order_Type, Business_Unit, DC_Name 
        FROM {tbl}
        """
        conditions = []
        if selected_order_type != "ALL":
            conditions.append(f"Order_Type = '{selected_order_type}'")
        if selected_business_unit != "ALL":
            conditions.append(f"Business_Unit = '{selected_business_unit}'")
        if selected_dc_name != "ALL":
            conditions.append(f"DC_Name = '{selected_dc_name}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        cursor.execute(query)
        result = cursor.fetchall()

        # Update Order Type
        order_types = sorted(set([row[0] for row in result if row[0]]))
        order_type_menu['values'] = ["ALL"] + order_types
        if selected_order_type not in order_types:
            order_type_var.set("ALL")

        # Update Business Unit
        business_units = sorted(set([row[1] for row in result if row[1]]))
        business_unit_menu['values'] = ["ALL"] + business_units
        if selected_business_unit not in business_units:
            business_unit_var.set("ALL")

        # Update DC Name
        dc_names = sorted(set([row[2] for row in result if row[2]]))
        dc_name_menu['values'] = ["ALL"] + dc_names
        if selected_dc_name not in dc_names:
            dc_name_var.set("ALL")

    except pymysql.Error as err:
        messagebox.showerror("Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()


# Function to handle closing the application properly
def on_closing():
    root.quit()  # Properly quit the Tkinter mainloop
    root.destroy()  # Destroy the root window


# Function to grant privileges (one-time setup)
def grant_privileges():
    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()
    try:
        # Grant privileges to 'kgupta' on 'client_data' database
        cursor.execute("GRANT ALL PRIVILEGES ON client_data.* TO 'kgupta'@'%';")
        cursor.execute("FLUSH PRIVILEGES;")
        messagebox.showinfo("Success", "Permissions granted to 'kgupta' on 'client_data'.")
    except pymysql.Error as e:
        messagebox.showerror("Privilege Error", f"Error granting privileges: {e}")
    finally:
        cursor.close()
        conn.close()


# Function to populate the initial filter lists
def populate_filters():
    tbl = f"client_data.{Shared.project}"

    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()

    try:
        # Populate Order_Type options
        cursor.execute(f"SELECT DISTINCT Order_Type FROM {tbl}")
        order_types = ["ALL"] + [row[0] for row in cursor.fetchall()]
        order_type_menu['values'] = order_types

        # Populate Business_Unit options
        cursor.execute(f"SELECT DISTINCT Business_Unit FROM {tbl}")
        business_units = ["ALL"] + [row[0] for row in cursor.fetchall()]
        business_unit_menu['values'] = business_units

        # Populate DC_Name options
        cursor.execute(f"SELECT DISTINCT DC_Name FROM {tbl}")
        dc_names = ["ALL"] + [row[0] for row in cursor.fetchall()]
        dc_name_menu['values'] = dc_names

        # Populate date range for Order Date
        cursor.execute(f"SELECT MIN(Order_Date), MAX(Order_Date) FROM {tbl}")
        min_order_date, max_order_date = cursor.fetchone()

        # Adjusting the date range by adding a buffer to include boundary dates
        adjusted_min_date = min_order_date - pd.Timedelta(days=1)
        adjusted_max_date = max_order_date + pd.Timedelta(days=1)

        order_min_max_date_var.set(f"Min Date: {min_order_date}, Max Date: {max_order_date}")

        # Set DateEntry limits
        # order_start_date_entry.set_date(min_order_date)
        # order_end_date_entry.set_date(max_order_date)
        # order_start_date_entry.config(mindate=adjusted_min_date, maxdate=adjusted_max_date)
        # order_end_date_entry.config(mindate=adjusted_min_date, maxdate=adjusted_max_date)

    except pymysql.Error as err:
        messagebox.showerror("Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()


# Define get_selected_filters at the top level
def get_selected_filters():
    return {
        'order_type': order_type_var.get(),
        'business_unit': business_unit_var.get(),
        'dc_name': dc_name_var.get(),
        'start_date': order_start_date_entry_outbound.get_date().strftime('%Y-%m-%d'),
        'end_date': order_end_date_entry_outbound.get_date().strftime('%Y-%m-%d')
    }


# Define export_to_excel at the top level
def export_to_excel(tree, file_name, filters):
    data = [tree.item(row)['values'] for row in tree.get_children()]

    if not data:
        messagebox.showwarning("Warning", "No data to export!")
        return

    df = pd.DataFrame(data, columns=[column for column in tree['columns']])

    # Create a DataFrame for the selected filters
    filter_df = pd.DataFrame({
        'Selected Filters': [
            f"Order Type: {filters['order_type']}",
            f"Business Unit: {filters['business_unit']}",
            f"DC Name: {filters['dc_name']}",
            f"Date Range: {filters['start_date']} to {filters['end_date']}",
            f"Design Throughput: {design_throughput_var.get()}"  # Include DesignThroughPut value here
        ]
    })

    with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
        filter_df.to_excel(writer, index=False, header=False, sheet_name="Summary")
        df.to_excel(writer, index=False, startrow=len(filter_df) + 2, sheet_name="Summary")

    messagebox.showinfo("Success", f"Data exported to {file_name} successfully!")


# Function to execute the SQL query for "Order-Tail Summary"
def execute_order_tail_summary_query():
    tbl = f"client_data.{Shared.project}"
    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()

    try:
        # Get filters
        order_type = order_type_var.get()
        business_unit = business_unit_var.get()
        dc_name = dc_name_var.get()
        design_throughput = int(design_throughput_entry.get())

        filters = []
        if order_type != "ALL":
            filters.append(f"Order_Type = '{order_type}'")
        if business_unit != "ALL":
            filters.append(f"Business_Unit = '{business_unit}'")
        if dc_name != "ALL":
            filters.append(f"DC_Name = '{dc_name}'")

        # Date Filters (Convert to YYYY-MM-DD format)
        order_start_date = order_start_date_entry_outbound.get_date().strftime('%Y-%m-%d')
        order_end_date = order_end_date_entry_outbound.get_date().strftime('%Y-%m-%d')
        filters.append(f"Order_Date >= '{order_start_date}'")
        filters.append(f"Order_Date <= '{order_end_date}'")

        # Join filters
        filter_clause = " AND ".join(filters)
        if filter_clause:
            filter_clause = "WHERE " + filter_clause
        else:
            filter_clause = ""

        # Define SQL statements for temporary tables in the database
        drop_filtered_data_sql = "DROP TABLE IF EXISTS client_data.filtered_data;"
        create_filtered_data_sql = f"""
            CREATE TABLE client_data.filtered_data AS
            SELECT 
                Order_Date AS Date,
                {design_throughput} AS DesignThroughPut,
                SUM(Qty) AS Demand_Units
            FROM 
                {tbl}
            {filter_clause}
            GROUP BY Date
            ORDER BY Date;
        """

        drop_backlog_result_sql = "DROP TABLE IF EXISTS client_data.backlog_result;"
        create_backlog_result_sql = """
            CREATE TABLE client_data.backlog_result (
                Date DATE,
                DesignThroughPut INT,
                Demand_Units INT,
                Processed_Units INT,
                Carry_Forward INT,
                Backlog_Days INT
            );
        """

        # Execute each SQL statement individually
        cursor.execute(drop_filtered_data_sql)
        cursor.execute(create_filtered_data_sql)
        cursor.execute(drop_backlog_result_sql)
        cursor.execute(create_backlog_result_sql)

        # Retrieve data from filtered_data for cumulative processing in Python
        cursor.execute("SELECT Date, DesignThroughPut, Demand_Units FROM client_data.filtered_data ORDER BY Date;")
        filtered_data = cursor.fetchall()

        if not filtered_data:
            messagebox.showwarning("Warning", "No data found for the selected filters!")
            return

        # Local data processing for `backlog_result`
        carry_forward = 0
        backlog_days = 0
        results = []
        backlog_days_data = []  # For plotting

        for row in filtered_data:
            date, design_throughput, demand_units = row

            # Calculate Processed_Units as minimum of DesignThroughPut and (Demand_Units + Carry_Forward)
            processed_units = min(design_throughput, demand_units + carry_forward)

            # Update Carry_Forward for the next date as the excess of (Demand_Units + Carry_Forward - Processed_Units)
            new_carry_forward = max(0, demand_units + carry_forward - design_throughput)
            # Update backlog days count
            # Check Carry_Forward from the previous day to determine Backlog_Days behavior
            if carry_forward == 0:
                backlog_days = 0  # Reset backlog days if no carry-forward from previous day
            elif new_carry_forward > 0:
                backlog_days += 1  # Increment backlog days if there is a new carry-forward
            else:
                backlog_days = 0  # Reset if there is no new carry-forward

            # Append the calculated row to results list
            results.append((date, design_throughput, demand_units, processed_units, new_carry_forward, backlog_days))
            backlog_days_data.append((date, backlog_days))  # Collect data for the chart

            # Update carry_forward for the next iteration
            carry_forward = new_carry_forward

        # Insert processed rows into backlog_result table
        cursor.executemany("""
            INSERT INTO client_data.backlog_result (Date, DesignThroughPut, Demand_Units, Processed_Units, Carry_Forward, Backlog_Days)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, results)

        # Commit changes to the database
        conn.commit()

        # Retrieve final results from backlog_result for display
        cursor.execute("""
            SELECT Date, DesignThroughPut, Demand_Units, Processed_Units, Carry_Forward, Backlog_Days 
            FROM client_data.backlog_result;
        """)
        rows = cursor.fetchall()

        # Check if rows were returned
        if not rows:
            messagebox.showwarning("Warning", "No data found for the selected filters!")
            return

        # Clear the tree view before inserting new data
        for item in order_tail_tree.get_children():
            order_tail_tree.delete(item)

        total_row = None  # Initialize total_row

        # Insert rows into the Treeview with comma-separated numeric values
        for row in rows:
            # Format each field with commas
            formatted_row = (
                row[0],  # Date remains unchanged
                f"{row[1]:,}",  # DesignThroughPut with comma formatting
                f"{row[2]:,}",  # Demand_Units with comma formatting
                f"{row[3]:,}",  # Processed_Units with comma formatting
                f"{row[4]:,}",  # Carry_Forward with comma formatting
                f"{row[5]:,}"  # Backlog_Days with comma formatting
            )

            if row[0] == 'Total':
                total_row = formatted_row  # Store the total row temporarily
            else:
                order_tail_tree.insert('', 'end', values=formatted_row)

        # After inserting all rows, insert the total row at the end
        if total_row:
            order_tail_tree.insert('', 'end', values=total_row, tags=('total',))

        # Apply bold styling to the total row
        order_tail_tree.tag_configure('total', background='#d3d3d3', font=('TkDefaultFont', 10, 'bold'))

        messagebox.showinfo("Success", "Order Tail Summary loaded successfully!")

        # Plot backlog days chart with the processed data
        plot_backlog_days_chart(backlog_days_data)

    except Exception as err:
        messagebox.showerror("Query Error", f"Error: {err}")


    finally:

        # Drop temporary tables to clean up

        cursor.execute("DROP TABLE IF EXISTS client_data.filtered_data;")
        cursor.execute("DROP TABLE IF EXISTS client_data.backlog_result;")

        cursor.close()
        conn.close()

        # Display the Order Tail Summary

        filters = get_selected_filters()
        display_order_tail_summary(filters)  # Call to display summary


# Global variable to hold the figure for the backlog days chart
fig = None


# Function to plot backlog days chart
def plot_backlog_days_chart(backlog_days_data):
    global fig  # Declare fig as global to be accessible for saving

    # Prepare data for plotting
    dates = [row[0] for row in backlog_days_data]  # Use datetime objects for accurate plotting
    formatted_dates = [date.strftime('%m-%d-%y') for date in dates]  # Format dates for display as MM-DD-YY
    backlog_days = [row[1] for row in backlog_days_data]

    # Clear any previous plot in backlog_days_frame
    for widget in backlog_days_frame.winfo_children():
        widget.destroy()

    # Plot the bar chart with adjusted figure size and layout
    fig, ax = plt.subplots(figsize=(10, 1.5))  # Increase width for a wider graph
    ax.bar(dates, backlog_days, color='skyblue', width=0.6)  # Adjust width for better spacing

    # Plot the bar chart
    fig, ax = plt.subplots()
    ax.bar(dates, backlog_days, color='skyblue')
    ax.set_xlabel("Date", fontsize=9)
    ax.set_ylabel("Backlog Days", fontsize=9)
    ax.set_title("Backlog Days Over Time", fontsize=9)

    # Set date format and display every 7th day
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=12))  # change the interval
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))  # Format dates as MMM-DD

    # Reduce font size for tick labels
    ax.tick_params(axis='x', labelsize=7)
    ax.tick_params(axis='y', labelsize=6)

    # Rotate dates and adjust layout
    plt.xticks(rotation=0, ha='right')
    fig.subplots_adjust(bottom=0.35, top=0.85)  # Add more space at the bottom to fit rotated dates
    plt.tight_layout()

    # Embed the chart into the Tkinter frame
    chart_canvas = FigureCanvasTkAgg(fig, master=backlog_days_frame)
    chart_canvas.draw()
    chart_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

def fetch_min_max_dates_outbound():
    tbl = f"client_data.{Shared.project}"
    global min_date_sql_outbound, max_date_sql_outbound
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



def order_tail(notebook):
    global order_type_var, business_unit_var, dc_name_var, order_type_menu, business_unit_menu, dc_name_menu, root, order_min_max_date_var, order_start_date_entry, order_end_date_entry, design_throughput_var, design_throughput_entry
    global order_tail_tree, display_order_tail_summary, backlog_days_frame, order_tail_summary_frame,order_end_date_entry_outbound,order_start_date_entry_outbound,min_date_sql_outbound,max_date_sql_outbound
    # Creating the Tkinter GUI
    # root = tk.Tk()
    # root.title("SQL Data Fetcher")

    style = ttk.Style()
    style.configure("Treeview.Heading", font=('TkDefaultFont', 10, 'bold'))

    order_type_var = tk.StringVar(value="ALL")
    business_unit_var = tk.StringVar(value="ALL")
    dc_name_var = tk.StringVar(value="ALL")
    order_min_max_date_var = tk.StringVar()
    design_throughput_var = tk.IntVar(value=3000)

    #fetch_min_max_dates_outbound()  # Fetch the min and max dates from the outbound table

    main_frame = ttk.Frame(notebook)
    main_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
    notebook.add(main_frame, text="Order Tail")
    # main_frame = ttk.Frame(root)
    # main_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

    center_frame = ttk.Frame(main_frame)
    center_frame.pack(pady=20, padx=10)

    # Design Throughput Entry
    ttk.Label(center_frame, text="Design Throughput:").grid(row=2, column=2, padx=10, pady=10)
    design_throughput_entry = ttk.Entry(center_frame, textvariable=design_throughput_var)
    design_throughput_entry.grid(row=2, column=3, padx=10, pady=10)
    # Fetch channel Names
    channel_names_outbound = Shared.channel_names_outbound # get_distinct_channel_filter_outbound()
    # Channel Filter
    channel_label = ttk.Label(center_frame, text="Order Type:", font=('Arial', 10))
    channel_label.grid(row=0, column=0, padx=10, pady=10)
    channel_filter = ttk.Combobox(center_frame, values=channel_names_outbound, state='readonly',
                                  font=('Arial', 10), textvariable=order_type_var)
    channel_filter.set("ALL")  # Set default value
    channel_filter.grid(row=0, column=1, padx=10, pady=10)
    # ttk.Label(center_frame, text="Order Type:").grid(row=0, column=0, padx=10, pady=10)
    # order_type_menu = ttk.Combobox(center_frame, textvariable=order_type_var)
    # order_type_menu.grid(row=0, column=1, padx=10, pady=10)
    # order_type_menu.bind("<<ComboboxSelected>>", update_filters)
    # Fetch BU Names
    bu_names_outbound = Shared.bu_names_outbound # get_distinct_bu_filter_outbound()
    # BU Name Filter
    bu_label = ttk.Label(center_frame, text="Business Unit:", font=('Arial', 10))
    bu_label.grid(row=0, column=2, padx=10, pady=10)
    bu_filter = ttk.Combobox(center_frame, values=bu_names_outbound, state='readonly', font=('Arial', 10), textvariable=business_unit_var)
    bu_filter.set("ALL")  # Set default value
    bu_filter.grid(row=0, column=3, padx=10, pady=10)
    # ttk.Label(center_frame, text="Business Unit:").grid(row=0, column=2, padx=10, pady=10)
    # business_unit_menu = ttk.Combobox(center_frame, textvariable=business_unit_var)
    # business_unit_menu.grid(row=0, column=3, padx=10, pady=10)
    # business_unit_menu.bind("<<ComboboxSelected>>", update_filters)
    # Fetch DC Names
    dc_names_outbound = Shared.dc_names_outbound # get_distinct_dc_names_outbound()
    dc_label_outbound = ttk.Label(center_frame, text="DC Name:", font=('Arial', 10))
    dc_label_outbound.grid(row=0, column=4, padx=10, pady=10)
    dc_filter = ttk.Combobox(center_frame, values=dc_names_outbound, state='readonly', font=('Arial', 10), textvariable=dc_name_var)
    dc_filter.set("ALL")  # Set the default value to "ALL"
    dc_filter.grid(row=0, column=5, padx=10, pady=10)
    # ttk.Label(center_frame, text="DC Name:").grid(row=0, column=4, padx=10, pady=10)
    # dc_name_menu = ttk.Combobox(center_frame, textvariable=dc_name_var)
    # dc_name_menu.grid(row=0, column=5, padx=10, pady=10)
    # dc_name_menu.bind("<<ComboboxSelected>>", update_filters)
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

    ttk.Label(center_frame, text="Start Date:").grid(row=1, column=1, padx=10, pady=10)
    order_start_date_entry_outbound = DateEntry(center_frame, width=12, background='darkblue', foreground='white', borderwidth=2)
    order_start_date_entry_outbound.set_date(Shared.min_date_sql_outbound)  # Default to the min date
    order_start_date_entry_outbound.grid(row=1, column=2, padx=10, pady=10)
    order_start_date_entry_outbound.bind("<<DateEntrySelected>>",
                                         validate_dates_outbound)  # Bind validation to date selection
    ttk.Label(center_frame, text="End Date:").grid(row=1, column=3, padx=10, pady=10)
    order_end_date_entry_outbound = DateEntry(center_frame, width=12, background='darkblue', foreground='white', borderwidth=2)
    order_end_date_entry_outbound.set_date(Shared.max_date_sql_outbound)
    order_end_date_entry_outbound.grid(row=1, column=4, padx=10, pady=10)
    order_end_date_entry_outbound.bind("<<DateEntrySelected>>",
                                       validate_dates_outbound)  # Bind validation to date selection
    # Initialize variables for summary frames
    order_tail_summary_frame = None  # Initialize here

    # Create a container for the summaries
    summary_container = ttk.Frame(main_frame)
    summary_container.pack(pady=10, padx=10, fill=tk.X)

    # Function to display order tail summary filters
    def display_order_tail_summary(filters):

        global order_tail_summary_frame

        if order_tail_summary_frame is not None:
            order_tail_summary_frame.destroy()

        order_tail_summary_frame = ttk.Frame(summary_container)
        order_tail_summary_frame.grid(row=0, column=0, padx=5, pady=10, sticky="nw")  # Position in row 0, column 0

        # ttk.Label(order_tail_summary_frame, text="Below was last filter applied:", font=("TkDefaultFont", 10, "italic", "bold")).pack(anchor='w')
        ttk.Label(order_tail_summary_frame, text=f"Order Type: {filters['order_type']}").pack(anchor='w')
        ttk.Label(order_tail_summary_frame, text=f"Business Unit: {filters['business_unit']}").pack(anchor='w')
        ttk.Label(order_tail_summary_frame, text=f"DC Name: {filters['dc_name']}").pack(anchor='w')
        ttk.Label(order_tail_summary_frame, text=f"Date Range: {filters['start_date']} to {filters['end_date']}").pack(
            anchor='w')

        # Adding the Design Throughput to the summary
        design_throughput_value = design_throughput_var.get()  # Get current value of Design Throughput
        ttk.Label(order_tail_summary_frame, text=f"Design Throughput: {design_throughput_value}").pack(anchor='w')

    # # Notebook for Tabs (Order Tail Summary & Date Summary)
    # notebook = ttk.Notebook(main_frame)
    # notebook.pack(expand=True, fill=tk.BOTH)
    # Create a new Notebook inside the SQL Data Fetcher tab to hold the two summaries
    summary_notebook = ttk.Notebook(main_frame)
    summary_notebook.pack(expand=True, fill=tk.BOTH)
    # Frame for "Order Tail Summary"
    order_tail_frame = ttk.Frame(summary_notebook)

    # Treeview for Order Tail Summary with Scrollbars
    order_tail_tree = ttk.Treeview(order_tail_frame,
                                   columns=(
                                   "Date", "DesignThroughPut", "Demand Units", "Processed Units", "Carry Forward",
                                   "Backlog Days"),
                                   show='headings')

    for col in order_tail_tree["columns"]:
        order_tail_tree.heading(col, text=col)
        order_tail_tree.column(col, width=150, anchor='center')

    vsb1 = ttk.Scrollbar(order_tail_frame, orient="vertical", command=order_tail_tree.yview)
    hsb1 = ttk.Scrollbar(order_tail_frame, orient="horizontal", command=order_tail_tree.xview)
    order_tail_tree.configure(yscrollcommand=vsb1.set, xscrollcommand=hsb1.set)

    vsb1.pack(side=tk.RIGHT, fill='y')
    hsb1.pack(side=tk.BOTTOM, fill='x')
    order_tail_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    summary_notebook.add(order_tail_frame, text="Order Tail Summary")

    # New frame for Backlog Days Chart
    backlog_days_frame = ttk.Frame(summary_notebook)
    summary_notebook.add(backlog_days_frame, text="Backlog Days Chart")

    # Buttons for executing and exporting the data
    buttons_frame = ttk.Frame(center_frame)
    buttons_frame.grid(row=3, column=0, columnspan=6, pady=10)

    execute_button = tk.Button(buttons_frame, text="Execute Order Tail Summary",
                               command=execute_order_tail_summary_query)
    execute_button.grid(row=0, column=0, padx=10)

    def export_order_tail_frame():
        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                 filetypes=[("Excel files", "*.xlsx")],
                                                 title="Save Order Tail Summary As")
        if file_path:  # Check if user selected a file
            export_to_excel(order_tail_tree, file_path, get_selected_filters())

    export_button = tk.Button(buttons_frame, text="Export Order Tail Summary", command=export_order_tail_frame)
    export_button.grid(row=0, column=2, padx=10)

    # Function to save the "Backlog Days Chart" as an image file
    def download_backlog_days_chart():
        if fig is None:
            messagebox.showerror("Error", "The Backlog Days Chart has not been generated yet.")
            return

        # Prompt the user to select a file path and name
        file_path = filedialog.asksaveasfilename(defaultextension=".png",
                                                 filetypes=[("PNG files", "*.png"), ("JPEG files", "*.jpg"),
                                                            ("ALL Files", "*.*")],
                                                 title="Save Backlog Days Chart As")
        if file_path:
            try:
                # Save the current figure (the backlog days chart) to the selected file path
                fig.savefig(file_path)
                messagebox.showinfo("Success", f"Backlog Days Chart saved successfully at:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred while saving the chart:\n{str(e)}")

    # Add the new button below existing buttons
    download_chart_button = tk.Button(buttons_frame, text="Download Backlog Days Chart",
                                      command=download_backlog_days_chart)
    download_chart_button.grid(row=0, column=3, padx=10)  # Position it in the next column

    # Initialize filters
    # populate_filters()

    # # Set window size and start main loop
    # root.geometry("1200x800")

    # root.mainloop()


def main():
    # Creating the Tkinter GUI
    # global root
    root = tk.Tk()
    root.title("Data Summary")
    # Set window size and start main loop
    root.geometry("1200x800")
    fetch_min_max_dates_outbound()  # Fetch the min and max dates from the outbound table
    # Notebook for Tabs (Order Categories Summary & S/M Unit Summary)
    notebook = ttk.Notebook(root)
    notebook.pack(expand=True, fill=tk.BOTH)
    order_tail(notebook)
    # Bind the window close event to `on_closing`
    root.protocol("WM_DELETE_WINDOW", on_closing)

    root.mainloop()


if __name__ == "__main__":
    main()
