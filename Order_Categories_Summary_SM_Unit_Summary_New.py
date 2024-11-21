import pandas as pd
# import mysql.connector
# import MySQLdb
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
from tkinter import filedialog
import pymysql
import Shared
from helper_functions import get_asset_path
from Inbound_outbound_sql_code_testing_summary import get_distinct_dc_names_outbound,get_distinct_bu_filter_outbound,get_distinct_channel_filter_outbound,fetch_min_max_dates_outbound
# tbl= f"client_data.{Shared.project}"
sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')

# Function to connect to the database with SSL
def connect_to_db():
    try:
        # Create the connection object
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
        return connection
    except pymysql.Error as err:
        messagebox.showerror("Connection Error", f"Error: {err}")
        return None

# Function to add a new range for Order Categories
def add_range():
    global current_range_count
    if current_range_count < 10:
        lower_range_vars.append(tk.IntVar(value=current_range_count * 2 + 2))
        upper_range_vars.append(tk.IntVar(value=(current_range_count + 1) * 2 + 1))

        ttk.Label(left_frame, text=f"Range {current_range_count + 1}:").grid(
            row=2 + current_range_count, column=0, padx=5, pady=5
        )
        ttk.Entry(left_frame, textvariable=lower_range_vars[current_range_count], width=5).grid(
            row=2 + current_range_count, column=1, padx=5, pady=5
        )
        ttk.Entry(left_frame, textvariable=upper_range_vars[current_range_count], width=5).grid(
            row=2 + current_range_count, column=2, padx=5, pady=5
        )

        current_range_count += 1

        # Move the buttons and other elements down
        add_range_button.grid(row=2 + current_range_count, column=0, padx=10, pady=10)
        reduce_range_button.grid(row=2 + current_range_count, column=1, padx=10, pady=10)

# Function to remove the last range for Order Categories
def reduce_range():
    global current_range_count
    if current_range_count > 3:
        # Remove the last range's widgets
        for widget in left_frame.grid_slaves(row=2 + current_range_count - 1):
            widget.grid_forget()

        lower_range_vars.pop()
        upper_range_vars.pop()
        current_range_count -= 1

        # Move the buttons and other elements up
        add_range_button.grid(row=2 + current_range_count, column=0, padx=10, pady=10)
        reduce_range_button.grid(row=2 + current_range_count, column=1, padx=10, pady=10)


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
                f"Date Range: {filters['start_date']} to {filters['end_date']}"
            ]
        })

        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            filter_df.to_excel(writer, index=False, header=False, sheet_name="Summary")
            df.to_excel(writer, index=False, startrow=len(filter_df) + 2, sheet_name="Summary")

        messagebox.showinfo("Success", f"Data exported to {file_name} successfully!")



# Function to execute the SQL query for "Order Categories Summary"
def execute_order_categories_query():
    tbl = f"client_data.{Shared.project}"
    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()

    # Get filters
    order_type = order_type_var.get()
    business_unit = business_unit_var.get()
    dc_name = dc_name_var.get()

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

    filter_clause = " AND ".join(filters)
    if filter_clause:
        filter_clause = "WHERE " + filter_clause

    # Get user-defined ranges for "Order Categories Summary"
    single_unit_lower = single_unit_var.get()
    lower_ranges_values = [lower_range_vars[i].get() for i in range(current_range_count)]
    upper_ranges_values = [upper_range_vars[i].get() for i in range(current_range_count)]
    # Full SQL Query for "Order Categories Summary"
    query = f"""
    WITH OrderTotals AS (
        SELECT
            Order_Number,
            COALESCE(SUM(Qty), 0) AS TotalUnits,
            COALESCE(SUM(Pallet_Picks), 0) AS TotalPalletPicks,
            COALESCE(SUM(Pallet_Units), 0) AS TotalPalletUnits,
            COALESCE(SUM(Layer_Picks), 0) AS TotalLayerPicks,
            COALESCE(SUM(Layer_Units), 0) AS TotalLayerUnits,
            COALESCE(SUM(Case_Picks), 0) AS TotalCasePicks,
            COALESCE(SUM(Case_Units), 0) AS TotalCaseUnits,
            COALESCE(SUM(Inner_Picks), 0) AS TotalInnerPicks,
            COALESCE(SUM(Inner_Units), 0) AS TotalInnerUnits,
            COALESCE(SUM(Each_Picks), 0) AS TotalEachPicks,
            COUNT(*) AS LineCount  -- New field to count the actual lines (rows)
        FROM
            {tbl}
        {filter_clause}
        GROUP BY
            Order_Number
    ),
    TotalOrders AS (
        SELECT COALESCE(COUNT(DISTINCT Order_Number), 0) AS TotalOrderCount,
            COALESCE(SUM(LineCount),0) AS TotalLineCount,
            COALESCE(SUM(TotalUnits), 0) AS TotalUnitsSum,
            COALESCE(SUM(TotalPalletPicks), 0) AS TotalPalletPicksSum,
            COALESCE(SUM(TotalPalletUnits), 0) AS TotalPalletUnitsSum,
            COALESCE(SUM(TotalLayerPicks), 0) AS TotalLayerPicksSum,
            COALESCE(SUM(TotalLayerUnits), 0) AS TotalLayerUnitsSum,
            COALESCE(SUM(TotalCasePicks), 0) AS TotalCasePicksSum,
            COALESCE(SUM(TotalCaseUnits), 0) AS TotalCaseUnitsSum,
            COALESCE(SUM(TotalInnerPicks), 0) AS TotalInnerPicksSum,
            COALESCE(SUM(TotalInnerUnits), 0) AS TotalInnerUnitsSum,
            COALESCE(SUM(TotalEachPicks), 0) AS TotalEachPicksSum
        FROM OrderTotals
    )
    SELECT * FROM (
        SELECT
            CASE
                WHEN TotalUnits = {single_unit_lower} THEN 'Single Unit Orders'
                {" ".join([f"WHEN TotalUnits BETWEEN {lower_ranges_values[i]} AND {upper_ranges_values[i]} THEN '{lower_ranges_values[i]} - {upper_ranges_values[i]} Unit Orders'" for i in range(current_range_count)])}
                ELSE 'Other'
            END AS `Order Categories`,
            FORMAT(COALESCE(COUNT(DISTINCT ot.Order_Number), 0), 0) AS `Orders`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(DISTINCT ot.Order_Number), 0) / (SELECT COALESCE(TotalOrderCount, 0) FROM TotalOrders), 2), '%') AS `%Orders`,
            FORMAT(COALESCE(COUNT(*), 0), 0) AS `Lines`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(*), 0) / (SELECT COALESCE(TotalLineCount, 0) FROM TotalOrders), 2), '%') AS `%Lines`,
            FORMAT(COALESCE(COUNT(DISTINCT td.SKU), 0), 0) AS `SKUs`,
            FORMAT(COALESCE(SUM(td.Qty), 0), 0) AS `Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Qty), 0) / (SELECT COALESCE(TotalUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Units`,
            FORMAT(COALESCE(SUM(td.Pallet_Picks), 0), 0) AS `Pallet_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Picks), 0) / (SELECT COALESCE(TotalPalletPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
            FORMAT(COALESCE(SUM(td.Pallet_Units), 0), 0) AS `Pallet_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Units), 0) / (SELECT COALESCE(TotalPalletUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
            FORMAT(COALESCE(SUM(td.Layer_Picks), 0), 0) AS `Layer_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Picks), 0) / (SELECT COALESCE(TotalLayerPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
            FORMAT(COALESCE(SUM(td.Layer_Units), 0), 0) AS `Layer_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Units), 0) / (SELECT COALESCE(TotalLayerUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Layer_Units`,
            FORMAT(COALESCE(SUM(td.Case_Picks), 0), 0) AS `Case_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Picks), 0) / (SELECT COALESCE(TotalCasePicksSum, 0) FROM TotalOrders), 2), '%') AS `%Case_Picks`,
            FORMAT(COALESCE(SUM(td.Case_Units), 0), 0) AS `Case_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Units), 0) / (SELECT COALESCE(TotalCaseUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Case_Units`,
            FORMAT(COALESCE(SUM(td.Inner_Picks), 0), 0) AS `Inner_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Picks), 0) / (SELECT COALESCE(TotalInnerPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
            FORMAT(COALESCE(SUM(td.Inner_Units), 0), 0) AS `Inner_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Units), 0) / (SELECT COALESCE(TotalInnerUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Inner_Units`,
            FORMAT(COALESCE(SUM(td.Each_Picks), 0), 0) AS `Each_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Each_Picks), 0) / (SELECT COALESCE(TotalEachPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Each_Picks`
        FROM
            OrderTotals ot
        JOIN
            {tbl} td
            ON ot.Order_Number = td.Order_Number
        GROUP BY
            CASE
                WHEN TotalUnits = {single_unit_lower} THEN 'Single Unit Orders'
                {" ".join([f"WHEN TotalUnits BETWEEN {lower_ranges_values[i]} AND {upper_ranges_values[i]} THEN '{lower_ranges_values[i]} - {upper_ranges_values[i]} Unit Orders'" for i in range(current_range_count)])}
                ELSE 'Other'
            END

        UNION ALL

        SELECT
            'Total' AS `Order Categories`,
            FORMAT(COALESCE(COUNT(DISTINCT ot.Order_Number), 0), 0) AS `Orders`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(DISTINCT ot.Order_Number), 0) / (SELECT COALESCE(TotalOrderCount, 0) FROM TotalOrders), 2), '%') AS `%Orders`,
            FORMAT(COALESCE(COUNT(*), 0), 0) AS `Lines`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(*), 0) / (SELECT COALESCE(TotalLineCount, 0) FROM TotalOrders), 2), '%') AS `%Lines`,
            FORMAT(COALESCE(COUNT(DISTINCT td.SKU), 0), 0) AS `SKUs`,
            FORMAT(COALESCE(SUM(td.Qty), 0), 0) AS `Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Qty), 0) / (SELECT COALESCE(TotalUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Units`,
            FORMAT(COALESCE(SUM(td.Pallet_Picks), 0), 0) AS `Pallet_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Picks), 0) / (SELECT COALESCE(TotalPalletPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
            FORMAT(COALESCE(SUM(td.Pallet_Units), 0), 0) AS `Pallet_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Units), 0) / (SELECT COALESCE(TotalPalletUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
            FORMAT(COALESCE(SUM(td.Layer_Picks), 0), 0) AS `Layer_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Picks), 0) / (SELECT COALESCE(TotalLayerPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
            FORMAT(COALESCE(SUM(td.Layer_Units), 0), 0) AS `Layer_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Units), 0) / (SELECT COALESCE(TotalLayerUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Layer_Units`,
            FORMAT(COALESCE(SUM(td.Case_Picks), 0), 0) AS `Case_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Picks), 0) / (SELECT COALESCE(TotalCasePicksSum, 0) FROM TotalOrders), 2), '%') AS `%Case_Picks`,
            FORMAT(COALESCE(SUM(td.Case_Units), 0), 0) AS `Case_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Units), 0) / (SELECT COALESCE(TotalCaseUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Case_Units`,
            FORMAT(COALESCE(SUM(td.Inner_Picks), 0), 0) AS `Inner_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Picks), 0) / (SELECT COALESCE(TotalInnerPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
            FORMAT(COALESCE(SUM(td.Inner_Units), 0), 0) AS `Inner_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Units), 0) / (SELECT COALESCE(TotalInnerUnitsSum, 0) FROM TotalOrders), 2), '%') AS `%Inner_Units`,
            FORMAT(COALESCE(SUM(td.Each_Picks), 0), 0) AS `Each_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Each_Picks), 0) / (SELECT COALESCE(TotalEachPicksSum, 0) FROM TotalOrders), 2), '%') AS `%Each_Picks`
        FROM
            OrderTotals ot
        JOIN
            {tbl} td
            ON ot.Order_Number = td.Order_Number
    ) AS Results
ORDER BY
    CASE
        WHEN `Order Categories` = 'Single Unit Orders' THEN 1
        {" ".join([f"WHEN `Order Categories` = '{lower_ranges_values[i]} - {upper_ranges_values[i]} Unit Orders' THEN {i + 2}" for i in range(current_range_count)])}
        WHEN `Order Categories` = 'Other' THEN 998
        WHEN `Order Categories` = 'Total' THEN 999
        ELSE 9999
    END;
"""

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        # Clear the tree view
        for item in order_categories_tree.get_children():
            order_categories_tree.delete(item)

        # Insert rows into the treeview
        for row in rows:
            if row[0] == "Total":
                order_categories_tree.insert('', 'end', values=row, tags=('total',))
            elif row[0] == "Other":
                order_categories_tree.insert('', 'end', values=row, tags=('other',))
            else:
                order_categories_tree.insert('', 'end', values=row)

        order_categories_tree.tag_configure('total', background='#d3d3d3', font=('Helvetica', 10, 'bold'))
        order_categories_tree.tag_configure('other', background='#ffffe0', font=('Helvetica', 10))

        messagebox.showinfo("Success", "Order Categories Summary loaded successfully!")
    except pymysql.Error as err:
        messagebox.showerror("Query Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

        filters = get_selected_filters()
        display_order_categories_summary(filters)  # Call to display summary


# Function to execute the SQL query for "S/M Unit Summary"
def execute_sm_unit_query():
    tbl = f"client_data.{Shared.project}"
    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()

    # Get filters
    order_type = order_type_var.get()
    business_unit = business_unit_var.get()
    dc_name = dc_name_var.get()

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

    filter_clause = " AND ".join(filters)
    if filter_clause:
        filter_clause = "WHERE " + filter_clause
    # SQL query for "S/M Unit Summary"
    query = f"""
    WITH OrderTotals AS (
        SELECT
            Order_Number,
            COALESCE(SUM(Qty), 0) AS TotalUnits,
            COALESCE(SUM(Pallet_Picks), 0) AS TotalPalletPicks,
            COALESCE(SUM(Pallet_Units), 0) AS TotalPalletUnits,
            COALESCE(SUM(Layer_Picks), 0) AS TotalLayerPicks,
            COALESCE(SUM(Layer_Units), 0) AS TotalLayerUnits,
            COALESCE(SUM(Case_Picks), 0) AS TotalCasePicks,
            COALESCE(SUM(Case_Units), 0) AS TotalCaseUnits,
            COALESCE(SUM(Inner_Picks), 0) AS TotalInnerPicks,
            COALESCE(SUM(Inner_Units), 0) AS TotalInnerUnits,
            COALESCE(SUM(Each_Picks), 0) AS TotalEachPicks,
            COUNT(*) AS LineCount  -- New field to count the actual lines (rows)
        FROM
            {tbl}
        {filter_clause}
        GROUP BY
            Order_Number
    ),
    TotalOrders AS (
        SELECT 
            COALESCE(COUNT(DISTINCT Order_Number), 0) AS TotalOrderCount,
            COALESCE(SUM(LineCount),0) AS TotalLineCount,
            COALESCE(SUM(TotalUnits), 0) AS TotalUnitsSum,
            COALESCE(SUM(TotalPalletPicks), 0) AS TotalPalletPicksSum,
            COALESCE(SUM(TotalPalletUnits), 0) AS TotalPalletUnitsSum,
            COALESCE(SUM(TotalLayerPicks), 0) AS TotalLayerPicksSum,
            COALESCE(SUM(TotalLayerUnits), 0) AS TotalLayerUnitsSum,
            COALESCE(SUM(TotalCasePicks), 0) AS TotalCasePicksSum,
            COALESCE(SUM(TotalCaseUnits), 0) AS TotalCaseUnitsSum,
            COALESCE(SUM(TotalInnerPicks), 0) AS TotalInnerPicksSum,
            COALESCE(SUM(TotalInnerUnits), 0) AS TotalInnerUnitsSum,
            COALESCE(SUM(TotalEachPicks), 0) AS TotalEachPicksSum
        FROM OrderTotals
    )
    SELECT * FROM (
        SELECT
            CASE
                WHEN ot.LineCount = 1 AND ot.TotalUnits = 1 THEN 'Single Line - Single Unit'
                WHEN ot.LineCount = 1 AND ot.TotalUnits > 1 THEN 'Single Line - Multi Unit'
                WHEN ot.LineCount > 1 THEN 'Multi Line - Multi Unit'
                ELSE 'Other'
            END AS `S/M Unit`,
            FORMAT(COALESCE(COUNT(DISTINCT ot.Order_Number), 0), 0) AS `Orders`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(DISTINCT ot.Order_Number), 0) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
            FORMAT(COALESCE(COUNT(*), 0), 0) AS `Lines`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(*), 0) / (SELECT TotalLineCount FROM TotalOrders), 2), '%') AS `%Lines`,
            FORMAT(COALESCE(COUNT(DISTINCT td.SKU), 0), 0) AS `SKUs`,
            FORMAT(COALESCE(SUM(td.Qty), 0), 0) AS `Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Qty), 0) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
            FORMAT(COALESCE(SUM(td.Pallet_Picks), 0), 0) AS `Pallet_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Picks), 0) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
            FORMAT(COALESCE(SUM(td.Pallet_Units), 0), 0) AS `Pallet_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Units), 0) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
            FORMAT(COALESCE(SUM(td.Layer_Picks), 0), 0) AS `Layer_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Picks), 0) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
            FORMAT(COALESCE(SUM(td.Layer_Units), 0), 0) AS `Layer_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Units), 0) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
            FORMAT(COALESCE(SUM(td.Case_Picks), 0), 0) AS `Case_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Picks), 0) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
            FORMAT(COALESCE(SUM(td.Case_Units), 0), 0) AS `Case_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Units), 0) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
            FORMAT(COALESCE(SUM(td.Inner_Picks), 0), 0) AS `Inner_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Picks), 0) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
            FORMAT(COALESCE(SUM(td.Inner_Units), 0), 0) AS `Inner_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Units), 0) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
            FORMAT(COALESCE(SUM(td.Each_Picks), 0), 0) AS `Each_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Each_Picks), 0) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
        FROM 
            OrderTotals ot
        JOIN 
            {tbl} td
            ON ot.Order_Number = td.Order_Number
        GROUP BY 
            CASE
                WHEN ot.LineCount = 1 AND ot.TotalUnits = 1 THEN 'Single Line - Single Unit'
                WHEN ot.LineCount = 1 AND ot.TotalUnits > 1 THEN 'Single Line - Multi Unit'
                WHEN ot.LineCount > 1 THEN 'Multi Line - Multi Unit'
                ELSE 'Other'
            END

        UNION ALL

        SELECT
            'Total' AS `S/M Unit`,
            FORMAT(COALESCE(COUNT(DISTINCT ot.Order_Number), 0), 0) AS `Orders`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(DISTINCT ot.Order_Number), 0) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
            FORMAT(COALESCE(COUNT(*), 0), 0) AS `Lines`,
            CONCAT(ROUND(100.0 * COALESCE(COUNT(*), 0) / (SELECT TotalLineCount FROM TotalOrders), 2), '%') AS `%Lines`,
            FORMAT(COALESCE(COUNT(DISTINCT td.SKU), 0), 0) AS `SKUs`,
            FORMAT(COALESCE(SUM(td.Qty), 0), 0) AS `Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Qty), 0) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
            FORMAT(COALESCE(SUM(td.Pallet_Picks), 0), 0) AS `Pallet_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Picks), 0) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
            FORMAT(COALESCE(SUM(td.Pallet_Units), 0), 0) AS `Pallet_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Pallet_Units), 0) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
            FORMAT(COALESCE(SUM(td.Layer_Picks), 0), 0) AS `Layer_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Picks), 0) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
            FORMAT(COALESCE(SUM(td.Layer_Units), 0), 0) AS `Layer_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Layer_Units), 0) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
            FORMAT(COALESCE(SUM(td.Case_Picks), 0), 0) AS `Case_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Picks), 0) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
            FORMAT(COALESCE(SUM(td.Case_Units), 0), 0) AS `Case_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Case_Units), 0) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
            FORMAT(COALESCE(SUM(td.Inner_Picks), 0), 0) AS `Inner_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Picks), 0) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
            FORMAT(COALESCE(SUM(td.Inner_Units), 0), 0) AS `Inner_Units`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Inner_Units), 0) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
            FORMAT(COALESCE(SUM(td.Each_Picks), 0), 0) AS `Each_Picks`,
            CONCAT(ROUND(100.0 * COALESCE(SUM(td.Each_Picks), 0) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
        FROM 
            OrderTotals ot
        JOIN 
            {tbl} td
            ON ot.Order_Number = td.Order_Number
    ) AS Results
     ORDER BY
        CASE
            WHEN `S/M Unit` = 'Single Line - Single Unit' THEN 3
            WHEN `S/M Unit` = 'Single Line - Multi Unit' THEN 2
            WHEN `S/M Unit` = 'Multi Line - Multi Unit' THEN 1
            WHEN `S/M Unit` = 'Other' THEN 998
            WHEN `S/M Unit` = 'Total' THEN 999
            ELSE 9999
        END;
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        # Clear the tree view
        for item in sm_unit_tree.get_children():
            sm_unit_tree.delete(item)

        # Insert rows into the treeview
        for row in rows:
            if row[0] == "Total":
                sm_unit_tree.insert('', 'end', values=row, tags=('total',))
            else:
                sm_unit_tree.insert('', 'end', values=row)

        sm_unit_tree.tag_configure('total', background='#d3d3d3', font=('Helvetica', 10, 'bold'))

        messagebox.showinfo("Success", "S/M Unit Summary loaded successfully!")
    except pymysql.Error as err:
        messagebox.showerror("Query Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

        filters = get_selected_filters()
        display_sm_unit_summary(filters)  # Call to display summary

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


def final_main(notebook):
    global lower_range_vars,upper_range_vars,left_frame,add_range_button,reduce_range_button
    global order_type_var,business_unit_var,dc_name_var,order_type_menu,business_unit_menu,dc_name_menu
    global order_min_max_date_var,order_start_date_entry,order_end_date_entry,single_unit_var,order_categories_tree
    global display_order_categories_summary,sm_unit_tree,display_sm_unit_summary,current_range_count
    global order_categories_summary_frame,sm_unit_summary_frame,min_date_sql_outbound,max_date_sql_outbound,order_start_date_entry_outbound,order_end_date_entry_outbound,min_date_sql_outbound
    # # Creating the Tkinter GUI
    # root = tk.Tk()
    # root.title("SQL Data Fetcher")
    #
    # Set style for bold column headers in Treeview
    style = ttk.Style()
    style.configure("Treeview.Heading", font=('TkDefaultFont', 10, 'bold'))  # Bold font for headers

    order_type_var = tk.StringVar(value="ALL")
    business_unit_var = tk.StringVar(value="ALL")
    dc_name_var = tk.StringVar(value="ALL")
    order_min_max_date_var = tk.StringVar()

    #fetch_min_max_dates_outbound()  # Fetch the min and max dates from the outbound table
    # Main Frame Layout (Centered content)
    # main_frame = ttk.Frame(root)
    # main_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
    main_frame = ttk.Frame(notebook)
    main_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
    notebook.add(main_frame, text="Order Profile")

    # Create a left frame to hold the range inputs and buttons
    left_frame = ttk.Frame(main_frame)
    left_frame.pack(side="left",fill="y", padx=10, pady=10)

    # Filters and Date range inputs
    center_frame = ttk.Frame(main_frame)
    center_frame.pack(pady=20, padx=10)
    # Fetch channel Names
    channel_names_outbound = Shared.channel_names_outbound #get_distinct_channel_filter_outbound()
    # Channel Filter
    channel_label = ttk.Label(center_frame, text="Order Type:", font=('Arial', 10))
    channel_label.grid(row=0, column=0, padx=10, pady=10)
    channel_filter = ttk.Combobox(center_frame, values=channel_names_outbound, state='readonly',
                                  font=('Arial', 10), textvariable=order_type_var)
    channel_filter.set("ALL")  # Set default value
    channel_filter.grid(row=0, column=1, padx=10, pady=10)
    # Filters input fields
    # ttk.Label(center_frame, text="Order Type:").grid(row=0, column=0, padx=10, pady=10)
    # order_type_menu = ttk.Combobox(center_frame, textvariable=order_type_var)
    # order_type_menu.grid(row=0, column=1, padx=10, pady=10)
    # order_type_menu.bind("<<ComboboxSelected>>", update_filters)
    # Fetch BU Names
    bu_names_outbound = Shared.bu_names_outbound #get_distinct_bu_filter_outbound()
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
    dc_names_outbound = Shared.dc_names_outbound #get_distinct_dc_names_outbound()
    dc_label_outbound = ttk.Label(center_frame, text="DC Name:", font=('Arial', 10))
    dc_label_outbound.grid(row=0, column=4, padx=10, pady=10)
    dc_filter = ttk.Combobox(center_frame, values=dc_names_outbound, state='readonly', font=('Arial', 10), textvariable=dc_name_var)
    dc_filter.set("ALL")  # Set the default value to "All"
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
            order_start_date_entry_outbound.set_date(Shared.min_date_sql_outbound_date)  # Reset start date to the min date
            order_end_date_entry_outbound.set_date(Shared.max_date_sql_outbound_date)  # Reset end date to the max date

    # Date Entry fields for Order Dates
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
    # Left Frame content (Range Inputs)
    range_label = ttk.Label(left_frame, text="Provide the range for order categorization", font=('Helvetica', 14, 'bold'))
    range_label.grid(row=0, column=0, columnspan=3, pady=(0, 10), sticky=tk.W + tk.E)

    # Left Frame for Single Unit and Ranges
    ttk.Label(left_frame, text="Single Unit Orders:").grid(row=1, column=0, padx=5, pady=5)
    single_unit_var = tk.IntVar(value=1)
    # print(single_unit_var)
    ttk.Entry(left_frame, textvariable=single_unit_var, width=5).grid(row=1, column=1, padx=5, pady=5)
    # print(f"Initial Single Unit Orders value: {single_unit_var.get()}")

    lower_range_vars = [tk.IntVar(value=i * 2 + 2) for i in range(3)]
    # print(lower_range_vars)
    upper_range_vars = [tk.IntVar(value=(i + 1) * 2 + 1) for i in range(3)]
    # print(upper_range_vars)

    current_range_count = 3

    # Generate initial Range inputs up to Range 3
    for i in range(current_range_count):
        ttk.Label(left_frame, text=f"Range {i + 1}:").grid(row=2 + i, column=0, padx=5, pady=5)
        ttk.Entry(left_frame, textvariable=lower_range_vars[i], width=5).grid(row=2 + i, column=1, padx=5, pady=5)
        # print(f"Initial Range {i + 1} Lower value: {lower_range_vars[i].get()}")
        # print(ttk.Entry)
        ttk.Entry(left_frame, textvariable=upper_range_vars[i], width=5).grid(row=2 + i, column=2, padx=5, pady=5)
        # print(f"Initial Range {i + 1} Upper value: {upper_range_vars[i].get()}")
        # print(ttk.Entry)

    # Add Range and Reduce Range Buttons (dynamic)
    add_range_button = tk.Button(left_frame, text="Add Range", command=add_range)
    add_range_button.grid(row=6, column=0, padx=10, pady=10)

    reduce_range_button = tk.Button(left_frame, text="Reduce Range", command=reduce_range)
    reduce_range_button.grid(row=6, column=1, padx=10, pady=10)

    # Initialize variables for summary frames
    order_categories_summary_frame = None
    sm_unit_summary_frame = None

    # Create a container for the summaries
    summary_container = ttk.Frame(main_frame)
    summary_container.pack(pady=10, padx=10, fill=tk.X)

    # Function to display Order Categories summary filters
    def display_order_categories_summary(filters):
        global order_categories_summary_frame
        if order_categories_summary_frame is not None:
            order_categories_summary_frame.destroy()
        order_categories_summary_frame = ttk.Frame(summary_container)
        order_categories_summary_frame.grid(row=0, column=0, padx=5, pady=10, sticky="nw")  # Position in row 0, column 0
        # ttk.Label(order_categories_summary_frame, text="Below was last filter applied to Order Categories summary:", font=("TkDefaultFont", 10, "italic", "bold")).pack(anchor='w')
        ttk.Label(order_categories_summary_frame, text=f"Order Type: {filters['order_type']}").pack(anchor='w')
        ttk.Label(order_categories_summary_frame, text=f"Business Unit: {filters['business_unit']}").pack(anchor='w')
        ttk.Label(order_categories_summary_frame, text=f"DC Name: {filters['dc_name']}").pack(anchor='w')
        ttk.Label(order_categories_summary_frame, text=f"Date Range: {filters['start_date']} to {filters['end_date']}").pack(anchor='w')

    # Function to display SM Unit summary filters
    def display_sm_unit_summary(filters):
        global sm_unit_summary_frame
        if sm_unit_summary_frame is not None:
            sm_unit_summary_frame.destroy()
        sm_unit_summary_frame = ttk.Frame(summary_container)
        sm_unit_summary_frame.grid(row=0, column=1, padx=5, pady=10, sticky="nw")  # Position in row 0, column 1
        # ttk.Label(sm_unit_summary_frame, text="Below was last filter applied to SM Unit summary:", font=("TkDefaultFont", 10, "italic", "bold")).pack(anchor='w')
        ttk.Label(sm_unit_summary_frame, text=f"Order Type: {filters['order_type']}").pack(anchor='w')
        ttk.Label(sm_unit_summary_frame, text=f"Business Unit: {filters['business_unit']}").pack(anchor='w')
        ttk.Label(sm_unit_summary_frame, text=f"DC Name: {filters['dc_name']}").pack(anchor='w')
        ttk.Label(sm_unit_summary_frame, text=f"Date Range: {filters['start_date']} to {filters['end_date']}").pack(anchor='w')


    # # Notebook for Tabs (Order Categories Summary & S/M Unit Summary)
    # notebook = ttk.Notebook(main_frame)
    # notebook.pack(expand=True, fill=tk.BOTH)
    # Create a new Notebook inside the SQL Data Fetcher tab to hold the two summaries
    summary_notebook = ttk.Notebook(main_frame)
    summary_notebook.pack(expand=True, fill=tk.BOTH)
    ### Frame for "Order Categories Summary"
    order_categories_frame = ttk.Frame(summary_notebook)

    # Treeview for Order Categories Summary with Scrollbars
    order_categories_tree = ttk.Treeview(order_categories_frame,
                                         columns=("Order Categories", "Orders", "%Orders", "Lines", "%Lines", "SKUs", "Units", "%Units", "Pallet_Picks", "%Pallet_Picks", "Pallet_Units", "%Pallet_Units", "Layer_Picks", "%Layer_Picks", "Layer_Units", "%Layer_Units", "Case_Picks", "%Case_Picks", "Case_Units", "%Case_Units", "Inner_Picks", "%Inner_Picks", "Inner_Units", "%Inner_Units", "Each_Picks", "%Each_Picks"),
                                         show='headings')

    # Configure headings and columns
    for col in order_categories_tree["columns"]:
        order_categories_tree.heading(col, text=col)  # Headers will automatically be bold due to the style
        order_categories_tree.column(col, width=150, anchor='center')

    # Add vertical and horizontal scrollbars for Order Categories
    vsb1 = ttk.Scrollbar(order_categories_frame, orient="vertical", command=order_categories_tree.yview)
    hsb1 = ttk.Scrollbar(order_categories_frame, orient="horizontal", command=order_categories_tree.xview)
    order_categories_tree.configure(yscrollcommand=vsb1.set, xscrollcommand=hsb1.set)

    # Pack Treeview and Scrollbars
    vsb1.pack(side=tk.RIGHT, fill='y')
    hsb1.pack(side=tk.BOTTOM, fill='x')
    order_categories_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    summary_notebook.add(order_categories_frame, text="Order Categories Summary")

    ### Frame for "S/M Unit Summary"
    sm_unit_frame = ttk.Frame(summary_notebook)

    # Treeview for S/M Unit Summary with Scrollbars
    sm_unit_tree = ttk.Treeview(sm_unit_frame,
                                columns=("S/M Unit", "Orders", "%Orders", "Lines", "%Lines", "SKUs", "Units", "%Units", "Pallet_Picks", "%Pallet_Picks", "Pallet_Units", "%Pallet_Units", "Layer_Picks", "%Layer_Picks", "Layer_Units", "%Layer_Units", "Case_Picks", "%Case_Picks", "Case_Units", "%Case_Units", "Inner_Picks", "%Inner_Picks", "Inner_Units", "%Inner_Units", "Each_Picks", "%Each_Picks"),
                                show='headings')

    # Configure headings and columns
    for col in sm_unit_tree["columns"]:
        sm_unit_tree.heading(col, text=col)  # Headers will automatically be bold due to the style
        sm_unit_tree.column(col, width=150, anchor='center')

    # Add vertical and horizontal scrollbars for S/M Unit Summary
    vsb2 = ttk.Scrollbar(sm_unit_frame, orient="vertical", command=sm_unit_tree.yview)
    hsb2 = ttk.Scrollbar(sm_unit_frame, orient="horizontal", command=sm_unit_tree.xview)
    sm_unit_tree.configure(yscrollcommand=vsb2.set, xscrollcommand=hsb2.set)

    # Pack Treeview and Scrollbars
    vsb2.pack(side=tk.RIGHT, fill='y')
    hsb2.pack(side=tk.BOTTOM, fill='x')
    sm_unit_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    summary_notebook.add(sm_unit_frame, text="S/M Unit Summary")

    # Execute and Export buttons
    buttons_frame = ttk.Frame(center_frame)
    buttons_frame.grid(row=2, column=0, columnspan=6, pady=10)

    execute_button = tk.Button(buttons_frame, text="Execute Order Categories Summary", command=execute_order_categories_query)
    execute_button.grid(row=0, column=0, padx=10)

    execute_sm_unit_button = tk.Button(buttons_frame, text="Execute S/M Unit Summary", command=execute_sm_unit_query)
    execute_sm_unit_button.grid(row=0, column=1, padx=10)

    def export_order_categories_summary():
        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                 filetypes=[("Excel files", "*.xlsx")],
                                                 title="Save Order Categories Summary As")
        if file_path:  # Check if user selected a file
            export_to_excel(order_categories_tree, file_path, get_selected_filters())

    def export_sm_unit_summary():
        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                 filetypes=[("Excel files", "*.xlsx")],
                                                 title="Save S/M Unit Summary As")
        if file_path:  # Check if user selected a file
            export_to_excel(sm_unit_tree, file_path, get_selected_filters())

    export_button = tk.Button(buttons_frame, text="Export Order Categories", command=export_order_categories_summary)
    export_button.grid(row=0, column=2, padx=10)

    export_sm_unit_button = tk.Button(buttons_frame, text="Export S/M Unit", command=export_sm_unit_summary)
    export_sm_unit_button.grid(row=0, column=3, padx=10)

    # Initialize filters
    # populate_filters()


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
    final_main(notebook)
    root.mainloop()

if __name__ == "__main__":
    main()


