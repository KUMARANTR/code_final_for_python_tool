import pandas as pd
# import mysql.connector
# import MySQLdb
import pymysql
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
from tkinter import filedialog
import Shared
from Order_Categories_Summary_SM_Unit_Summary_New import final_main
from Seasonality_1031 import SeasonalityApp
from Summary_tab_new import create_notebook_page_inbound_outbound
# tbl= f"client_data.{Shared.project}"
from Inbound_outbound_sql_code_testing_summary import get_distinct_dc_names_outbound,get_distinct_bu_filter_outbound,get_distinct_channel_filter_outbound,fetch_min_max_dates_outbound

from helper_functions import get_asset_path

sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')

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

        # # Set DateEntry limits
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

# Function to execute the SQL query for "SKU-wise Summary"
def execute_sku_summary_query():
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

    # Join filters
    filter_clause = " AND ".join(filters)
    if filter_clause:
        filter_clause = "WHERE " + filter_clause
    else:
        filter_clause = ""  # Ensure this is empty if there are no filters
    #SKU_WISE_SUMMARY
    query = f"""
    WITH OrderTotals AS (
        SELECT
            SKU,
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
        {filter_clause}  -- This would dynamically be substituted with the actual filters
        GROUP BY SKU, Order_Number
    ),
    TotalOrders AS (
        SELECT 
            COUNT(DISTINCT Order_Number) AS TotalOrderCount,
            COALESCE(SUM(LineCount), 0) AS LineCountSum, 
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
    SELECT 
        SKU,
        FORMAT(COUNT(DISTINCT Order_Number), 0) AS `Orders`,
        CONCAT(ROUND(100.0 * COUNT(DISTINCT Order_Number) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
        FORMAT(SUM(LineCount), 0) AS `Lines`,  -- Summing LineCount to get total lines per SKU
        CONCAT(ROUND(100.0 * SUM(LineCount) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
        FORMAT(SUM(TotalUnits), 0) AS `Units`,
        CONCAT(ROUND(100.0 * SUM(TotalUnits) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
        FORMAT(SUM(TotalPalletPicks), 0) AS `Pallet_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalPalletPicks) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
        FORMAT(SUM(TotalPalletUnits), 0) AS `Pallet_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalPalletUnits) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
        FORMAT(SUM(TotalLayerPicks), 0) AS `Layer_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalLayerPicks) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
        FORMAT(SUM(TotalLayerUnits), 0) AS `Layer_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalLayerUnits) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
        FORMAT(SUM(TotalCasePicks), 0) AS `Case_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalCasePicks) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
        FORMAT(SUM(TotalCaseUnits), 0) AS `Case_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalCaseUnits) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
        FORMAT(SUM(TotalInnerPicks), 0) AS `Inner_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalInnerPicks) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
        FORMAT(SUM(TotalInnerUnits), 0) AS `Inner_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalInnerUnits) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
        FORMAT(SUM(TotalEachPicks), 0) AS `Each_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalEachPicks) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
    FROM 
        OrderTotals
    GROUP BY SKU
    UNION ALL
    SELECT 
        'Total' AS SKU,
        FORMAT(COUNT(DISTINCT Order_Number), 0) AS `Orders`,
        CONCAT(ROUND(100.0 * COUNT(DISTINCT Order_Number) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
        FORMAT(SUM(LineCount), 0) AS `Lines`,  -- Summing LineCount to get total lines
        CONCAT(ROUND(100.0 * SUM(LineCount) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
        FORMAT(SUM(TotalUnits), 0) AS `Units`,
        CONCAT(ROUND(100.0 * SUM(TotalUnits) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
        FORMAT(SUM(TotalPalletPicks), 0) AS `Pallet_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalPalletPicks) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
        FORMAT(SUM(TotalPalletUnits), 0) AS `Pallet_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalPalletUnits) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
        FORMAT(SUM(TotalLayerPicks), 0) AS `Layer_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalLayerPicks) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
        FORMAT(SUM(TotalLayerUnits), 0) AS `Layer_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalLayerUnits) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
        FORMAT(SUM(TotalCasePicks), 0) AS `Case_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalCasePicks) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
        FORMAT(SUM(TotalCaseUnits), 0) AS `Case_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalCaseUnits) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
        FORMAT(SUM(TotalInnerPicks), 0) AS `Inner_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalInnerPicks) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
        FORMAT(SUM(TotalInnerUnits), 0) AS `Inner_Units`,
        CONCAT(ROUND(100.0 * SUM(TotalInnerUnits) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
        FORMAT(SUM(TotalEachPicks), 0) AS `Each_Picks`,
        CONCAT(ROUND(100.0 * SUM(TotalEachPicks) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
    FROM 
        OrderTotals
    ORDER BY CAST(REPLACE(`Units`, ',', '') AS UNSIGNED) DESC;  -- Sorting by Units, high to low
    """

    #     query = f"""
# WITH OrderTotals AS (
#     SELECT
#         SKU,
#         Order_Number,
#         SUM(Qty) AS TotalUnits,
#         SUM(Pallet_Picks) AS TotalPalletPicks,
#         SUM(Pallet_Units) AS TotalPalletUnits,
#         SUM(Layer_Picks) AS TotalLayerPicks,
#         SUM(Layer_Units) AS TotalLayerUnits,
#         SUM(Case_Picks) AS TotalCasePicks,
#         SUM(Case_Units) AS TotalCaseUnits,
#         SUM(Inner_Picks) AS TotalInnerPicks,
#         SUM(Inner_Units) AS TotalInnerUnits,
#         SUM(Each_Picks) AS TotalEachPicks,
#         COUNT(*) AS LineCount  -- New field to count the actual lines (rows)
#     FROM
#         {tbl}
#     {filter_clause}  -- This would dynamically be substituted with the actual filters
#     GROUP BY SKU, Order_Number
# ),
# TotalOrders AS (
#     SELECT
#         COUNT(DISTINCT Order_Number) AS TotalOrderCount,
#         SUM(LineCount) AS LineCountSum,
#         SUM(TotalUnits) AS TotalUnitsSum,
#         SUM(TotalPalletPicks) AS TotalPalletPicksSum,
#         SUM(TotalPalletUnits) AS TotalPalletUnitsSum,
#         SUM(TotalLayerPicks) AS TotalLayerPicksSum,
#         SUM(TotalLayerUnits) AS TotalLayerUnitsSum,
#         SUM(TotalCasePicks) AS TotalCasePicksSum,
#         SUM(TotalCaseUnits) AS TotalCaseUnitsSum,
#         SUM(TotalInnerPicks) AS TotalInnerPicksSum,
#         SUM(TotalInnerUnits) AS TotalInnerUnitsSum,
#         SUM(TotalEachPicks) AS TotalEachPicksSum
#     FROM OrderTotals
# )
# SELECT
#     SKU,
#     FORMAT(COUNT(DISTINCT Order_Number), 0) AS `Orders`,
#     CONCAT(ROUND(100.0 * COUNT(DISTINCT Order_Number) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
#     FORMAT(SUM(LineCount), 0) AS `Lines`,  -- Summing LineCount to get total lines per SKU
#     CONCAT(ROUND(100.0 * SUM(LineCount) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
#     FORMAT(SUM(TotalUnits), 0) AS `Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalUnits) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
#     FORMAT(SUM(TotalPalletPicks), 0) AS `Pallet_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletPicks) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
#     FORMAT(SUM(TotalPalletUnits), 0) AS `Pallet_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletUnits) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
#     FORMAT(SUM(TotalLayerPicks), 0) AS `Layer_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerPicks) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
#     FORMAT(SUM(TotalLayerUnits), 0) AS `Layer_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerUnits) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
#     FORMAT(SUM(TotalCasePicks), 0) AS `Case_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalCasePicks) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
#     FORMAT(SUM(TotalCaseUnits), 0) AS `Case_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalCaseUnits) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
#     FORMAT(SUM(TotalInnerPicks), 0) AS `Inner_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerPicks) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
#     FORMAT(SUM(TotalInnerUnits), 0) AS `Inner_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerUnits) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
#     FORMAT(SUM(TotalEachPicks), 0) AS `Each_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalEachPicks) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
# FROM
#     OrderTotals
# GROUP BY SKU
# UNION ALL
# SELECT
#     'Total' AS SKU,
#     FORMAT(COUNT(DISTINCT Order_Number), 0) AS `Orders`,
#     CONCAT(ROUND(100.0 * COUNT(DISTINCT Order_Number) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
#     FORMAT(SUM(LineCount), 0) AS `Lines`,  -- Summing LineCount to get total lines
#     CONCAT(ROUND(100.0 * SUM(LineCount) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
#     FORMAT(SUM(TotalUnits), 0) AS `Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalUnits) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
#     FORMAT(SUM(TotalPalletPicks), 0) AS `Pallet_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletPicks) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
#     FORMAT(SUM(TotalPalletUnits), 0) AS `Pallet_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletUnits) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
#     FORMAT(SUM(TotalLayerPicks), 0) AS `Layer_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerPicks) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
#     FORMAT(SUM(TotalLayerUnits), 0) AS `Layer_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerUnits) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
#     FORMAT(SUM(TotalCasePicks), 0) AS `Case_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalCasePicks) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
#     FORMAT(SUM(TotalCaseUnits), 0) AS `Case_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalCaseUnits) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
#     FORMAT(SUM(TotalInnerPicks), 0) AS `Inner_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerPicks) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
#     FORMAT(SUM(TotalInnerUnits), 0) AS `Inner_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerUnits) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
#     FORMAT(SUM(TotalEachPicks), 0) AS `Each_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalEachPicks) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
# FROM
#     OrderTotals
# ORDER BY CAST(REPLACE(`Units`, ',', '') AS UNSIGNED) DESC;  -- Sorting by Units, high to low
#     """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        # Check if rows were returned
        if not rows:
            messagebox.showwarning("Warning", "No data found for the selected filters!")
            return

        # Clear the tree view before inserting new data
        for item in SKU_wise_tree.get_children():
            SKU_wise_tree.delete(item)

        total_row = None  # Initialize total_row

        # Insert rows into the Treeview
        for row in rows:
            if row[0] == 'Total':
                total_row = row  # Store the total row temporarily
            else:
                SKU_wise_tree.insert('', 'end', values=row)

        # After inserting all rows, insert the total row at the end
        if total_row:
            SKU_wise_tree.insert('', 'end', values=total_row, tags=('total',))

        # Apply bold styling to the total row
        SKU_wise_tree.tag_configure('total', background='#d3d3d3', font=('TkDefaultFont', 10, 'bold'))

        messagebox.showinfo("Success", "SKU-wise Summary loaded successfully!")
    except pymysql.Error as err:
        messagebox.showerror("Query Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

        filters = get_selected_filters()
        display_sku_summary(filters)  # Call to display summary

# Function to execute the SQL query for "Date-wise Summary"
def execute_date_summary_query():
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

    # Join filters
    filter_clause = " AND ".join(filters)
    if filter_clause:
        filter_clause = "WHERE " + filter_clause
    else:
        filter_clause = ""  # Ensure this is empty if there are no filters
    #Date_wise_summary
    query = f"""
    WITH OrderTotals AS (
        SELECT
            Order_Date,
            Order_Number,
            SKU,
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
            COALESCE(COUNT(*), 0) AS LineCount  -- New field to count the actual lines (rows)
        FROM
            {tbl}
        {filter_clause}  -- This would dynamically be substituted with the actual filters
        GROUP BY Order_Date, Order_Number, SKU
    ),
    TotalOrders AS (
        SELECT 
            COALESCE(COUNT(DISTINCT Order_Number), 0) AS TotalOrderCount, 
            COALESCE(SUM(LineCount), 0) AS LineCountSum,
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
    SELECT 
        Order_Date,
        FORMAT(COALESCE(COUNT(DISTINCT Order_Number), 0), 0) AS `Orders`,
        CONCAT(ROUND(100.0 * COALESCE(COUNT(DISTINCT Order_Number), 0) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
        FORMAT(COALESCE(SUM(LineCount), 0), 0) AS `Lines`,  -- Summing LineCount to get total lines per Order_Date
        CONCAT(ROUND(100.0 * COALESCE(SUM(LineCount), 0) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
        FORMAT(COALESCE(COUNT(DISTINCT SKU), 0), 0) AS `SKUs`,  -- Added distinct SKU count
        FORMAT(COALESCE(SUM(TotalUnits), 0), 0) AS `Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalUnits), 0) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
        FORMAT(COALESCE(SUM(TotalPalletPicks), 0), 0) AS `Pallet_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalPalletPicks), 0) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
        FORMAT(COALESCE(SUM(TotalPalletUnits), 0), 0) AS `Pallet_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalPalletUnits), 0) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
        FORMAT(COALESCE(SUM(TotalLayerPicks), 0), 0) AS `Layer_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalLayerPicks), 0) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
        FORMAT(COALESCE(SUM(TotalLayerUnits), 0), 0) AS `Layer_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalLayerUnits), 0) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
        FORMAT(COALESCE(SUM(TotalCasePicks), 0), 0) AS `Case_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalCasePicks), 0) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
        FORMAT(COALESCE(SUM(TotalCaseUnits), 0), 0) AS `Case_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalCaseUnits), 0) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
        FORMAT(COALESCE(SUM(TotalInnerPicks), 0), 0) AS `Inner_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalInnerPicks), 0) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
        FORMAT(COALESCE(SUM(TotalInnerUnits), 0), 0) AS `Inner_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalInnerUnits), 0) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
        FORMAT(COALESCE(SUM(TotalEachPicks), 0), 0) AS `Each_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalEachPicks), 0) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
    FROM 
        OrderTotals
    GROUP BY Order_Date
    UNION ALL
    SELECT 
        'Total' AS Order_Date,
        FORMAT(COALESCE(COUNT(DISTINCT Order_Number), 0), 0) AS `Orders`,
        CONCAT(ROUND(100.0 * COALESCE(COUNT(DISTINCT Order_Number), 0) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
        FORMAT(COALESCE(SUM(LineCount), 0), 0) AS `Lines`,  -- Summing LineCount to get total lines for all dates
        CONCAT(ROUND(100.0 * COALESCE(SUM(LineCount), 0) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
        FORMAT(COALESCE(COUNT(DISTINCT SKU), 0), 0) AS `SKUs`,  -- Added distinct SKU count for the total row
        FORMAT(COALESCE(SUM(TotalUnits), 0), 0) AS `Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalUnits), 0) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
        FORMAT(COALESCE(SUM(TotalPalletPicks), 0), 0) AS `Pallet_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalPalletPicks), 0) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
        FORMAT(COALESCE(SUM(TotalPalletUnits), 0), 0) AS `Pallet_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalPalletUnits), 0) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
        FORMAT(COALESCE(SUM(TotalLayerPicks), 0), 0) AS `Layer_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalLayerPicks), 0) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
        FORMAT(COALESCE(SUM(TotalLayerUnits), 0), 0) AS `Layer_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalLayerUnits), 0) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
        FORMAT(COALESCE(SUM(TotalCasePicks), 0), 0) AS `Case_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalCasePicks), 0) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
        FORMAT(COALESCE(SUM(TotalCaseUnits), 0), 0) AS `Case_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalCaseUnits), 0) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
        FORMAT(COALESCE(SUM(TotalInnerPicks), 0), 0) AS `Inner_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalInnerPicks), 0) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
        FORMAT(COALESCE(SUM(TotalInnerUnits), 0), 0) AS `Inner_Units`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalInnerUnits), 0) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
        FORMAT(COALESCE(SUM(TotalEachPicks), 0), 0) AS `Each_Picks`,
        CONCAT(ROUND(100.0 * COALESCE(SUM(TotalEachPicks), 0) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
    FROM 
        OrderTotals
ORDER BY Order_Date DESC;     
    """

    #     query = f"""
# WITH OrderTotals AS (
#     SELECT
#         Order_Date,
#         Order_Number,
#         SKU,
#         SUM(Qty) AS TotalUnits,
#         SUM(Pallet_Picks) AS TotalPalletPicks,
#         SUM(Pallet_Units) AS TotalPalletUnits,
#         SUM(Layer_Picks) AS TotalLayerPicks,
#         SUM(Layer_Units) AS TotalLayerUnits,
#         SUM(Case_Picks) AS TotalCasePicks,
#         SUM(Case_Units) AS TotalCaseUnits,
#         SUM(Inner_Picks) AS TotalInnerPicks,
#         SUM(Inner_Units) AS TotalInnerUnits,
#         SUM(Each_Picks) AS TotalEachPicks,
#         COUNT(*) AS LineCount  -- New field to count the actual lines (rows)
#     FROM
#         {tbl}
#     {filter_clause}  -- This would dynamically be substituted with the actual filters
#     GROUP BY Order_Date, Order_Number, SKU
# ),
# TotalOrders AS (
#     SELECT
#         COUNT(DISTINCT Order_Number) AS TotalOrderCount,
#         SUM(LineCount) AS LineCountSum,
#         SUM(TotalUnits) AS TotalUnitsSum,
#         SUM(TotalPalletPicks) AS TotalPalletPicksSum,
#         SUM(TotalPalletUnits) AS TotalPalletUnitsSum,
#         SUM(TotalLayerPicks) AS TotalLayerPicksSum,
#         SUM(TotalLayerUnits) AS TotalLayerUnitsSum,
#         SUM(TotalCasePicks) AS TotalCasePicksSum,
#         SUM(TotalCaseUnits) AS TotalCaseUnitsSum,
#         SUM(TotalInnerPicks) AS TotalInnerPicksSum,
#         SUM(TotalInnerUnits) AS TotalInnerUnitsSum,
#         SUM(TotalEachPicks) AS TotalEachPicksSum
#     FROM OrderTotals
# )
# SELECT
#     Order_Date,
#     FORMAT(COUNT(DISTINCT Order_Number), 0) AS `Orders`,
#     CONCAT(ROUND(100.0 * COUNT(DISTINCT Order_Number) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
#     FORMAT(SUM(LineCount), 0) AS `Lines`,  -- Summing LineCount to get total lines per Order_Date
#     CONCAT(ROUND(100.0 * SUM(LineCount) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
#     FORMAT(COUNT(DISTINCT SKU), 0) AS `SKUs`,  -- Added distinct SKU count
#     FORMAT(SUM(TotalUnits), 0) AS `Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalUnits) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
#     FORMAT(SUM(TotalPalletPicks), 0) AS `Pallet_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletPicks) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
#     FORMAT(SUM(TotalPalletUnits), 0) AS `Pallet_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletUnits) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
#     FORMAT(SUM(TotalLayerPicks), 0) AS `Layer_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerPicks) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
#     FORMAT(SUM(TotalLayerUnits), 0) AS `Layer_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerUnits) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
#     FORMAT(SUM(TotalCasePicks), 0) AS `Case_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalCasePicks) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
#     FORMAT(SUM(TotalCaseUnits), 0) AS `Case_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalCaseUnits) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
#     FORMAT(SUM(TotalInnerPicks), 0) AS `Inner_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerPicks) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
#     FORMAT(SUM(TotalInnerUnits), 0) AS `Inner_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerUnits) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
#     FORMAT(SUM(TotalEachPicks), 0) AS `Each_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalEachPicks) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
# FROM
#     OrderTotals
# GROUP BY Order_Date
# UNION ALL
# SELECT
#     'Total' AS Order_Date,
#     FORMAT(COUNT(DISTINCT Order_Number), 0) AS `Orders`,
#     CONCAT(ROUND(100.0 * COUNT(DISTINCT Order_Number) / (SELECT TotalOrderCount FROM TotalOrders), 2), '%') AS `%Orders`,
#     FORMAT(SUM(LineCount), 0) AS `Lines`,  -- Summing LineCount to get total lines for all dates
#     CONCAT(ROUND(100.0 * SUM(LineCount) / (SELECT LineCountSum FROM TotalOrders), 2), '%') AS `%Lines`,
#     FORMAT(COUNT(DISTINCT SKU), 0) AS `SKUs`,  -- Added distinct SKU count for the total row
#     FORMAT(SUM(TotalUnits), 0) AS `Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalUnits) / (SELECT TotalUnitsSum FROM TotalOrders), 2), '%') AS `%Units`,
#     FORMAT(SUM(TotalPalletPicks), 0) AS `Pallet_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletPicks) / (SELECT TotalPalletPicksSum FROM TotalOrders), 2), '%') AS `%Pallet_Picks`,
#     FORMAT(SUM(TotalPalletUnits), 0) AS `Pallet_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalPalletUnits) / (SELECT TotalPalletUnitsSum FROM TotalOrders), 2), '%') AS `%Pallet_Units`,
#     FORMAT(SUM(TotalLayerPicks), 0) AS `Layer_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerPicks) / (SELECT TotalLayerPicksSum FROM TotalOrders), 2), '%') AS `%Layer_Picks`,
#     FORMAT(SUM(TotalLayerUnits), 0) AS `Layer_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalLayerUnits) / (SELECT TotalLayerUnitsSum FROM TotalOrders), 2), '%') AS `%Layer_Units`,
#     FORMAT(SUM(TotalCasePicks), 0) AS `Case_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalCasePicks) / (SELECT TotalCasePicksSum FROM TotalOrders), 2), '%') AS `%Case_Picks`,
#     FORMAT(SUM(TotalCaseUnits), 0) AS `Case_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalCaseUnits) / (SELECT TotalCaseUnitsSum FROM TotalOrders), 2), '%') AS `%Case_Units`,
#     FORMAT(SUM(TotalInnerPicks), 0) AS `Inner_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerPicks) / (SELECT TotalInnerPicksSum FROM TotalOrders), 2), '%') AS `%Inner_Picks`,
#     FORMAT(SUM(TotalInnerUnits), 0) AS `Inner_Units`,
#     CONCAT(ROUND(100.0 * SUM(TotalInnerUnits) / (SELECT TotalInnerUnitsSum FROM TotalOrders), 2), '%') AS `%Inner_Units`,
#     FORMAT(SUM(TotalEachPicks), 0) AS `Each_Picks`,
#     CONCAT(ROUND(100.0 * SUM(TotalEachPicks) / (SELECT TotalEachPicksSum FROM TotalOrders), 2), '%') AS `%Each_Picks`
# FROM
#     OrderTotals
# ORDER BY Order_Date DESC;
# """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        # Check if rows were returned
        if not rows:
            messagebox.showwarning("Warning", "No data found for the selected filters!")
            return

        # Clear the tree view before inserting new data
        for item in Date_wise_tree.get_children():
            Date_wise_tree.delete(item)

        total_row = None  # Initialize total_row

        # Insert rows into the Treeview
        for row in rows:
            if row[0] == 'Total':
                total_row = row  # Store the total row temporarily
            else:
                Date_wise_tree.insert('', 'end', values=row)

        # After inserting all rows, insert the total row at the end
        if total_row:
            Date_wise_tree.insert('', 'end', values=total_row, tags=('total',))

        # Apply bold styling to the total row
        Date_wise_tree.tag_configure('total', background='#d3d3d3', font=('TkDefaultFont', 10, 'bold'))

        messagebox.showinfo("Success", "Date-wise Summary loaded successfully!")
    except pymysql.Error as err:
        messagebox.showerror("Query Error", f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

        filters = get_selected_filters()
        display_date_summary(filters)  # Call to display summary


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
def final_main_sku_date_analysis(notebook):

    global order_type_var,business_unit_var,dc_name_var,order_type_menu,business_unit_menu,dc_name_menu
    global order_min_max_date_var,order_start_date_entry,order_end_date_entry,SKU_wise_tree,display_sku_summary
    global Date_wise_tree,display_date_summary,display_sku_summary,sku_summary_frame,date_summary_frame,order_start_date_entry_outbound,order_end_date_entry_outbound
    # # Creating the Tkinter GUI
    # root = tk.Tk()
    # root.title("SQL Data Fetcher")

    style = ttk.Style()
    style.configure("Treeview.Heading", font=('TkDefaultFont', 10, 'bold'))

    #fetch_min_max_dates_outbound()
    order_type_var = tk.StringVar(value="ALL")
    business_unit_var = tk.StringVar(value="ALL")
    dc_name_var = tk.StringVar(value="ALL")
    order_min_max_date_var = tk.StringVar()

    main_frame = ttk.Frame(notebook)
    main_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
    notebook.add(main_frame, text="Skuwise_Datewise_Summary")

    # main_frame = ttk.Frame(root)
    # main_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

    center_frame = ttk.Frame(main_frame)
    center_frame.pack(pady=20, padx=10)
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
    sku_summary_frame = None
    date_summary_frame = None

    # Create a container for the summaries
    summary_container = ttk.Frame(main_frame)
    summary_container.pack(pady=10, padx=10, fill=tk.X)

    # Function to display SKU summary filters
    def display_sku_summary(filters):
        global sku_summary_frame
        if sku_summary_frame is not None:
            sku_summary_frame.destroy()
        sku_summary_frame = ttk.Frame(summary_container)
        sku_summary_frame.grid(row=0, column=0, padx=5, pady=10, sticky="nw")  # Position in row 0, column 0
        # ttk.Label(sku_summary_frame, text="Below was last filter applied to SKU wise summary:", font=("TkDefaultFont", 10, "italic", "bold")).pack(anchor='w')
        ttk.Label(sku_summary_frame, text=f"Order Type: {filters['order_type']}").pack(anchor='w')
        ttk.Label(sku_summary_frame, text=f"Business Unit: {filters['business_unit']}").pack(anchor='w')
        ttk.Label(sku_summary_frame, text=f"DC Name: {filters['dc_name']}").pack(anchor='w')
        ttk.Label(sku_summary_frame, text=f"Date Range: {filters['start_date']} to {filters['end_date']}").pack(anchor='w')

    # Function to display Date summary filters
    def display_date_summary(filters):
        global date_summary_frame
        if date_summary_frame is not None:
            date_summary_frame.destroy()
        date_summary_frame = ttk.Frame(summary_container)
        date_summary_frame.grid(row=0, column=1, padx=5, pady=10, sticky="nw")  # Position in row 0, column 1
        # ttk.Label(date_summary_frame, text="Below was last filter applied to Date wise summary:", font=("TkDefaultFont", 10, "italic", "bold")).pack(anchor='w')
        ttk.Label(date_summary_frame, text=f"Order Type: {filters['order_type']}").pack(anchor='w')
        ttk.Label(date_summary_frame, text=f"Business Unit: {filters['business_unit']}").pack(anchor='w')
        ttk.Label(date_summary_frame, text=f"DC Name: {filters['dc_name']}").pack(anchor='w')
        ttk.Label(date_summary_frame, text=f"Date Range: {filters['start_date']} to {filters['end_date']}").pack(anchor='w')

    # # Notebook for Tabs (SKU Summary & Date Summary)
    # notebook = ttk.Notebook(main_frame)
    # notebook.pack(expand=True, fill=tk.BOTH)
    # Create a new Notebook inside the SQL Data Fetcher tab to hold the two summaries
    summary_notebook = ttk.Notebook(main_frame)
    summary_notebook.pack(expand=True, fill=tk.BOTH)

    # Frame for "SKU-wise Summary"
    sku_frame = ttk.Frame(summary_notebook)

    # Treeview for SKU Summary with Scrollbars
    SKU_wise_tree = ttk.Treeview(sku_frame,
                                 columns=("SKU", "Orders", "%Orders", "Lines", "%Lines", "Units", "%Units", "Pallet_Picks", "%Pallet_Picks", "Pallet_Units", "%Pallet_Units", "Layer_Picks", "%Layer_Picks", "Layer_Units", "%Layer_Units", "Case_Picks", "%Case_Picks", "Case_Units", "%Case_Units", "Inner_Picks", "%Inner_Picks", "Inner_Units", "%Inner_Units", "Each_Picks", "%Each_Picks"),
                                 show='headings')

    for col in SKU_wise_tree["columns"]:
        SKU_wise_tree.heading(col, text=col)
        SKU_wise_tree.column(col, width=150, anchor='center')

    vsb1 = ttk.Scrollbar(sku_frame, orient="vertical", command=SKU_wise_tree.yview)
    hsb1 = ttk.Scrollbar(sku_frame, orient="horizontal", command=SKU_wise_tree.xview)
    SKU_wise_tree.configure(yscrollcommand=vsb1.set, xscrollcommand=hsb1.set)

    vsb1.pack(side=tk.RIGHT, fill='y')
    hsb1.pack(side=tk.BOTTOM, fill='x')
    SKU_wise_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    summary_notebook.add(sku_frame, text="SKU-wise Summary")

    # Frame for "Date-wise Summary"
    date_frame = ttk.Frame(summary_notebook)

    # Treeview for Date Summary with Scrollbars
    Date_wise_tree = ttk.Treeview(date_frame,
                                columns=("Order_Date", "Orders", "%Orders", "Lines", "%Lines", "SKUs", "Units", "%Units", "Pallet_Picks", "%Pallet_Picks", "Pallet_Units", "%Pallet_Units", "Layer_Picks", "%Layer_Picks", "Layer_Units", "%Layer_Units", "Case_Picks", "%Case_Picks", "Case_Units", "%Case_Units", "Inner_Picks", "%Inner_Picks", "Inner_Units", "%Inner_Units", "Each_Picks", "%Each_Picks"),
                                show='headings')

    for col in Date_wise_tree["columns"]:
        Date_wise_tree.heading(col, text=col)
        Date_wise_tree.column(col, width=150, anchor='center')

    vsb2 = ttk.Scrollbar(date_frame, orient="vertical", command=Date_wise_tree.yview)
    hsb2 = ttk.Scrollbar(date_frame, orient="horizontal", command=Date_wise_tree.xview)
    Date_wise_tree.configure(yscrollcommand=vsb2.set, xscrollcommand=hsb2.set)

    vsb2.pack(side=tk.RIGHT, fill='y')
    hsb2.pack(side=tk.BOTTOM, fill='x')
    Date_wise_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    summary_notebook.add(date_frame, text="Date-wise Summary")

    # Buttons for executing and exporting the data
    buttons_frame = ttk.Frame(center_frame)
    buttons_frame.grid(row=2, column=0, columnspan=6, pady=10)

    execute_button = tk.Button(buttons_frame, text="Execute SKU Summary", command=execute_sku_summary_query)
    execute_button.grid(row=0, column=0, padx=10)

    execute_sm_unit_button = tk.Button(buttons_frame, text="Execute Date Summary", command=execute_date_summary_query)
    execute_sm_unit_button.grid(row=0, column=1, padx=10)

    def export_sku_summary():
        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                 filetypes=[("Excel files", "*.xlsx")],
                                                 title="Save SKU Summary As")
        if file_path:  # Check if user selected a file
            export_to_excel(SKU_wise_tree, file_path, get_selected_filters())

    def export_date_summary():
        file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                 filetypes=[("Excel files", "*.xlsx")],
                                                 title="Save Date Summary As")
        if file_path:  # Check if user selected a file
            export_to_excel(Date_wise_tree, file_path, get_selected_filters())

    export_button = tk.Button(buttons_frame, text="Export SKU Summary", command=export_sku_summary)
    export_button.grid(row=0, column=2, padx=10)

    export_sm_unit_button = tk.Button(buttons_frame, text="Export Date Summary", command=export_date_summary)
    export_sm_unit_button.grid(row=0, column=3, padx=10)

    # Initialize filters
    # populate_filters()

def main():
    # Creating the Tkinter GUI

    root = tk.Tk()
    root.title("Data Summary")
    # Set window size and start main loop
    fetch_min_max_dates_outbound()
    root.geometry("1200x800")
    # Notebook for Tabs (Order Categories Summary & S/M Unit Summary)
    notebook = ttk.Notebook(root)
    notebook.pack(expand=True, fill=tk.BOTH)
    # create_notebook_page_inbound_outbound(notebook)
    print(f"Testing Inbound Table Name: {Shared.project_inbound}")
    # SeasonalityApp(notebook)
    final_main(notebook)
    final_main_sku_date_analysis(notebook)
    root.mainloop()

if __name__ == "__main__":
    main()
