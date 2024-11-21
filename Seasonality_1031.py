import pandas as pd
import pymysql
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates
from decimal import Decimal
import tkinter as tk
from tkinter import ttk, messagebox, Scrollbar, Frame
from tkcalendar import DateEntry
from tkinter import filedialog
from tkinter import simpledialog
# import config as cfg
import Shared
from helper_functions import get_asset_path
# from Engineering_metrics_python.all5th import create_tab1, create_tab2, create_tab3
# from Engineering_metrics_python.Order_Categories_Summary_SM_Unit_Summary import final_main
# from Engineering_metrics_python.new_tab import create_notebook_page_inbound_outbound
from Inbound_outbound_sql_code_testing_summary import get_distinct_dc_names ,get_distinct_dc_names_outbound,get_distinct_bu_filter_outbound,get_distinct_channel_filter_outbound

# tbl = Shared.projtbl
# inbound_datatbl =Shared.inboundtbl

# inbound_datatbl=f"client_data.{Shared.project_inbound}"


sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')

# print(f"Testing Inbound Table Name: {Shared.project_inbound}")
# class SeasonalityApp:
#     # inbound_datatbl = f"client_data.{Shared.project_inbound}"
#     def __init__(self, notebook):
#         self.tbl = f"client_data.{Shared.project}"
#         self.inbound_datatbl = f"client_data.{Shared.project_inbound}"
#         # inbound_datatbl = f"client_data.{Shared.project_inbound}"
#         # self.root = root
#         # self.root.title("Seasonality Analysis")
#         # Remove root, use the passed notebook
#         self.notebook = notebook
#         # print(f"Saved Inbound Project: {Shared.project_inbound}")
#         # print(f"Saved Outbound Project: {Shared.project}")
#         # Connect to the database
#         self.connection = pymysql.connect(
#             # host=cfg.host,
#             # user=cfg.user,
#             # passwd=cfg.passwd,
#             # db=cfg.db,
#             # port=cfg.port,
#             # ssl_ca=cfg.ssl_ca,
#             # ssl_key=cfg.ssl_key,
#             # ssl_cert=cfg.ssl_cert
#
#         host = '10.216.252.8',  #
#         user = Shared.userid,
#         passwd = Shared.password,
#         db = 'client_data',
#         port = 3306,
#         ssl_ca = sslca,
#         ssl_key = sslkey,
#         ssl_cert = sslcert
#         )
#
#         # Fetch unique DC_NAMES for the dropdown
#         cursor = self.connection.cursor()
#         cursor.execute(f"SELECT DISTINCT Destination_DC FROM {self.inbound_datatbl}")
#         if not Shared.project_inbound:
#             print("Inbound table name is missing. Skipping distinct DC names query.")
#             return None
#         self.unique_dc_names_ib = ["ALL"]+[row[0] for row in cursor.fetchall()]
#
#         # Fetch min and max dates
#         cursor.execute(f"SELECT MIN(Received_Date), MAX(Received_Date) FROM {self.inbound_datatbl}")
#         self.min_date_ib, self.max_date_ib = cursor.fetchone()
#
#         # Fetch unique DC_NAMES, BUSINESSUNITS, and ORDERTYPES for the dropdown
#         cursor = self.connection.cursor()
#         cursor.execute(f"SELECT DISTINCT DC_Name FROM {self.tbl}")
#         self.unique_dc_names_ob = ["ALL"]+[row[0] for row in cursor.fetchall()]
#
#         cursor.execute(f"SELECT DISTINCT Business_Unit FROM {self.tbl}")
#         self.unique_business_units = ["ALL"]+[row[0] for row in cursor.fetchall()]
#
#         cursor.execute(f"SELECT DISTINCT Order_Type FROM {self.tbl}")
#         self.unique_order_types = ["ALL"]+[row[0] for row in cursor.fetchall()]
#
#         # Fetch min and max dates
#         cursor.execute(f"SELECT MIN(Order_Date), MAX(Order_Date) FROM {self.tbl}")
#         self.min_date_ob, self.max_date_ob = cursor.fetchone()
#         cursor.close()
#
#         # Create the Seasonality Tab directly in the existing notebook
#         self.seasonality_tab = ttk.Frame(self.notebook)
#         self.notebook.add(self.seasonality_tab, text="Seasonality")
#
#         # Create a notebook for inbound and outbound seasonality within the seasonality tab
#         self.sub_notebook = ttk.Notebook(self.seasonality_tab)
#         self.sub_notebook.pack(fill=tk.BOTH, expand=True)
#
#         # Create tabs for Inbound and Outbound
#         self.inbound_tab = ttk.Frame(self.sub_notebook)
#         self.outbound_tab = ttk.Frame(self.sub_notebook)
#
#         self.sub_notebook.add(self.inbound_tab, text="Inbound")
#         self.sub_notebook.add(self.outbound_tab, text="Outbound")
#
#         # Create widgets for Inbound and Outbound tabs
#         self.create_inbound_widgets()
#         self.create_outbound_widgets()

import pymysql
import tkinter as tk
from tkinter import ttk
# from global_variables import unique_dc_names_ib, min_date_ib, max_date_ib,unique_dc_names_ob,unique_business_units,unique_order_types,min_date_ob,max_date_ob


class SeasonalityApp:
    def __init__(self, notebook):
        self.tbl = f"client_data.{Shared.project}"
        self.inbound_datatbl = f"client_data.{Shared.project_inbound}"
        self.notebook = notebook

        # Connect to the database
        self.connection = self.connect_to_db()

        # Check if inbound data table exists
        if Shared.project_inbound:
            self.fetch_inbound_data()
        else:
            # print("Inbound table is missing. Skipping inbound data processing.")
            self.unique_dc_names_ib = []  # Set to empty or None to handle absence
            self.min_date_ib = self.max_date_ib = None

        # Check if outbound data table exists
        if Shared.project:
            self.fetch_outbound_data()
        else:
            print("Outbound table is missing. Skipping outbound data processing.")
            self.unique_dc_names_ob = []
            self.unique_business_units = []
            self.unique_order_types = []
            self.min_date_ob = self.max_date_ob = None

        # Create the Seasonality Tab
        self.create_seasonality_tab()

    def connect_to_db(self):
        """Connect to the MySQL database."""
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
            return connection
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None


    # def fetch_inbound_data(self):
    #     """Fetch data from the inbound table if it exists."""
    #     try:
    #         cursor = self.connection.cursor()
    #         cursor.execute(f"SELECT DISTINCT Destination_DC FROM {self.inbound_datatbl}")
    #         self.unique_dc_names_ib = ["ALL"] + [row[0] for row in cursor.fetchall()]
    #
    #         cursor.execute(f"SELECT MIN(Received_Date), MAX(Received_Date) FROM {self.inbound_datatbl}")
    #         self.min_date_ib, self.max_date_ib = cursor.fetchone()
    #         cursor.close()
    #     except Exception as e:
    #         print(f"Error fetching inbound data: {e}")
    #         self.unique_dc_names_ib = []  # Set to empty list or None
    #         self.min_date_ib = self.max_date_ib = None

    # Global variables
    min_date_sql_inbound = None
    max_date_sql_inbound = None

    def fetch_min_max_dates_inbound(self):
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

    def fetch_inbound_data(self):
        """Fetch data from the inbound table if it exists."""
        try:
            global min_date_sql_inbound, max_date_sql_inbound
            # Call the imported function to get distinct DC names
            self.unique_dc_names_ib = Shared.dc_names #get_distinct_dc_names()

            # Check if the result is valid and has DC names
            if not self.unique_dc_names_ib:
                self.unique_dc_names_ib = ["ALL"]  # Default value if no DC names are found

            # # Fetch min and max dates from the inbound table (if needed)
            # # self.min_date_ib, self.max_date_ib = fetch_min_max_dates_inbound()
            #
            # # Fetch min and max dates from the inbound table (using global variables)
            self.fetch_min_max_dates_inbound()  # This updates the global variables min_date_sql_inbound, max_date_sql_inbound
            # Fetch min and max dates from the inbound table


            # Access the global variables directly
            if min_date_sql_inbound and max_date_sql_inbound:
                self.min_date_ib = min_date_sql_inbound
                self.max_date_ib = max_date_sql_inbound
            else:
                self.min_date_ib, self.max_date_ib = None, None  # Set to None if no valid dates were fetched

        except Exception as e:
            print(f"Error fetching inbound data: {e}")
            self.unique_dc_names_ib = []  # Set to empty list or None
            self.min_date_ib = self.max_date_ib = None




    # def fetch_outbound_data(self):
    #     """Fetch data from the outbound table if it exists."""
    #     try:
    #         cursor = self.connection.cursor()
    #         cursor.execute(f"SELECT DISTINCT DC_Name FROM {self.tbl}")
    #         self.unique_dc_names_ob = ["ALL"] + [row[0] for row in cursor.fetchall()]
    #
    #         cursor.execute(f"SELECT DISTINCT Business_Unit FROM {self.tbl}")
    #         self.unique_business_units = ["ALL"] + [row[0] for row in cursor.fetchall()]
    #
    #         cursor.execute(f"SELECT DISTINCT Order_Type FROM {self.tbl}")
    #         self.unique_order_types = ["ALL"] + [row[0] for row in cursor.fetchall()]
    #
    #         cursor.execute(f"SELECT MIN(Order_Date), MAX(Order_Date) FROM {self.tbl}")
    #         self.min_date_ob, self.max_date_ob = cursor.fetchone()
    #         cursor.close()
    #     except Exception as e:
    #         print(f"Error fetching outbound data: {e}")
    #         self.unique_dc_names_ob = []
    #         self.unique_business_units = []
    #         self.unique_order_types = []
    #         self.min_date_ob = self.max_date_ob = None

    # Global variables
    min_date_sql_outbound = None
    max_date_sql_outbound = None

    def fetch_min_max_dates_outbound(self):
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

    def fetch_outbound_data(self):
        """Fetch data from the outbound table if it exists."""
        try:
            self.unique_dc_names_ob = Shared.dc_names_outbound #get_distinct_dc_names_outbound()

            self.unique_business_units = Shared.bu_names_outbound #get_distinct_bu_filter_outbound()

            self.unique_order_types = Shared.channel_names_outbound #get_distinct_channel_filter_outbound()

            # self.min_date_ob, self.max_date_ob = fetch_min_max_dates_outbound()
            # Fetch min and max dates from the outbound table (if needed)
            global min_date_sql_outbound, max_date_sql_outbound
            #self.fetch_min_max_dates_outbound()
            if Shared.min_date_sql_outbound and Shared.max_date_sql_outbound:
                self.min_date_ob = Shared.min_date_sql_outbound
                self.max_date_ob = Shared.max_date_sql_outbound
            else:
                self.min_date_ob,self.max_date_ob = None , None


        except Exception as e:
            print(f"Error fetching outbound data: {e}")
            self.unique_dc_names_ob = []
            self.unique_business_units = []
            self.unique_order_types = []
            self.min_date_ob = self.max_date_ob = None

    def create_seasonality_tab(self):
        """Create the Seasonality Tab and handle missing data gracefully."""
        self.seasonality_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.seasonality_tab, text="Seasonality")

        # Create a notebook for inbound and outbound seasonality within the seasonality tab
        self.sub_notebook = ttk.Notebook(self.seasonality_tab)
        self.sub_notebook.pack(fill=tk.BOTH, expand=True)

        # Create tabs for Inbound and Outbound
        self.inbound_tab = ttk.Frame(self.sub_notebook)
        self.outbound_tab = ttk.Frame(self.sub_notebook)

        self.sub_notebook.add(self.inbound_tab, text="Inbound")
        self.sub_notebook.add(self.outbound_tab, text="Outbound")

        # Create widgets for Inbound and Outbound tabs
        if self.unique_dc_names_ib:  # Only create widgets if inbound data exists
            self.create_inbound_widgets()
        else:
            self.disable_inbound_widgets()

        if self.unique_dc_names_ob:  # Only create widgets if outbound data exists
            self.create_outbound_widgets()
        else:
            self.disable_outbound_widgets()



    def disable_inbound_widgets(self):
        """Disable widgets related to the Inbound tab."""
        # print("Inbound data is missing, disabling inbound widgets...")
        # You can either hide the inbound tab or disable the widgets for the inbound tab
        self.sub_notebook.tab(self.inbound_tab, state="disabled")

    def disable_outbound_widgets(self):
        """Disable widgets related to the Outbound tab."""
        # print("Outbound data is missing, disabling outbound widgets...")
        # You can either hide the outbound tab or disable the widgets for the outbound tab
        self.sub_notebook.tab(self.outbound_tab, state="disabled")






# class SeasonalityApp:
#     def __init__(self, notebook):
#         self.tbl = f"client_data.{Shared.project}"
#         print(self.tbl)
#         self.inbound_datatbl = f"client_data.{Shared.project_inbound}"
#         print(self.inbound_datatbl)
#         self.notebook = notebook
#         self.connection = self.connect_to_db()
#         self.table_exists_inbound = self.table_exists(self.inbound_datatbl)
#         self.table_exists_outbound = self.table_exists(self.tbl)
#
#         # Handle inbound data table existence
#         if self.table_exists_inbound:
#             self.fetch_inbound_data()
#         else:
#             print(f"Table {self.inbound_datatbl} not found. Skipping inbound-related processing.")
#
#         # Handle outbound data table existence
#         if self.table_exists_outbound:
#             self.fetch_outbound_data()
#         else:
#             print(f"Table {self.tbl} not found. Skipping outbound-related processing.")
#
#         # Create the Seasonality Tab
#         self.create_seasonality_tab()
#
#     def connect_to_db(self):
#         try:
#             connection = pymysql.connect(
#                 host='10.216.252.8',
#                 user=Shared.userid,
#                 passwd=Shared.password,
#                 db='client_data',
#                 port=3306,
#                 ssl_ca=sslca,
#                 ssl_key=sslkey,
#                 ssl_cert=sslcert
#             )
#             return connection
#         except Exception as e:
#             print(f"Error connecting to database: {e}")
#             return None
#
#     def table_exists(self, table_name):
#         """Check if a table exists in the database."""
#         try:
#             cursor = self.connection.cursor()
#             cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
#             result = cursor.fetchone()
#             cursor.close()
#             return result is not None
#         except Exception as e:
#             print(f"Error checking table existence: {e}")
#             return False
#
#     def fetch_inbound_data(self):
#         """Fetch data for the inbound table if it exists."""
#         cursor = self.connection.cursor()
#         cursor.execute(f"SELECT DISTINCT Destination_DC FROM {self.inbound_datatbl}")
#         self.unique_dc_names_ib = ["ALL"] + [row[0] for row in cursor.fetchall()]
#
#         cursor.execute(f"SELECT MIN(Received_Date), MAX(Received_Date) FROM {self.inbound_datatbl}")
#         self.min_date_ib, self.max_date_ib = cursor.fetchone()
#         cursor.close()
#
#     def fetch_outbound_data(self):
#         """Fetch data for the outbound table if it exists."""
#         cursor = self.connection.cursor()
#         cursor.execute(f"SELECT DISTINCT DC_Name FROM {self.tbl}")
#         self.unique_dc_names_ob = ["ALL"] + [row[0] for row in cursor.fetchall()]
#
#         cursor.execute(f"SELECT DISTINCT Business_Unit FROM {self.tbl}")
#         self.unique_business_units = ["ALL"] + [row[0] for row in cursor.fetchall()]
#
#         cursor.execute(f"SELECT DISTINCT Order_Type FROM {self.tbl}")
#         self.unique_order_types = ["ALL"] + [row[0] for row in cursor.fetchall()]
#
#         cursor.execute(f"SELECT MIN(Order_Date), MAX(Order_Date) FROM {self.tbl}")
#         self.min_date_ob, self.max_date_ob = cursor.fetchone()
#         cursor.close()
#
#     def create_seasonality_tab(self):
#         """Create the Seasonality Tab and disable features based on missing tables."""
#         self.seasonality_tab = ttk.Frame(self.notebook)
#         self.notebook.add(self.seasonality_tab, text="Seasonality")
#
#         self.sub_notebook = ttk.Notebook(self.seasonality_tab)
#         self.sub_notebook.pack(fill=tk.BOTH, expand=True)
#
#         self.inbound_tab = ttk.Frame(self.sub_notebook)
#         self.outbound_tab = ttk.Frame(self.sub_notebook)
#
#         self.sub_notebook.add(self.inbound_tab, text="Inbound")
#         self.sub_notebook.add(self.outbound_tab, text="Outbound")
#
#         # Create widgets only if the corresponding table exists
#         if self.table_exists_inbound:
#             self.create_inbound_widgets()
#         else:
#             self.disable_inbound_widgets()
#
#         if self.table_exists_outbound:
#             self.create_outbound_widgets()
#         else:
#             self.disable_outbound_widgets()
#
#     def create_inbound_widgets(self):
#         """Create widgets for the Inbound tab."""
#         print("Creating Inbound widgets...")  # Example logic to add widgets
#         # Add widgets related to Inbound here, e.g. dropdowns, datepickers, etc.
#
#     def create_outbound_widgets(self):
#         """Create widgets for the Outbound tab."""
#         print("Creating Outbound widgets...")  # Example logic to add widgets
#         # Add widgets related to Outbound here, e.g. dropdowns, datepickers, etc.
#
#     def disable_inbound_widgets(self):
#         """Disable widgets related to the Inbound tab."""
#         print("Inbound data is missing, disabling inbound widgets...")
#         # Disable the widgets related to the inbound tab, e.g. by using `widget.config(state=tk.DISABLED)`
#         # Or you can hide the Inbound tab itself
#         self.sub_notebook.tab(self.inbound_tab, state="disabled")
#
#     def disable_outbound_widgets(self):
#         """Disable widgets related to the Outbound tab."""
#         print("Outbound data is missing, disabling outbound widgets...")
#         # Disable the widgets related to the outbound tab, e.g. by using `widget.config(state=tk.DISABLED)`
#         # Or you can hide the Outbound tab itself
#         self.sub_notebook.tab(self.outbound_tab, state="disabled")

    def create_inbound_widgets(self):
        # Create a frame for Inbound filters
        self.inbound_filter_frame = Frame(self.inbound_tab)
        self.inbound_filter_frame.pack(pady=10)

        # DC_NAME selection for Inbound
        self.inbound_dc_label = tk.Label(self.inbound_filter_frame, text="DC Name:")
        self.inbound_dc_label.grid(row=0, column=0, padx=5, pady=5)

        self.inbound_dc_var = tk.StringVar()
        # print(self.inbound_dc_var)
        self.inbound_dc_dropdown = ttk.Combobox(self.inbound_filter_frame, textvariable=self.inbound_dc_var,
                                                values=self.unique_dc_names_ib)
        self.inbound_dc_dropdown.grid(row=1, column=0, padx=5, pady=5)

        # Profile Metric selection for Inbound
        self.inbound_metric_label = tk.Label(self.inbound_filter_frame, text="Profile Metric:")
        self.inbound_metric_label.grid(row=0, column=1, padx=5, pady=5)

        self.inbound_metric_var = tk.StringVar()
        self.inbound_metric_dropdown = ttk.Combobox(self.inbound_filter_frame, textvariable=self.inbound_metric_var,
                                                    values=["Units", "Orders", "Lines", "SKUs"])
        self.inbound_metric_dropdown.grid(row=1, column=1, padx=5, pady=5)

        # Pick UOM selection for Inbound
        self.inbound_uom_label = tk.Label(self.inbound_filter_frame, text="Pick UOM:")
        self.inbound_uom_label.grid(row=0, column=2, padx=5, pady=5)

        self.inbound_uom_var = tk.StringVar()
        self.inbound_uom_dropdown = ttk.Combobox(self.inbound_filter_frame, textvariable=self.inbound_uom_var,
                                                 values=["Case", "Pallet", "Inner", "Each", "Layer", "All"])
        self.inbound_uom_dropdown.grid(row=1, column=2, padx=5, pady=5)

        # Date filters - move these inside the inbound_filter_frame
        self.from_label = tk.Label(self.inbound_filter_frame, text="From Date:")
        self.from_label.grid(row=0, column=3, padx=5, pady=5)
        self.from_date_ib = DateEntry(self.inbound_filter_frame, width=12, background='darkblue', foreground='white',
                                   borderwidth=2)
        self.from_date_ib.set_date(self.min_date_ib)
        self.from_date_ib.grid(row=1, column=3, padx=5, pady=5)
        self.from_date_ib.bind("<FocusOut>", lambda e: self.analyze_inbound())

        self.to_label = tk.Label(self.inbound_filter_frame, text="To Date:")
        self.to_label.grid(row=0, column=4, padx=5, pady=5)
        self.to_date_ib = DateEntry(self.inbound_filter_frame, width=12, background='darkblue', foreground='white',
                                 borderwidth=2)
        self.to_date_ib.set_date(self.max_date_ib)
        self.to_date_ib.grid(row=1, column=4, padx=5, pady=5)
        self.to_date_ib.bind("<FocusOut>", lambda e: self.analyze_inbound())

        # Frequency selection for Inbound
        self.frequency_label = tk.Label(self.inbound_filter_frame, text="Frequency:")
        self.frequency_label.grid(row=0, column=5, padx=5, pady=5)

        self.frequency_var_ib = tk.StringVar()
        self.frequency_dropdown = ttk.Combobox(self.inbound_filter_frame, textvariable=self.frequency_var_ib,
                                               values=["Daily", "Weekly", "Monthly"])
        self.frequency_dropdown.grid(row=1, column=5, padx=5, pady=5)

        # Analyze Button for Inbound
        self.inbound_analyze_button = tk.Button(self.inbound_filter_frame, text="Analyze", command=self.analyze_inbound)
        self.inbound_analyze_button.grid(row=2, column=2, pady=10,sticky='ew')

        # Export Button
        self.export_button = tk.Button(self.inbound_filter_frame, text="Export to CSV", command=self.export_to_csv)
        self.export_button.grid(row=2, column=3, padx=5, pady=10, sticky='ew')  # Moved inside the frame and to a new row

        # Create a frame for the graphs with a scrollbar for Inbound
        self.inbound_graph_frame = Frame(self.inbound_tab)
        self.inbound_graph_frame.pack(fill=tk.BOTH, expand=True)


        self.inbound_canvas = tk.Canvas(self.inbound_graph_frame)
        self.inbound_scroll_y = Scrollbar(self.inbound_graph_frame, orient="vertical",
                                          command=self.inbound_canvas.yview)
        self.inbound_scrollable_frame = Frame(self.inbound_canvas)

        self.inbound_scrollable_frame.bind(
            "<Configure>",
            lambda e: self.inbound_canvas.configure(scrollregion=self.inbound_canvas.bbox("all"))
        )

        self.inbound_canvas.create_window((0, 0), window=self.inbound_scrollable_frame, anchor="nw")

        self.inbound_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.inbound_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        # Initialize figures for Inbound frequencies
        self.inbound_daily_fig = plt.Figure(figsize=(13, 7), dpi=100)
        self.inbound_weekly_fig = plt.Figure(figsize=(13, 7), dpi=100)
        self.inbound_monthly_fig = plt.Figure(figsize=(13, 7), dpi=100)

        self.inbound_daily_canvas = FigureCanvasTkAgg(self.inbound_daily_fig, self.inbound_scrollable_frame)
        self.inbound_weekly_canvas = FigureCanvasTkAgg(self.inbound_weekly_fig, self.inbound_scrollable_frame)
        self.inbound_monthly_canvas = FigureCanvasTkAgg(self.inbound_monthly_fig, self.inbound_scrollable_frame)

        # Layout the canvas for Inbound
        self.inbound_daily_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.inbound_weekly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.inbound_monthly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        # Initialize figures for each UOM
        self.daily_fig_UOM = plt.Figure(figsize=(10, 5), dpi=100)
        self.weekly_fig_UOM = plt.Figure(figsize=(10, 5), dpi=100)
        self.monthly_fig_UOM = plt.Figure(figsize=(10, 5), dpi=100)

        self.daily_canvas_UOM = FigureCanvasTkAgg(self.daily_fig_UOM, self.inbound_scrollable_frame)
        self.weekly_canvas_UOM = FigureCanvasTkAgg(self.weekly_fig_UOM, self.inbound_scrollable_frame)
        self.monthly_canvas_UOM = FigureCanvasTkAgg(self.monthly_fig_UOM, self.inbound_scrollable_frame)

        # Layout the canvas
        self.daily_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.weekly_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.monthly_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

    def create_outbound_widgets(self):
        # Create a frame for Outbound filters
        self.outbound_filter_frame = Frame(self.outbound_tab)
        self.outbound_filter_frame.pack(pady=10)

        # DC_NAME selection for Outbound
        self.outbound_dc_label = tk.Label(self.outbound_filter_frame, text="DC Name:")
        self.outbound_dc_label.grid(row=0, column=0, padx=5, pady=5)

        self.outbound_dc_var = tk.StringVar()
        self.outbound_dc_dropdown = ttk.Combobox(self.outbound_filter_frame, textvariable=self.outbound_dc_var,
                                                 values=self.unique_dc_names_ob)
        self.outbound_dc_dropdown.grid(row=1, column=0, padx=5, pady=5)

        # BUSINESS UNIT selection
        self.bu_label = tk.Label(self.outbound_filter_frame, text="Business Unit:")
        self.bu_label.grid(row=0, column=1, padx=5, pady=5)

        self.bu_var = tk.StringVar()
        self.bu_dropdown = ttk.Combobox(self.outbound_filter_frame, textvariable=self.bu_var,
                                        values=self.unique_business_units)
        self.bu_dropdown.grid(row=1, column=1, padx=5, pady=5)

        # ORDERTYPE selection
        self.ot_label = tk.Label(self.outbound_filter_frame, text="Order Type:")
        self.ot_label.grid(row=0, column=2, padx=5, pady=5)

        self.ot_var = tk.StringVar()
        self.ot_dropdown = ttk.Combobox(self.outbound_filter_frame, textvariable=self.ot_var,
                                        values=self.unique_order_types)
        self.ot_dropdown.grid(row=1, column=2, padx=5, pady=5)

        # Profile Metric selection for Outbound
        self.outbound_metric_label = tk.Label(self.outbound_filter_frame, text="Profile Metric:")
        self.outbound_metric_label.grid(row=0, column=3, padx=5, pady=5)

        self.outbound_metric_var = tk.StringVar()
        self.outbound_metric_dropdown = ttk.Combobox(self.outbound_filter_frame, textvariable=self.outbound_metric_var,
                                                     values=["Units", "Orders", "Lines", "SKUs"])
        self.outbound_metric_dropdown.grid(row=1, column=3, padx=5, pady=5)

        # Pick UOM selection for Outbound
        self.outbound_uom_label = tk.Label(self.outbound_filter_frame, text="Pick UOM:")
        self.outbound_uom_label.grid(row=0, column=4, padx=5, pady=5)

        self.outbound_uom_var = tk.StringVar()
        self.outbound_uom_dropdown = ttk.Combobox(self.outbound_filter_frame, textvariable=self.outbound_uom_var,
                                                  values=["Case", "Pallet", "Inner", "Each", "Layer", "All"])
        self.outbound_uom_dropdown.grid(row=1, column=4, padx=5, pady=5)

        # Date filters - move these into the outbound_filter_frame
        self.from_label = tk.Label(self.outbound_filter_frame, text="From Date:")
        self.from_label.grid(row=0, column=5, padx=5, pady=5)
        self.from_date_ob = DateEntry(self.outbound_filter_frame, width=12, background='darkblue', foreground='white',
                                   borderwidth=2)
        self.from_date_ob.set_date(self.min_date_ob)
        self.from_date_ob.grid(row=1, column=5, padx=5, pady=5)
        self.from_date_ob.bind("<FocusOut>", lambda e: self.analyze_outbound())

        self.to_label = tk.Label(self.outbound_filter_frame, text="To Date:")
        self.to_label.grid(row=0, column=6, padx=5, pady=5)
        self.to_date_ob = DateEntry(self.outbound_filter_frame, width=12, background='darkblue', foreground='white',
                                 borderwidth=2)
        self.to_date_ob.set_date(self.max_date_ob)
        self.to_date_ob.grid(row=1, column=6, padx=5, pady=5)
        self.to_date_ob.bind("<FocusOut>", lambda e: self.analyze_outbound())

        # Frequency selection for Outbound
        self.frequency_label = tk.Label(self.outbound_filter_frame, text="Frequency:")
        self.frequency_label.grid(row=0, column=7, padx=5, pady=5)

        self.frequency_var_ob = tk.StringVar()
        self.frequency_dropdown = ttk.Combobox(self.outbound_filter_frame, textvariable=self.frequency_var_ob,
                                               values=["Daily", "Weekly", "Monthly"])
        self.frequency_dropdown.grid(row=1, column=7, padx=5, pady=5)

        # Analyze Button for Outbound
        self.outbound_analyze_button = tk.Button(self.outbound_filter_frame, text="Analyze", command=self.analyze_outbound)
        self.outbound_analyze_button.grid(row=2, column=3, pady=10, sticky='ew')

        # Export Button
        self.export_button_outbound = tk.Button(self.outbound_filter_frame, text="Export to CSV", command=self.export_to_csv_outbound)
        self.export_button_outbound.grid(row=2, column=4, padx=5, pady=10,sticky='ew')  # Moved inside the frame and to a new row

        # Create a frame for the graphs with a scrollbar for Outbound
        self.outbound_graph_frame = Frame(self.outbound_tab)
        self.outbound_graph_frame.pack(fill=tk.BOTH, expand=True)

        self.outbound_canvas = tk.Canvas(self.outbound_graph_frame)
        self.outbound_scroll_y = Scrollbar(self.outbound_graph_frame, orient="vertical",
                                           command=self.outbound_canvas.yview)
        self.outbound_scrollable_frame = Frame(self.outbound_canvas)

        self.outbound_scrollable_frame.bind(
            "<Configure>",
            lambda e: self.outbound_canvas.configure(scrollregion=self.outbound_canvas.bbox("all"))
        )

        self.outbound_canvas.create_window((0, 0), window=self.outbound_scrollable_frame, anchor="nw")

        self.outbound_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.outbound_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        # Initialize figures for Outbound frequencies
        self.outbound_daily_fig = plt.Figure(figsize=(13, 7), dpi=100)
        self.outbound_weekly_fig = plt.Figure(figsize=(13, 7), dpi=100)
        self.outbound_monthly_fig = plt.Figure(figsize=(13, 7), dpi=100)

        self.outbound_daily_canvas = FigureCanvasTkAgg(self.outbound_daily_fig, self.outbound_scrollable_frame)
        self.outbound_weekly_canvas = FigureCanvasTkAgg(self.outbound_weekly_fig, self.outbound_scrollable_frame)
        self.outbound_monthly_canvas = FigureCanvasTkAgg(self.outbound_monthly_fig, self.outbound_scrollable_frame)

        # Layout the canvas for Outbound
        self.outbound_daily_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.outbound_weekly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.outbound_monthly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        # Initialize figures for Outbound UOM
        self.outbound_daily_fig_UOM = plt.Figure(figsize=(13, 7), dpi=100)
        self.outbound_weekly_fig_UOM = plt.Figure(figsize=(13, 7), dpi=100)
        self.outbound_monthly_fig_UOM = plt.Figure(figsize=(13, 7), dpi=100)

        self.outbound_daily_canvas_UOM = FigureCanvasTkAgg(self.outbound_daily_fig_UOM, self.outbound_scrollable_frame)
        self.outbound_weekly_canvas_UOM = FigureCanvasTkAgg(self.outbound_weekly_fig_UOM,
                                                            self.outbound_scrollable_frame)
        self.outbound_monthly_canvas_UOM = FigureCanvasTkAgg(self.outbound_monthly_fig_UOM,
                                                             self.outbound_scrollable_frame)

        # Layout the canvas for Outbound UOM
        self.outbound_daily_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.outbound_weekly_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.outbound_monthly_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

    def analyze_inbound(self):
        # self.inbound_datatbl = f"client_data.{Shared.project_inbound}"
        # self.tbl = tbl
        # self.inbound_datatbl = inbound_datatbl
        # print(f"Testing Inbound Table Name: {Shared.project_inbound}")
        self.tbl = f"client_data.{Shared.project}"
        self.inbound_datatbl = f"client_data.{Shared.project_inbound}"
        selected_dc_name = self.inbound_dc_var.get()
        # print(selected_dc_name)
        selected_profile_metric = self.inbound_metric_var.get()
        selected_pick_uom = self.inbound_uom_var.get()
        # Get the selected dates from the date entry fields
        from_date = self.from_date_ib.get_date()
        to_date = self.to_date_ib.get_date()
        selected_frequency_ib = self.frequency_var_ib.get()  # Get selected frequency

        # List to keep track of missing filters
        missing_filters = []

        # Check for missing filters and add them to the list
        if not selected_dc_name:
            missing_filters.append("DC Name")
        if not selected_profile_metric:
            missing_filters.append("Profile Metric")
        if not selected_pick_uom:
            missing_filters.append("Pick UOM")
        if not selected_frequency_ib:
            missing_filters.append("Frequency")

        # If any required filters are missing, show a message
        if missing_filters:
            missing_filters_msg = "Please select the following filters: " + ", ".join(missing_filters)
            messagebox.showwarning("Missing Filters", missing_filters_msg)
            return

        # Prepare the SQL query
        if selected_dc_name == "ALL":
            query = f"""
                       SELECT
                           Received_Date,
                           SUM(Case_Units) AS Units_Case,
                           SUM(Pallet_Units) AS Units_Pallet,
                           SUM(Each_Picks) AS Units_Each,
                           SUM(Inner_Units) AS Units_Inner, 
                           SUM(Layer_Units) AS Units_Layer, 
                           COUNT(DISTINCT CASE WHEN Case_Picks > 0 THEN PO_Number END) AS Orders_Case,
                           COUNT(CASE WHEN Case_Picks > 0 THEN PO_Number END) AS Lines_Case,
                           COUNT(DISTINCT CASE WHEN Case_Picks > 0 THEN SKU END) AS SKUs_Case,
                           COUNT(DISTINCT CASE WHEN Pallet_Picks > 0 THEN PO_Number END) AS Orders_Pallet,
                           COUNT(CASE WHEN Pallet_Picks > 0 THEN PO_Number END) AS Lines_Pallet,
                           COUNT(DISTINCT CASE WHEN Pallet_Picks > 0 THEN SKU END) AS SKUs_Pallet,
                           COUNT(DISTINCT CASE WHEN Qty > 0 THEN PO_Number END) AS Orders_Each,
                           COUNT(CASE WHEN Qty > 0 THEN PO_Number END) AS Lines_Each,
                           COUNT(DISTINCT CASE WHEN Qty > 0 THEN SKU END) AS SKUs_Each,
                           COUNT(DISTINCT CASE WHEN Inner_Picks > 0 THEN PO_Number END) AS Orders_Inner,
                           COUNT(CASE WHEN Inner_Picks > 0 THEN PO_Number END) AS Lines_Inner,
                           COUNT(DISTINCT CASE WHEN Inner_Picks > 0 THEN SKU END) AS SKUs_Inner,
                           COUNT(DISTINCT CASE WHEN Layer_Picks > 0 THEN PO_Number END) AS Orders_Layer,
                           COUNT(CASE WHEN Layer_Picks > 0 THEN PO_Number END) AS Lines_Layer,
                           COUNT(DISTINCT CASE WHEN Layer_Picks > 0 THEN SKU END) AS SKUs_Layer,
                           SUM(Pallet_Picks) AS Pallet_Picks,
                           SUM(Layer_Picks) AS Layer_Picks,
                           SUM(Case_Picks) AS Case_Picks,
                           SUM(Inner_Picks) AS Inner_Picks,
                           SUM(Each_Picks) AS Each_Picks
                       FROM
                           {self.inbound_datatbl}
                       WHERE
                           Received_Date BETWEEN %s AND %s
                       GROUP BY
                           Received_Date
                       ORDER BY
                           Received_Date;
                       """
            params = (from_date, to_date)
        else:
            query = f"""
                       SELECT
                           Received_Date,
                           SUM(Case_Units) AS Units_Case,
                           SUM(Pallet_Units) AS Units_Pallet,
                           SUM(Each_Picks) AS Units_Each,
                           SUM(Inner_Units) AS Units_Inner, 
                           SUM(Layer_Units) AS Units_Layer, 
                           COUNT(DISTINCT CASE WHEN Case_Picks > 0 THEN PO_Number END) AS Orders_Case,
                           COUNT(CASE WHEN Case_Picks > 0 THEN PO_Number END) AS Lines_Case,
                           COUNT(DISTINCT CASE WHEN Case_Picks > 0 THEN SKU END) AS SKUs_Case,
                           COUNT(DISTINCT CASE WHEN Pallet_Picks > 0 THEN PO_Number END) AS Orders_Pallet,
                           COUNT(CASE WHEN Pallet_Picks > 0 THEN PO_Number END) AS Lines_Pallet,
                           COUNT(DISTINCT CASE WHEN Pallet_Picks > 0 THEN SKU END) AS SKUs_Pallet,
                           COUNT(DISTINCT CASE WHEN Qty > 0 THEN PO_Number END) AS Orders_Each,
                           COUNT(CASE WHEN Qty > 0 THEN PO_Number END) AS Lines_Each,
                           COUNT(DISTINCT CASE WHEN Qty > 0 THEN SKU END) AS SKUs_Each,
                           COUNT(DISTINCT CASE WHEN Inner_Picks > 0 THEN PO_Number END) AS Orders_Inner,
                           COUNT(CASE WHEN Inner_Picks > 0 THEN PO_Number END) AS Lines_Inner,
                           COUNT(DISTINCT CASE WHEN Inner_Picks > 0 THEN SKU END) AS SKUs_Inner,
                           COUNT(DISTINCT CASE WHEN Layer_Picks > 0 THEN PO_Number END) AS Orders_Layer,
                           COUNT(CASE WHEN Layer_Picks > 0 THEN PO_Number END) AS Lines_Layer,
                           COUNT(DISTINCT CASE WHEN Layer_Picks > 0 THEN SKU END) AS SKUs_Layer,
                           SUM(Pallet_Picks) AS Pallet_Picks,
                           SUM(Layer_Picks) AS Layer_Picks,
                           SUM(Case_Picks) AS Case_Picks,
                           SUM(Inner_Picks) AS Inner_Picks,
                           SUM(Each_Picks) AS Each_Picks
                       FROM
                           {self.inbound_datatbl}
                       WHERE
                           Destination_DC = %s AND
                           Received_Date BETWEEN %s AND %s
                       GROUP BY
                           Received_Date, Destination_DC
                       ORDER BY
                           Received_Date;
                       """

            params = (selected_dc_name, from_date, to_date)

        try:
            with self.connection.cursor() as cursor:

                cursor.execute(query, params)
                myresult = cursor.fetchall()
                # print(f"Query: {query}")
                # print(f"Params: {params}")

            if not myresult:
                # print(f"Params: {params}")
                # print({self.connection})
                # print(f"no of file: {Shared.project_inbound}")
                # print(myresult)
                messagebox.showwarning("No Data", "No data found for the selected options.")
                return

            # Create DataFrame
            columns = [col[0] for col in cursor.description]
            INBOUND = pd.DataFrame(myresult, columns=columns)
            INBOUND['Received_Date'] = pd.to_datetime(INBOUND['Received_Date'])

            # Store data for plotting
            self.INBOUND = INBOUND
            # print(self.INBOUND)
            self.selected_profile_metric = selected_profile_metric
            self.selected_pick_uom = selected_pick_uom

            # Plot only for the selected frequency
            self.plot_inbound_graph(INBOUND, selected_frequency_ib, selected_profile_metric, selected_pick_uom)
            self.plot_inbound_uom_graph()  # New line to plot UOM graph


        except pymysql.MySQLError as e:
            messagebox.showerror("Database Error", f"An error occurred: {e}")
        except Exception as e:
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")

    def analyze_outbound(self):
        selected_dc_name = self.outbound_dc_var.get()
        selected_order_type=self.ot_var.get()
        selected_business_unit=self.bu_var.get()
        selected_profile_metric = self.outbound_metric_var.get()
        selected_pick_uom = self.outbound_uom_var.get()
        selected_frequency_ob = self.frequency_var_ob.get()  # Get selected frequency

        # Get the selected dates from the date entry fields
        from_date = self.from_date_ob.get_date()
        to_date = self.to_date_ob.get_date()

        # List to keep track of missing filters
        missing_filters = []

        # Check for missing filters and add them to the list
        if not selected_order_type:
            missing_filters.append("Order Type")
        if not selected_business_unit:
            missing_filters.append("Business Unit")
        if not selected_dc_name:
            missing_filters.append("DC Name")
        if not selected_profile_metric:
            missing_filters.append("Profile Metric")
        if not selected_pick_uom:
            missing_filters.append("Pick UOM")
        if not selected_frequency_ob:
            missing_filters.append("Frequency")

        # If any required filters are missing, show a message
        if missing_filters:
            missing_filters_msg = "Please select the following filters: " + ", ".join(missing_filters)
            messagebox.showwarning("Missing Filters", missing_filters_msg)
            return

        # Create base query
        query = f"""
                  SELECT 
                      Order_Date,
                      SUM(CASE WHEN Case_Picks > 0 THEN Case_Units ELSE 0 END) AS TotalCaseUnits,
                      COUNT(DISTINCT CASE WHEN Case_Picks > 0 THEN Order_Number END) AS TotalCaseOrders,
                      COUNT(CASE WHEN Case_Picks > 0 THEN Order_Number END) AS TotalCaseLines,
                      COUNT(DISTINCT CASE WHEN Case_Picks > 0 THEN SKU END) AS TotalCaseSKUs,
                      SUM(CASE WHEN Pallet_Picks > 0 THEN Pallet_Units ELSE 0 END) AS TotalPalletUnits,
                      COUNT(DISTINCT CASE WHEN Pallet_Picks > 0 THEN Order_Number END) AS TotalPalletOrders,
                      COUNT(CASE WHEN Pallet_Picks > 0 THEN Order_Number END) AS TotalPalletLines,
                      COUNT(DISTINCT CASE WHEN Pallet_Picks > 0 THEN SKU END) AS TotalPalletSKUs,
                      SUM(CASE WHEN Inner_Picks > 0 THEN Inner_Units ELSE 0 END) AS TotalInnerUnits,
                      COUNT(DISTINCT CASE WHEN Inner_Picks > 0 THEN Order_Number END) AS TotalInnerOrders,
                      COUNT(CASE WHEN Inner_Picks > 0 THEN Order_Number END) AS TotalInnerLines,
                      COUNT(DISTINCT CASE WHEN Inner_Picks > 0 THEN SKU END) AS TotalInnerSKUs,
                      SUM(CASE WHEN Each_Picks > 0 THEN Each_Picks ELSE 0 END) AS TotalEachUnits,
                      COUNT(DISTINCT CASE WHEN Each_Picks > 0 THEN ORDER_NUMBER END) AS TotalEachOrders,
                      COUNT(CASE WHEN Each_Picks > 0 THEN Order_Number END) AS TotalEachLines,
                      COUNT(DISTINCT CASE WHEN Each_Picks > 0 THEN SKU END) AS TotalEachSKUs,
                      SUM(CASE WHEN Layer_Picks > 0 THEN Layer_Units ELSE 0 END) AS TotalLayerUnits,
                      COUNT(DISTINCT CASE WHEN Layer_Picks > 0 THEN Order_Number END) AS TotalLayerOrders,
                      COUNT(CASE WHEN Layer_Picks > 0 THEN Order_Number END) AS TotalLayerLines,
                      COUNT(DISTINCT CASE WHEN Layer_Picks > 0 THEN SKU END) AS TotalLayerSKUs,
                       SUM(Pallet_Picks) AS Pallet_Picks,
                      SUM(Layer_Picks) AS Layer_Picks,
                      SUM(Case_Picks) AS Case_Picks,
                      SUM(Inner_Picks) AS Inner_Picks,
                      SUM(Each_Picks) AS Each_Picks
                  FROM {self.tbl}
                  WHERE 
                  (Business_Unit = %s OR %s = 'ALL') AND 
                  (Order_Type = %s OR %s = 'ALL') AND 
                  (DC_Name = %s OR %s = 'ALL') AND 
                  Order_Date BETWEEN %s AND %s
                  GROUP BY Order_Date
                  """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query,
                               (selected_business_unit, selected_business_unit,
                                selected_order_type, selected_order_type,
                                selected_dc_name, selected_dc_name,
                                from_date, to_date))
                myresult = cursor.fetchall()

            if not myresult:
                messagebox.showwarning("No Data", "No data found for the selected options.")
                return

            columns = ['Order_Date', 'TotalCaseUnits', 'TotalCaseOrders', 'TotalCaseLines', 'TotalCaseSKUs',
                       'TotalPalletUnits', 'TotalPalletOrders', 'TotalPalletLines', 'TotalPalletSKUs',
                       'TotalInnerUnits', 'TotalInnerOrders', 'TotalInnerLines', 'TotalInnerSKUs',
                       'TotalEachUnits', 'TotalEachOrders', 'TotalEachLines', 'TotalEachSKUs',
                       'TotalLayerUnits', 'TotalLayerOrders', 'TotalLayerLines', 'TotalLayerSKUs']

            # Create DataFrame
            columns = [col[0] for col in cursor.description]
            OUTBOUND = pd.DataFrame(myresult, columns=columns)
            OUTBOUND['Order_Date'] = pd.to_datetime(OUTBOUND['Order_Date'])

            # Store data for plotting
            self.OUTBOUND = OUTBOUND
            self.selected_profile_metric = selected_profile_metric
            self.selected_pick_uom = selected_pick_uom

            # Plot only for the selected frequency
            self.plot_outbound_graph(OUTBOUND, selected_frequency_ob, selected_profile_metric, selected_pick_uom,
                                     selected_dc_name)
            self.plot_outbound_uom_graph()  # New line to plot UOM graph

        except pymysql.MySQLError as e:
            messagebox.showerror("Database Error", f"An error occurred: {e}")
        except Exception as e:
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")


    def plot_inbound_graph(self, df, frequency, profile_metric, pick_uom):
        # Get the selected dates (for inbound, you can implement similar logic if you have date range selection for inbound)
        from_date = self.from_date_ib.get_date()
        to_date = self.to_date_ib.get_date()

        # Validate date range for Monthly frequency
        if frequency == 'Monthly' and (to_date - from_date).days < 30:
            messagebox.showerror("Invalid Selection",
                                 "For monthly frequency, please select a date range of at least one month.")
            return  # Exit the function to prevent further execution

        # Check for any invalid values in the 'Order_Date' column and attempt to convert to datetime
        try:
            df['Received_Date'] = pd.to_datetime(df['Received_Date'],
                                              errors='coerce')  # 'coerce' will handle invalid values by turning them to NaT
        except Exception as e:
            messagebox.showerror("Invalid Data",
                                 f"An error occurred while converting 'Received_Date' to datetime: {str(e)}")
            return

        # Check if any rows have become NaT (Not a Time) due to conversion failure
        if df['Received_Date'].isna().any():
            messagebox.showerror("Invalid Data", "The 'Received_Date' column contains invalid or missing datetime values.")
            return

        # Now set 'Order_Date' as the index
        df.set_index('Received_Date', inplace=True)
        results = self.calculate_metrics_inbound(df, profile_metric, pick_uom)

        # Clear previous graphs
        self.inbound_daily_fig.clf()
        self.inbound_weekly_fig.clf()
        self.inbound_monthly_fig.clf()

        # Hide all canvases initially
        self.inbound_daily_canvas.get_tk_widget().pack_forget()
        self.inbound_weekly_canvas.get_tk_widget().pack_forget()
        self.inbound_monthly_canvas.get_tk_widget().pack_forget()

        # Plot based on the selected frequency
        if frequency == 'Daily':
            self.create_graph_inbound(results['Daily'], self.inbound_daily_fig, frequency, profile_metric, pick_uom)
            self.inbound_daily_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
            self.inbound_daily_canvas.draw()

        elif frequency == 'Weekly':
            self.create_graph_inbound(results['Weekly'], self.inbound_weekly_fig, frequency, profile_metric, pick_uom)
            self.inbound_weekly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
            self.inbound_weekly_canvas.draw()

        elif frequency == 'Monthly':
            self.create_graph_inbound(results['Monthly'], self.inbound_monthly_fig, frequency, profile_metric, pick_uom)
            self.inbound_monthly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
            self.inbound_monthly_canvas.draw()

        # Optional: Force a layout update to minimize empty space
        self.inbound_graph_frame.update_idletasks()


    def plot_inbound_uom_graph(self):
        uom_data = self.calculate_inbound_uom_data()  # Calculate UOM data

        def format_value(value):
            """Format the value for display."""
            if value >= 1000:
                return f"{value / 1000:.1f}k"  # Convert to 'k' format
            return str(value)

        # Clear previous UOM graph
        self.daily_fig_UOM.clf()
        self.weekly_fig_UOM.clf()
        self.monthly_fig_UOM.clf()

        # Hide all UOM canvases initially
        self.daily_canvas_UOM.get_tk_widget().pack_forget()
        self.weekly_canvas_UOM.get_tk_widget().pack_forget()
        self.monthly_canvas_UOM.get_tk_widget().pack_forget()

        # Create UOM figure
        ax = self.daily_fig_UOM.add_subplot(211)  # Use daily figure for UOM

        # Plot UOM data based on frequency
        frequency = self.frequency_var_ib.get()

        if frequency == 'Daily':
            if self.selected_pick_uom == "All":
                ax.plot(uom_data.index, uom_data['Total_Picks'], color='blue', label='Total Picks', marker='None')
            else:
                ax.plot(uom_data.index, uom_data[self.selected_pick_uom + '_Picks'], color='blue', marker='None')

            ax.set_title(f'{self.selected_pick_uom} Received - Frequency: Daily')
            ax.set_xlabel('Date')
            ax.set_ylabel(f'{self.selected_pick_uom} Pick')

            # Formatting for date labels
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=10))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # Annotate only the top 10 values
            heights = uom_data['Total_Picks'] if self.selected_pick_uom == "All" else uom_data[
                self.selected_pick_uom + '_Picks']
            top_10_indices = heights.nlargest(10).index

            # Set y-axis limits
            ax.set_ylim(0, heights.max() * 1.1)  # Adding some padding above the max value

            for index in top_10_indices:
                height = heights[index]
                ax.annotate(format_value(height),
                            xy=(index, height),
                            xytext=(0, 3),  # Slightly above the bar
                            textcoords="offset points",
                            ha='center', color='blue', clip_on=True,fontsize=6)  # Ensure it's clipped

        elif frequency == 'Weekly':
            week_labels = [f'W{week + 1}' for week in range(len(uom_data))]
            if self.selected_pick_uom == "All":
                bars = ax.bar(week_labels, uom_data['Total_Picks'].values, color='blue', label='Total Picks')
            else:
                bars = ax.bar(week_labels, uom_data[self.selected_pick_uom + '_Picks'].values, color='blue')

            ax.set_title(f'{self.selected_pick_uom} Pick - Frequency: Weekly')
            ax.set_xlabel('Week')
            ax.set_ylabel(f'{self.selected_pick_uom} Pick')
            ax.tick_params(axis='x', rotation=45)

            # Set y-axis limits
            heights = [bar.get_height() for bar in bars]
            ax.set_ylim(0, max(heights) * 1.1)  # Adding some padding

            # Annotate all bars
            if bars is not None:
                for index, bar in enumerate(bars):
                    height = bar.get_height()
                    ax.annotate(format_value(height),
                                xy=(bar.get_x() + bar.get_width() / 2, height),
                                xytext=(0, 3),  # Slightly above the bar
                                textcoords="offset points",
                                ha='center', color='blue', clip_on=True, fontsize=6)


        elif frequency == 'Monthly':
            if self.selected_pick_uom == "All":
                bars = ax.bar(uom_data.index.strftime('%b %Y'), uom_data['Total_Picks'].values, color='blue',
                              label='Total Picks')
            else:
                bars = ax.bar(uom_data.index.strftime('%b %Y'), uom_data[self.selected_pick_uom + '_Picks'].values,
                              color='blue')

            ax.set_title(f'{self.selected_pick_uom} Pick - Frequency: Monthly')
            ax.set_xlabel('Month')
            ax.set_ylabel(f'{self.selected_pick_uom} Pick')
            ax.tick_params(axis='x', rotation=45)

            # Set y-axis limits
            heights = [bar.get_height() for bar in bars]
            ax.set_ylim(0, max(heights) * 1.1)  # Adding some padding

            # Annotate all bars
            if bars is not None:
                for index, bar in enumerate(bars):
                    height = bar.get_height()
                    ax.annotate(format_value(height),
                                xy=(bar.get_x() + bar.get_width() / 2, height),
                                xytext=(0, 3),  # Slightly above the bar
                                textcoords="offset points",
                                ha='center', color='blue', clip_on=True, fontsize=6)

        plt.tight_layout()

        # Pack the UOM canvas again
        self.daily_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.daily_canvas_UOM.draw()  # Update the UOM canvas

    def plot_outbound_graph(self, df, frequency, profile_metric, pick_uom, dc_name):
        # Get the selected dates
        from_date = self.from_date_ob.get_date()
        to_date = self.to_date_ob.get_date()

        # Validate date range for Monthly frequency
        if frequency == 'Monthly' and (to_date - from_date).days < 30:
            messagebox.showerror("Invalid Selection",
                                 "For monthly frequency, please select a date range of at least one month.")
            return  # Exit the function to prevent further execution

        try:
            df['Order_Date'] = pd.to_datetime(df['Order_Date'],
                                              errors='coerce')  # Use 'coerce' to handle invalid values
        except Exception as e:
            messagebox.showerror("Invalid Data",
                                 f"An error occurred while converting 'Order_Date' to datetime: {str(e)}")
            return

        # Check if any rows have become NaT (Not a Time) due to conversion failure
        if df['Order_Date'].isna().any():
            messagebox.showerror("Invalid Data", "The 'Order_Date' column contains invalid or missing datetime values.")
            return

        # Now set 'Order_Date' as the index
        df.set_index('Order_Date', inplace=True)

        # Calculate metrics
        results = self.calculate_metrics_outbound(df, profile_metric, pick_uom)

        # Clear previous graphs
        self.outbound_daily_fig.clf()
        self.outbound_weekly_fig.clf()
        self.outbound_monthly_fig.clf()

        # Hide all canvases initially
        self.outbound_daily_canvas.get_tk_widget().pack_forget()
        self.outbound_weekly_canvas.get_tk_widget().pack_forget()
        self.outbound_monthly_canvas.get_tk_widget().pack_forget()

        # Plot based on the selected frequency
        if frequency == 'Daily':
            self.create_graph_outbound(results['Daily'], frequency, profile_metric, pick_uom,
                                       dc_name)
            self.outbound_daily_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
            self.outbound_daily_canvas.draw()

        elif frequency == 'Weekly':
            self.create_graph_outbound(results['Weekly'], frequency, profile_metric, pick_uom,
                                       dc_name)
            self.outbound_weekly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
            self.outbound_weekly_canvas.draw()

        elif frequency == 'Monthly':
            self.create_graph_outbound(results['Monthly'], frequency, profile_metric,
                                       pick_uom, dc_name)
            self.outbound_monthly_canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
            self.outbound_monthly_canvas.draw()

    def plot_outbound_uom_graph(self):
        uom_data = self.calculate_outbound_uom_data()  # Calculate UOM data
        # print(uom_data.columns)  # Check the columns
        # print(uom_data.head())  # Print first few rows

        def format_value(value):
            """Format the value for display."""
            if value >= 1000:
                return f"{value / 1000:.1f}k"  # Convert to 'k' format
            return str(value)

        # Clear previous UOM graph
        self.outbound_daily_fig_UOM.clf()
        self.outbound_weekly_fig_UOM.clf()
        self.outbound_monthly_fig_UOM.clf()

        # Hide all UOM canvases initially
        self.outbound_daily_canvas_UOM.get_tk_widget().pack_forget()
        self.outbound_weekly_canvas_UOM.get_tk_widget().pack_forget()
        self.outbound_monthly_canvas_UOM.get_tk_widget().pack_forget()

        # Create UOM figure
        ax = self.outbound_daily_fig_UOM.add_subplot(211)  # Use daily figure for UOM

        # Plot UOM data based on frequency
        frequency = self.frequency_var_ob.get()  # Assuming this is for outbound frequency

        # Convert relevant columns to numeric
        uom_data['Total_Picks'] = pd.to_numeric(uom_data['Total_Picks'], errors='coerce')
        if self.selected_pick_uom != "All":
            uom_data[self.selected_pick_uom + '_Picks'] = pd.to_numeric(uom_data[self.selected_pick_uom + '_Picks'],
                                                                        errors='coerce')

        if frequency == 'Daily':
            if self.selected_pick_uom == "All":
                ax.plot(uom_data.index, uom_data['Total_Picks'], color='blue', label='Total Picks', marker=None)
            else:
                ax.plot(uom_data.index, uom_data[self.selected_pick_uom + '_Picks'], color='blue', marker=None)

            ax.set_title(f'{self.selected_pick_uom} Pick - Frequency: Daily')
            ax.set_xlabel('Date')
            ax.set_ylabel(f'{self.selected_pick_uom} Pick')

            # Formatting for date labels
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=10))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # Annotate top 10 peaks
            heights = uom_data['Total_Picks'] if self.selected_pick_uom == "All" else uom_data[
                self.selected_pick_uom + '_Picks']
            top_10_indices = heights.nlargest(10).index
            for index in top_10_indices:
                height = heights[index]
                ax.annotate(format_value(height),
                            xy=(index, height),
                            xytext=(0, 2),  # Slightly above the point
                            textcoords="offset points",
                            ha='center', color='blue', clip_on=True, fontsize=5)

        elif frequency == 'Weekly':
            week_labels = [f'W{week + 1}' for week in range(len(uom_data))]
            if self.selected_pick_uom == "All":
                bars = ax.bar(week_labels, uom_data['Total_Picks'].values, color='blue', label='Total Picks')
            else:
                bars = ax.bar(week_labels, uom_data[self.selected_pick_uom + '_Picks'].values, color='blue')

            ax.set_title(f'{self.selected_pick_uom} Pick - Frequency: Weekly')
            ax.set_xlabel('Week')
            ax.set_ylabel(f'{self.selected_pick_uom} Pick')
            ax.tick_params(axis='x', rotation=45)

            # Annotate all bars
            for index, bar in enumerate(bars):
                height = bar.get_height()
                ax.annotate(format_value(height),
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 2),  # Slightly above the bar
                            textcoords="offset points",
                            ha='center', color='blue', clip_on=True, fontsize=5)

            # Set y-axis limits
            ax.set_ylim(0, max([bar.get_height() for bar in bars]) * 1.1)  # Adding some padding


        elif frequency == 'Monthly':
            if self.selected_pick_uom == "All":
                bars = ax.bar(uom_data.index.strftime('%b %Y'), uom_data['Total_Picks'].values, color='blue',
                              label='Total Picks')
            else:
                bars = ax.bar(uom_data.index.strftime('%b %Y'), uom_data[self.selected_pick_uom + '_Picks'].values,
                              color='blue')

            ax.set_title(f'{self.selected_pick_uom} Pick - Frequency: Monthly')
            ax.set_xlabel('Month')
            ax.set_ylabel(f'{self.selected_pick_uom} Pick')
            ax.tick_params(axis='x', rotation=45)

            # Annotate all bars
            for index, bar in enumerate(bars):
                height = bar.get_height()
                ax.annotate(format_value(height),
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 2),  # Slightly above the bar
                            textcoords="offset points",
                            ha='center', color='blue', clip_on=True, fontsize=5)

            # Set y-axis limits
            ax.set_ylim(0, max([bar.get_height() for bar in bars]) * 1.1)  # Adding some padding

        plt.tight_layout()

        # Pack the UOM canvas again
        self.outbound_daily_canvas_UOM.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        self.outbound_daily_canvas_UOM.draw()  # Update the UOM canvas

    def create_graph_inbound(self, data, fig, frequency, profile_metric, pick_uom):


        ax = fig.add_subplot(211)
        fig = plt.Figure(figsize=(8, 4), dpi=100)
        # Convert data to numeric and drop NaN values
        data = pd.to_numeric(data, errors='coerce').dropna()
        # print(data)
        # print(frequency)
        def format_value(value):
            """Format the value for display."""
            if value >= 1000:
                return f"{value / 1000:.1f}k"  # Convert to 'k' format
            return str(value)

        # Set up the title logic based on Pick UOM
        if pick_uom == 'All':
            title = f"Overall {profile_metric} Received - {frequency}"
        else:
            title = f"{profile_metric} Received as {pick_uom} - {frequency}"

        if frequency == 'Daily':
            ax.plot(data.index, data.values,color='#2760F8')
            ax.set_title(title)
            ax.set_title(f'{profile_metric} Received as {pick_uom} - Daily')

            ax.set_xlabel('Date')
            ax.set_ylabel(profile_metric)
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=10))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # Identify the top 10 peaks
            top_peaks = data.nlargest(10)
            # ax.scatter(top_peaks.index, top_peaks.values, color='blue', s=100, zorder=5)  # Mark top peaks

            # Annotate top peaks
            for peak in top_peaks.index:
                ax.annotate(format_value(top_peaks[peak]), (peak, top_peaks[peak]),
                            textcoords="offset points", xytext=(0, 5), ha='center', color='blue',fontsize=6)


        elif frequency == 'Weekly':
            week_labels = [f'W{week + 1}' for week in range(len(data))]
            bars=ax.bar(week_labels, data.values, color='#2760F8', width=0.5)
            ax.set_title(title)
            ax.set_xlabel('Week')
            ax.set_ylabel(profile_metric)
            ax.tick_params(axis='x', rotation=45)
            # Annotate values on top of each bar
            for bar in bars:
                height = bar.get_height()
                ax.annotate(format_value(height),
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 5),
                            textcoords="offset points",
                            ha='center', color='blue',fontsize=6)
            ax.yaxis.grid(False)


        elif frequency == 'Monthly':
            bars=ax.bar(data.index.strftime('%b %Y'), data.values, color='#2760F8', width=0.4)
            ax.set_title(title)
            ax.set_xlabel('Month')
            ax.set_ylabel(profile_metric)
            ax.tick_params(axis='x', rotation=45)
            # Annotate values on top of each bar
            for bar in bars:
                height = bar.get_height()
                ax.annotate(format_value(height),
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 5),
                            textcoords="offset points",
                            ha='center', color='blue',fontsize=6)
            ax.yaxis.grid(False)

        # Set y-axis limits
        if data.size > 0:
            ax.set_ylim(0, max(data.values) * 1.1)
        plt.tight_layout()

        plt.tight_layout()


    def create_graph_outbound(self, results, freq, profile_metric, pick_uom, dc_name):
        colors = ['#2760F8', '#2760F8', '#2760F8']
        frequencies = ['Daily', 'Weekly', 'Monthly']
        ax = None
        # print(results)
        # print(freq)
        def format_value(value):
            """Format the value for display."""
            if value >= 1000:
                return f"{value / 1000:.1f}k"  # Convert to 'k' format
            return str(value)

        results = pd.to_numeric(results, errors='coerce').dropna()

        # Set up the label/title logic based on Pick UOM
        if pick_uom == 'All':
            title = f"Overall {profile_metric} Picked - {dc_name} ({freq})"
        else:
            title = f"{profile_metric} Picked as {pick_uom} - {dc_name} ({freq})"

        if freq == 'Daily':
            ax = self.outbound_daily_fig.add_subplot(211)
            fig = plt.Figure(figsize=(8, 4), dpi=100)
            ax.plot(results.index.strftime('%Y-%m-%d'), results.values, color=colors[0])
            ax.set_title(title)
            ax.set_xlabel('Date')
            ax.set_ylabel(profile_metric)
            ax.tick_params(axis='x', rotation=45)
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=10))
            # Identify and annotate the top 10 peaks
            top_peaks = results.nlargest(10)
            # ax.scatter(top_peaks.index.strftime('%Y-%m-%d'), top_peaks.values, color='blue', s=100, zorder=5)

            for peak in top_peaks.index:
                ax.annotate(format_value(top_peaks[peak]), (peak.strftime('%Y-%m-%d'), top_peaks[peak]),
                            textcoords="offset points", xytext=(0, 5), ha='center', color='blue',fontsize=6)



        elif freq == 'Weekly':
            fig = plt.Figure(figsize=(8, 4), dpi=100)
            ax = self.outbound_weekly_fig.add_subplot(211)
            week_labels = [f'W{week + 1}' for week in range(len(results))]
            bars=ax.bar(week_labels, results.values, color=colors[1], width=0.5)
            ax.set_title(title)
            ax.set_xlabel('Week')
            ax.set_ylabel(profile_metric)
            # Annotate values on top of each bar
            for bar in bars:
                height = bar.get_height()
                ax.annotate(format_value(height), xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 5), textcoords="offset points", ha='center', color='blue',fontsize=6)



        elif freq == 'Monthly':
            fig = plt.Figure(figsize=(8, 4), dpi=100)
            ax = self.outbound_monthly_fig.add_subplot(211)
            bars=ax.bar(results.index.strftime('%b %Y'), results.values, color=colors[2], width=0.4)
            ax.set_title(title)
            ax.set_xlabel('Month')
            ax.set_ylabel(profile_metric)
            # Annotate values on top of each bar
            for bar in bars:
                height = bar.get_height()
                ax.annotate(format_value(height), xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 5), textcoords="offset points", ha='center', color='blue',fontsize=6)

        if ax:
            ax.tick_params(axis='x', rotation=45)
            if results.size > 0:
                ax.set_ylim(0, max(results.values) * 1.1)
            plt.tight_layout()


    def calculate_metrics_inbound(self, df, profile_metric, pick_uom):
        # Ensure that 'Order_Date' is the index and it's a DatetimeIndex
        if df.index.dtype != 'datetime64[ns]':
            df['Received_Date'] = pd.to_datetime(df['Received_Date'])
            df.set_index('Received_Date', inplace=True)
        # df.set_index('Received_Date', inplace=True)
        results = {}
        frequencies = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'ME'}

        for freq_name, freq in frequencies.items():
            grouped = df.resample(freq)

            if pick_uom == "Case":
                if profile_metric == "Units":
                    results[freq_name] = grouped['Units_Case'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['Orders_Case'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['Lines_Case'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['SKUs_Case'].sum()

            elif pick_uom == "Pallet":
                if profile_metric == "Units":
                    results[freq_name] = grouped['Units_Pallet'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['Orders_Pallet'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['Lines_Pallet'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['SKUs_Pallet'].sum()

            elif pick_uom == "Inner":
                if profile_metric == "Units":
                    results[freq_name] = grouped['Units_Inner'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['Orders_Inner'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['Lines_Inner'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['SKUs_Inner'].sum()

            elif pick_uom == "Each":
                if profile_metric == "Units":
                    results[freq_name] = grouped['Units_Each'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['Orders_Each'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['Lines_Each'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['SKUs_Each'].sum()

            elif pick_uom == "Layer":
                if profile_metric == "Units":
                    results[freq_name] = grouped['Units_Layer'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['Orders_Layer'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['Lines_Layer'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['SKUs_Layer'].sum()

            elif pick_uom == "All":
                if profile_metric == "Units":
                    results[freq_name] = (
                            grouped['Units_Case'].sum().apply(Decimal) +
                            grouped['Units_Pallet'].sum().apply(Decimal) +
                            grouped['Units_Inner'].sum().apply(Decimal) +
                            grouped['Units_Each'].sum().apply(Decimal) +
                            grouped['Units_Layer'].sum().apply(Decimal)
                    )
                elif profile_metric == "Orders":
                    results[freq_name] = (
                            grouped['Orders_Case'].sum().apply(Decimal) +
                            grouped['Orders_Pallet'].sum().apply(Decimal) +
                            grouped['Orders_Inner'].sum().apply(Decimal) +
                            grouped['Orders_Each'].sum().apply(Decimal) +
                            grouped['Orders_Layer'].sum().apply(Decimal)
                    )
                elif profile_metric == "Lines":
                    results[freq_name] = (
                            grouped['Lines_Case'].sum().apply(Decimal) +
                            grouped['Lines_Pallet'].sum().apply(Decimal) +
                            grouped['Lines_Inner'].sum().apply(Decimal) +
                            grouped['Lines_Each'].sum().apply(Decimal) +
                            grouped['Lines_Layer'].sum().apply(Decimal)
                    )
                elif profile_metric == "SKUs":
                    results[freq_name] = (
                            grouped['SKUs_Case'].sum().apply(Decimal) +
                            grouped['SKUs_Pallet'].sum().apply(Decimal) +
                            grouped['SKUs_Inner'].sum().apply(Decimal) +
                            grouped['SKUs_Each'].sum().apply(Decimal) +
                            grouped['SKUs_Layer'].sum().apply(Decimal)
                    )


            # Store results as DataFrames for export
        self.daily_data = results.get('Daily', pd.DataFrame())
        self.weekly_data = results.get('Weekly', pd.DataFrame())
        self.monthly_data = results.get('Monthly', pd.DataFrame())

        return results

    def calculate_inbound_uom_data(self):
        # Set the index for easier resampling
        # self.INBOUND.set_index('DATE', inplace=True)

        # Resample based on the selected frequency
        freq_map = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'ME'}
        frequency = freq_map.get(self.frequency_var_ib.get(), 'D')

        # Aggregate UOM data
        uom_data = self.INBOUND.resample(frequency).sum()[
            ['Pallet_Picks', 'Case_Picks', 'Layer_Picks', 'Inner_Picks', 'Each_Picks']]

        # Convert columns to numeric to avoid NaNs
        uom_data = uom_data.apply(pd.to_numeric, errors='coerce')

        # Always create Total_Picks column
        uom_data['Total_Picks'] = uom_data.sum(axis=1)  # Sum across the rows

        # Create final DataFrames for each frequency
        self.daily_uom_data = uom_data.resample('D').sum()  # Daily data
        if self.daily_uom_data.empty:
            print("No daily UOM data available to export.")
            return  # Exit the method

        self.weekly_uom_data = uom_data.resample('W').sum()  # Weekly data
        self.monthly_uom_data = uom_data.resample('ME').sum()  # Monthly data

        return uom_data

    def export_to_csv(self):
        selected_frequency = self.frequency_var_ib.get()
        selected_pick = self.inbound_uom_var.get()  # Assuming you have a variable for selected pick
        uom_column_map = {
            "Case": "Case_Picks",
            "Pallet": "Pallet_Picks",
            "Inner": "Inner_Picks",
            "Each": "Each_Picks",
            "Layer": "Layer_Picks",
            "All": "Total_Picks" # Handle "All" separately if needed
        }

        if selected_frequency:
            # Prompt user to choose a location and name for the metrics CSV
            metrics_filename = filedialog.asksaveasfilename(
                title=f"Save {selected_frequency} Metrics Data Inbound",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile=f'{selected_frequency}_metrics.csv'
            )

            # Check if a filename was provided
            if metrics_filename:
                # Export metrics data
                if selected_frequency == 'Daily' and hasattr(self, 'daily_data'):
                    self.daily_data.to_csv(metrics_filename, index=True)
                    print(f"Daily metrics data exported to {metrics_filename}")
                elif selected_frequency == 'Weekly' and hasattr(self, 'weekly_data'):
                    self.weekly_data.to_csv(metrics_filename, index=True)
                    print(f"Weekly metrics data exported to {metrics_filename}")
                elif selected_frequency == 'Monthly' and hasattr(self, 'monthly_data'):
                    self.monthly_data.to_csv(metrics_filename, index=True)
                    print(f"Monthly metrics data exported to {metrics_filename}")
                else:
                    print("No metrics data available for the selected frequency.")


            # Determine the UOM column based on the selected pick
            uom_column = uom_column_map.get(selected_pick)

            # Prompt user to choose a location and name for the UOM CSV
            uom_filename = filedialog.asksaveasfilename(
                title=f"Save {selected_frequency} UOM Data Inbound",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile=f'{selected_frequency}_uom.csv'
            )

            # Export UOM data if filename is provided
            if uom_filename:
                if uom_column:  # Only export if a specific column is selected
                    if selected_frequency == 'Daily' and hasattr(self, 'daily_uom_data'):
                        self.daily_uom_data[[uom_column]].to_csv(uom_filename, index=True)
                        print(f"Daily UOM data exported to {uom_filename}")
                    elif selected_frequency == 'Weekly' and hasattr(self, 'weekly_uom_data'):
                        self.weekly_uom_data[[uom_column]].to_csv(uom_filename, index=True)
                        print(f"Weekly UOM data exported to {uom_filename}")
                    elif selected_frequency == 'Monthly' and hasattr(self, 'monthly_uom_data'):
                        self.monthly_uom_data[[uom_column]].to_csv(uom_filename, index=True)
                        print(f"Monthly UOM data exported to {uom_filename}")
                    else:
                        print("No UOM data available for the selected frequency.")
                else:
                    # Handle the "All" case if needed
                    if selected_frequency == 'Daily' and hasattr(self, 'daily_uom_data'):
                        self.daily_uom_data.to_csv(uom_filename, index=True)
                        print(f"Daily UOM data exported to {uom_filename}")
                    elif selected_frequency == 'Weekly' and hasattr(self, 'weekly_uom_data'):
                        self.weekly_uom_data.to_csv(uom_filename, index=True)
                        print(f"Weekly UOM data exported to {uom_filename}")
                    elif selected_frequency == 'Monthly' and hasattr(self, 'monthly_uom_data'):
                        self.monthly_uom_data.to_csv(uom_filename, index=True)
                        print(f"Monthly UOM data exported to {uom_filename}")
                    else:
                        print("No UOM data available for the selected frequency.")

    def calculate_metrics_outbound(self, df, profile_metric, pick_uom):
        # Ensure that 'Order_Date' is the index and it's a DatetimeIndex
        if df.index.dtype != 'datetime64[ns]':
            df['Order_Date'] = pd.to_datetime(df['Order_Date'])
            df.set_index('Order_Date', inplace=True)
        # df.set_index('Order_Date', inplace=True)
        results = {}
        frequencies = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'ME'}

        for freq_name, freq in frequencies.items():
            grouped = df.resample(freq)

            if pick_uom == "Case":
                if profile_metric == "Units":
                    results[freq_name] = grouped['TotalCaseUnits'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['TotalCaseOrders'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['TotalCaseLines'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['TotalCaseSKUs'].sum()

            elif pick_uom == "Pallet":
                if profile_metric == "Units":
                    results[freq_name] = grouped['TotalPalletUnits'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['TotalPalletOrders'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['TotalPalletLines'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['TotalPalletSKUs'].sum()

            elif pick_uom == "Inner":
                if profile_metric == "Units":
                    results[freq_name] = grouped['TotalInnerUnits'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['TotalInnerOrders'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['TotalInnerLines'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['TotalInnerSKUs'].sum()

            elif pick_uom == "Each":
                if profile_metric == "Units":
                    results[freq_name] = grouped['TotalEachUnits'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['TotalEachOrders'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['TotalEachLines'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['TotalEachSKUs'].sum()

            elif pick_uom == "Layer":
                if profile_metric == "Units":
                    results[freq_name] = grouped['TotalLayerUnits'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['TotalLayerOrders'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['TotalLayerLines'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['TotalLayerSKUs'].sum()

            elif pick_uom == "All":
                if profile_metric == "Units":
                    results[freq_name] = grouped['TotalCaseUnits'].sum() + grouped['TotalPalletUnits'].sum() + \
                                         grouped['TotalInnerUnits'].sum() + grouped['TotalEachUnits'].sum() + \
                                         grouped['TotalLayerUnits'].sum()
                elif profile_metric == "Orders":
                    results[freq_name] = grouped['TotalCaseOrders'].sum() + grouped['TotalPalletOrders'].sum() + \
                                         grouped['TotalInnerOrders'].sum() + grouped['TotalEachOrders'].sum() + \
                                         grouped['TotalLayerOrders'].sum()
                elif profile_metric == "Lines":
                    results[freq_name] = grouped['TotalCaseLines'].sum() + grouped['TotalPalletLines'].sum() + \
                                         grouped['TotalInnerLines'].sum() + grouped['TotalEachLines'].sum() + \
                                         grouped['TotalLayerLines'].sum()
                elif profile_metric == "SKUs":
                    results[freq_name] = grouped['TotalCaseSKUs'].sum() + grouped['TotalPalletSKUs'].sum() + \
                                         grouped['TotalInnerSKUs'].sum() + grouped['TotalEachSKUs'].sum() + \
                                         grouped['TotalLayerSKUs'].sum()
        # print(results)
            # Store results as DataFrames for export
        self.daily_data = results.get('Daily', pd.DataFrame())
        self.weekly_data = results.get('Weekly', pd.DataFrame())
        self.monthly_data = results.get('Monthly', pd.DataFrame())

        return results

    def calculate_outbound_uom_data(self):
        # Resample based on the selected frequency
        freq_map = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'ME'}
        frequency = freq_map.get(self.frequency_var_ob.get(), 'D')

        # Aggregate UOM data
        uom_data = self.OUTBOUND.resample(frequency).sum()[
            ['Pallet_Picks', 'Case_Picks', 'Layer_Picks', 'Inner_Picks', 'Each_Picks']
        ]

        # Convert columns to numeric to avoid NaNs
        uom_data = uom_data.apply(pd.to_numeric, errors='coerce')

        # Always create Total_Picks column
        uom_data['Total_Picks'] = uom_data.sum(axis=1)  # Sum across the rows

        # Create final DataFrames for each frequency
        self.daily_uom_data = uom_data.resample('D').sum()  # Daily data
        if self.daily_uom_data.empty:
            print("No daily UOM data available to export.")
            return  # Exit the method

        self.weekly_uom_data = uom_data.resample('W').sum()  # Weekly data
        self.monthly_uom_data = uom_data.resample('ME').sum()  # Monthly data

        return uom_data  # Return full uom_data, including Total_Picks

    def export_to_csv_outbound(self):
        selected_frequency = self.frequency_var_ob.get()
        selected_pick = self.outbound_uom_var.get()  # Assuming you have a variable for selected pick
        uom_column_map = {
            "Case": "Case_Picks",
            "Pallet": "Pallet_Picks",
            "Inner": "Inner_Picks",
            "Each": "Each_Picks",
            "Layer": "Layer_Picks",
            "All": "Total_Picks"  # Handle "All" to export Total_Picks
        }

        if selected_frequency:
            # Prompt user to choose a location for the metrics CSV
            metrics_filename = filedialog.asksaveasfilename(
                title=f"Save {selected_frequency} Metrics Data Outbound",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile=f'{selected_frequency}_metrics.csv'
            )

            # Check if a filename was provided
            if metrics_filename:
                # Export metrics data
                if selected_frequency == 'Daily' and hasattr(self, 'daily_data'):
                    self.daily_data.to_csv(metrics_filename, index=True)
                    print(f"Daily metrics data exported to {metrics_filename}")
                elif selected_frequency == 'Weekly' and hasattr(self, 'weekly_data'):
                    self.weekly_data.to_csv(metrics_filename, index=True)
                    print(f"Weekly metrics data exported to {metrics_filename}")
                elif selected_frequency == 'Monthly' and hasattr(self, 'monthly_data'):
                    self.monthly_data.to_csv(metrics_filename, index=True)
                    print(f"Monthly metrics data exported to {metrics_filename}")
                else:
                    print("No metrics data available for the selected frequency.")

            # Determine the UOM column based on the selected pick
            uom_column = uom_column_map.get(selected_pick)

            # Prompt user to choose a location for the UOM CSV
            uom_filename = filedialog.asksaveasfilename(
                title=f"Save {selected_frequency} UOM Data Outbound",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile=f'{selected_frequency}_uom.csv'
            )

            # Export UOM data if filename is provided
            if uom_filename:
                if uom_column:  # Only export if a specific column is selected
                    if selected_frequency == 'Daily' and hasattr(self, 'daily_uom_data'):
                        self.daily_uom_data[[uom_column]].to_csv(uom_filename, index=True)
                        print(f"Daily UOM data exported to {uom_filename}")
                    elif selected_frequency == 'Weekly' and hasattr(self, 'weekly_uom_data'):
                        self.weekly_uom_data[[uom_column]].to_csv(uom_filename, index=True)
                        print(f"Weekly UOM data exported to {uom_filename}")
                    elif selected_frequency == 'Monthly' and hasattr(self, 'monthly_uom_data'):
                        self.monthly_uom_data[[uom_column]].to_csv(uom_filename, index=True)
                        print(f"Monthly UOM data exported to {uom_filename}")
                    else:
                        print("No UOM data available for the selected frequency.")
                else:
                    # Handle the "All" case
                    if selected_frequency == 'Daily' and hasattr(self, 'daily_uom_data'):
                        self.daily_uom_data.to_csv(uom_filename, index=True)
                        print(f"Daily UOM data (Total Picks) exported to {uom_filename}")
                    elif selected_frequency == 'Weekly' and hasattr(self, 'weekly_uom_data'):
                        self.weekly_uom_data.to_csv(uom_filename, index=True)
                        print(f"Weekly UOM data (Total Picks) exported to {uom_filename}")
                    elif selected_frequency == 'Monthly' and hasattr(self, 'monthly_uom_data'):
                        self.monthly_uom_data.to_csv(uom_filename, index=True)
                        print(f"Monthly UOM data (Total Picks) exported to {uom_filename}")
                    else:
                        print("No UOM data available for the selected frequency.")
        else:
            print("Please select a frequency to export.")

    def on_closing(self):
        self.connection.close()
        # self.root.destroy()
        self.notebook.master.destroy()  # Close the main Tk window


def main():
    root = tk.Tk()
    root.title("Summary and Seasonality Analysis")
    root.geometry("1200x600")

    # Create a single Notebook for both Summary and Seasonality tabs
    # fetch_min_max_dates_inbound()
    # fetch_min_max_dates_outbound()
    notebook = ttk.Notebook(root)
    # notebook.pack(side='top', fill='both', expand=True)
    notebook.pack(expand=True, fill=tk.BOTH)


    # Create Summary Tab (assuming you have a function for summary tabs)
    # Move this import here to avoid circular import issues

    # create_notebook_page_inbound_outbound(notebook)  # Add Inbound/Outbound Summary tabs
    app = SeasonalityApp(notebook)

    # final_main(notebook)
    # Initialize SeasonalityApp and pass the notebook


    # # Create both tabs
    # create_tab1(notebook)
    # create_tab2(notebook)
    # create_tab3(notebook)  # Add the new tab

    # Handle window close event
    root.protocol("WM_DELETE_WINDOW", app.on_closing)

    root.mainloop()

if __name__ == "__main__":
    main()



# if __name__ == "__main__":
#     root = tk.Tk()
#     app = SeasonalityApp(root)
#     root.protocol("WM_DELETE_WINDOW", app.on_closing)
#     root.mainloop()
