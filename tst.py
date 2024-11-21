import threading
import time
from tkinter import *
from tkinter import messagebox, ttk

from EntrySuggestion import AutocompleteEntry
from SKU_and_Picks_Analysis_Tabs import create_monthly_sku_strat_tab, create_sku_strat_aff_tab, build #, create_pick_aff_tab
from SKU_and_Picks_Analysis_Tabs import create_sku_strat_aff_tab  #TODO: Replace with line above when Fred is back
from Inbound_outbound_sql_code_testing_summary import  create_notebook_page_inbound_outbound_new
from Order_Categories_Summary_SM_Unit_Summary_New import final_main
from Order_Tailing_Summary_with_Backlog_Chart_V3 import order_tail
from SKU_wise_Date_wise_Summary_New import final_main_sku_date_analysis
from Seasonality_1031 import SeasonalityApp
from sku_strat_aff_funcs import delete_mysql_tables_from_db
import Shared
import pymysql
import pandas as pd
import ssl
from ctypes import windll
from helper_functions import get_asset_path

windll.shcore.SetProcessDpiAwareness(1)

sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')
tbl = f"client_data.{Shared.project}"
inbound_datatbl = f"temporary_data.{Shared.project_inbound}"


# Function to get distinct project selections from Wave_Pick_Master_Table
def EntryDB():
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
    query = """
        SELECT CONCAT(Customer_Name, " ", RFP_Desc, " Version:", Version, " Round:", Round) AS client, a.*
        FROM Engineering.Wave_Pick_Master_Table a
    """
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in cursor.description])
    connection.close()  # Close connection after data retrieval
    return df


def existing_customer():
    project_selections = EntryDB()
    customer_selections = project_selections["client"]

    def click():
        tbl = f"client_data.{Shared.project}"
        inbound_datatbl = f"temporary_data.{Shared.project_inbound}"
        global test
        test = project_selections[project_selections["client"] == MyEntry.get()]

        # Set outbound table name if it exists
        Shared.project = test["OB_Table_Name"].iloc[0] if "OB_Table_Name" in test.columns and not test[
            "OB_Table_Name"].isnull().all() else ""
        # print(f"Selected Outbound Table: {Shared.project}")

        # Set inbound table name if it exists
        Shared.project_inbound = test["IB_Table_Name"].iloc[0] if "IB_Table_Name" in test.columns and not test[
            "IB_Table_Name"].isnull().all() else ""
        # print(f"Selected Inbound Table: {Shared.project_inbound}")

        # Only open data summary if at least one table name is available
        if Shared.project or Shared.project_inbound:
            open_data_summary()
        else:
            messagebox.showerror("Selection Error",
                                 "Both outbound and inbound table names are missing. Please select a valid project.")

    def open_data_summary():
        dfmthlist = build()
        #dfmthlist = 'testcsv.csv'
        # Query outbound data if Shared.project is set
        # if Shared.project:
        #     try:
        #         query_outbound = f"SELECT * FROM client_data.{Shared.project} "
        #         # print(f"Outbound Query_testing: {query_outbound}")  # Debugging: Print query
        #         # Run outbound query
        #         # Add your query and processing code here for outbound
        #     except pymysql.MySQLError as e:
        #         print(f"Error fetching outbound data: {e}")
        # else:
        #     print("Outbound table name is missing. Skipping outbound data query.")
        #
        # # **Only proceed to the summary if the outbound query has data or is set**
        # if not Shared.project and not Shared.project_inbound:
        #     print("Both outbound and inbound tables are missing, skipping data summary.")
        #     return
        #
        # # Query inbound data if Shared.project_inbound is set
        # if Shared.project_inbound:
        #     try:
        #         query_inbound = f"SELECT * FROM client_data.{Shared.project_inbound}"
        #         # print(f"Inbound Query_testing: {query_inbound}")  # Debugging: Print query
        #         # Run inbound query
        #         # Add your query and processing code here for inbound
        #     except pymysql.MySQLError as e:
        #         print(f"Error fetching inbound data: {e}")
        # else:
        #     print("Inbound table name is missing. Skipping inbound data query.")

        # Create a new window for the data summary
        data_summary_root = Toplevel()  # Toplevel creates a new window, unlike Tk which is for the main window
        data_summary_root.title("Data Summary")
        data_summary_root.geometry("1200x800")

        notebook = ttk.Notebook(data_summary_root)
        notebook.pack(expand=True, fill=BOTH)

        # Create tabs as required
        #create_notebook_page_inbound_outbound_new(notebook)
        #SeasonalityApp(notebook)
        #final_main(notebook)  # Add Order Categories tab
        #order_tail(notebook)
        #final_main_sku_date_analysis(notebook)
        create_monthly_sku_strat_tab(notebook, dfmthlist) #TODO: Uncomment when Fred is back
        #create_sku_strat_aff_tab(notebook)
        # create_pick_aff_tab(notebook) #TODO: Uncomment when Fred is back

        # Close the parent window only after the child window is fully created
        root.withdraw()  # Hide the parent window
        data_summary_root.protocol("WM_DELETE_WINDOW", close_parent_window)

    def close_parent_window():
        """This function ensures the parent window (root) is closed properly when the child window is closed."""
        delete_mysql_tables_from_db()
        root.quit()  # Stops the mainloop of the parent window
        root.destroy()  # Destroys the parent window

    # Main project selection window
    global root  # Make sure 'root' is a global variable so we can destroy it from within the `click` function
    root = Tk()
    root.configure(bg="#FFFFFF")

    GXO_Label = Label(root, text="GXO ENGINEERING", bg='#FF3A00', justify="center", font=("Helvetica", 16, "bold"),
                      fg='white', pady=10, padx=20)
    GXO_Label.pack(pady=10, fill=X, padx=20)

    canvas_frame = Frame(root, bg='#FFFFFF')
    canvas_frame.pack(padx=100, pady=50, fill=BOTH, expand=True)

    Label(canvas_frame, text="Project Selection", font=("Helvetica", 10), bg='white', justify="center").grid(
        row=0, column=0, pady=10)
    MyEntry = AutocompleteEntry(customer_selections, canvas_frame, width=50)
    MyEntry.grid(row=0, column=1, columnspan=2, pady=10)

    MyButton = Button(canvas_frame, text="Confirm selection", command=click, borderwidth=2,
                      highlightthickness=2, font=("Helvetica", 10), pady=10, padx=20)
    MyButton.grid(row=2, column=1, pady=20)

    button_quit = Button(canvas_frame, text="EXIT", command=root.quit, fg="black", font=("Helvetica", 10), pady=10,
                         padx=20)
    button_quit.grid(row=3, column=1, pady=20)

    root.mainloop()


existing_customer()
