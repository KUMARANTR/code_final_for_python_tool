from tkinter import *
from tkinter import messagebox
from EntrySuggestion import AutocompleteEntry
from SKU_and_Picks_Analysis_Tabs import dfmthlist, create_tab1, create_tab2, create_sku_strat_aff_tab, create_pick_aff_tab
from DBupload import EntryDB_inbound, EntryDB_outbound
import Shared
import warnings
import tkinter as tk
from tkinter import ttk, messagebox
from Order_Categories_Summary_SM_Unit_Summary_New import final_main
from Order_Tailing_Summary_with_Backlog_Chart_V3 import order_tail
from SKU_wise_Date_wise_Summary_New import final_main_sku_date_analysis
from Seasonality_1031 import SeasonalityApp
from Summary_tab_new import create_notebook_page_inbound_outbound
import sku_strat_aff_funcs
from ctypes import windll

windll.shcore.SetProcessDpiAwareness(1)
warnings.simplefilter(action='ignore', category=FutureWarning)


def existing_customer():
    # Get table names for inbound and outbound
    inbound_tables = EntryDB_inbound()
    outbound_tables = EntryDB_outbound()

    def click():
        # Get values from the autocomplete entries
        project_inbound = MyEntryIn.get()
        project_outbound = MyEntryOut.get()

        if not project_inbound or not project_outbound:
            messagebox.showwarning("Selection Error", "Please select a project for both Inbound and Outbound.")
            return

        # Save the selected project names in the Shared module
        Shared.project_inbound = project_inbound
        Shared.project = project_outbound  # Assuming 'project' is for outbound

        print(f"Saved Inbound Project: {Shared.project_inbound}")
        print(f"Saved Outbound Project: {Shared.project}")

        # Destroy the initial selection screen
        root.destroy()
        # Call to create the main GUI and add all tabs
        open_data_summary()

    def open_data_summary():
        # Create the main GUI with the notebook
        root = Tk()
        root.title("Data Summary")
        root.geometry("1200x800")

        # Notebook for Tabs (Order Categories Summary & S/M Unit Summary)
        notebook = ttk.Notebook(root)
        notebook.pack(expand=True, fill=BOTH)

        # Call other functions to add tabs to the notebook
        create_notebook_page_inbound_outbound(notebook)
        SeasonalityApp(notebook)
        final_main(notebook)  # Add Order Categories tab
        order_tail(notebook)
        final_main_sku_date_analysis(notebook)
        create_tab1(notebook, dfmthlist)
        create_tab2(notebook)
        create_sku_strat_aff_tab(notebook)
        create_pick_aff_tab(notebook)  # Add the new tab

        root.mainloop()

    # GUI setup
    root = Tk()
    # root.geometry("580x580")
    root.configure(bg="#FFFFFF")

    # GXO Engineering label
    GXO_Label = Label(root, text="GXO ENGINEERING", bg='#FF3A00', justify="center", font=("Helvetica", 16, "bold"),
                      fg='white', pady=10, padx=20)
    GXO_Label.pack(pady=10, fill=X, padx=20)

    # Creating Frame
    canvas_frame = Frame(root, bg='#FFFFFF')
    canvas_frame.pack(padx=100, pady=50, fill=BOTH, expand=True)

    # Project Selection Inbound
    Label(canvas_frame, text="Project Selection Inbound", font=("Helvetica", 10), bg='white', justify="center").grid(
        row=0, column=0, pady=10)
    MyEntryIn = AutocompleteEntry(inbound_tables, canvas_frame, width=50)
    MyEntryIn.grid(row=0, column=1, columnspan=2, pady=10)

    # Project Selection Outbound
    Label(canvas_frame, text="Project Selection Outbound", font=("Helvetica", 10), bg='white', justify="center").grid(
        row=1, column=0, pady=10)
    MyEntryOut = AutocompleteEntry(outbound_tables, canvas_frame, width=50)
    MyEntryOut.grid(row=1, column=1, columnspan=2, pady=10)

    # Customer selection confirmation button
    MyButton = Button(canvas_frame, text="Confirm selection", command=click, borderwidth=2,
                      highlightthickness=2, font=("Helvetica", 10), pady=10, padx=20)
    MyButton.grid(row=2, column=1, pady=20)

    # Exit button
    button_quit = Button(canvas_frame, text="EXIT", command=root.quit, fg="black", font=("Helvetica", 10), pady=10,
                         padx=20)
    button_quit.grid(row=3, column=1, pady=20)

    root.mainloop()


# Call the existing_customer function to run the program
existing_customer()
