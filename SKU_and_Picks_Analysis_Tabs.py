import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import ExecuteQrys #TODO: Uncomment when Fred is back
import time
import re
import os
from sku_strat_aff_funcs import *
import Shared
from datetime import datetime
from Inbound_outbound_sql_code_testing_summary import (get_distinct_channel_filter_outbound,
                                                       get_distinct_dc_names_outbound, get_distinct_bu_filter_outbound)


def build():
    tbl = Shared.projtbl
    tblib = Shared.inboundtbl
    projtbl = f"client_data.{Shared.project}"
    ibtbl = f"client_data.{Shared.project_inbound}"
    global dfall, dfmthlist, dfpick, dfpickIB, agg, agg2
    start_time = time.time()

    dfbaseall = ExecuteQrys.DB_Pull1()

    ## Group by specified columns and sum the others
    dfall = dfbaseall.groupby(['Business_Unit', 'DC', 'Segment', 'EOM', 'SkuStrat'], as_index=False).agg({
        'SKUs': 'sum',
        'Orders': 'sum',
        'Line': 'sum',
        'Line2': 'sum',
        'Qty': 'sum'
    })

    # Pivot the DataFrame
    dfmthlist = dfbaseall.pivot_table(
        index=['Business_Unit', 'DC', 'Segment', 'sku'],
        columns='EOM',
        values='SkuStrat',
        aggfunc='first'  # Use 'first' since you have only one value per group
    ).reset_index()

    # Optional: Flatten the columns
    dfmthlist.columns.name = None  # Remove the name of the columns
    dfmthlist.columns = [str(col) for col in dfmthlist.columns]  # Convert to string if needed
    #
    #
    #dfpick = ExecuteQrys.DB_Pull4()
    ## dfstrat = ExecuteQrys.DB_Pull2()
    ## dfvar = ExecuteQrys.DB_Pull3()
    #dfpickIB = ExecuteQrys.DB_Pull6()

    dfmthlist = dfmthlist.fillna("")

    #dfpick.fillna("N/A", inplace=True)
    #dfpick = dfpick.sort_values(by='Units', ascending=True)

    print("--- %s seconds ---" % (time.time() - start_time))

    #dfpick.fillna("N/A", inplace=True)
    dfall['EOM'] = pd.to_datetime(dfall['EOM']).dt.date

    agg = dfall.groupby(['Business_Unit', 'DC', 'EOM', 'Segment', 'SkuStrat'])[['Qty']].sum().unstack().fillna(0)
    agg2 = dfall.groupby(['Business_Unit', 'DC', 'EOM', 'Segment', 'SkuStrat'])[['SKUs']].sum().unstack().fillna(0)

    agg['Total'] = agg.sum(axis=1)
    agg2['Total'] = agg2.sum(axis=1)

    return dfmthlist


def build2():
    tbl = Shared.projtbl
    tblib = Shared.inboundtbl
    projtbl = f"client_data.{Shared.project}"
    ibtbl = f"client_data.{Shared.project_inbound}"
    global dfpick, dfpickIB
    start_time = time.time()

    dfpick = ExecuteQrys.DB_Pull4()
    dfpickIB = ExecuteQrys.DB_Pull6()

    dfpick.fillna("N/A", inplace=True)
    dfpick = dfpick.sort_values(by='Units', ascending=True)

    print("--- %s seconds ---" % (time.time() - start_time))

    dfpick.fillna("N/A", inplace=True)

    return dfpick, dfpickIB
#

# build()


def convert_date_format(date_str):
    # Convert from 'MM/DD/YYYY' to 'YYYY-MM-DD'
    try:
        date_obj = datetime.strptime(date_str, '%m/%d/%Y')  # Parse the date string
        return date_obj.strftime('%Y-%m-%d')  # Format it as 'YYYY-MM-DD'
    except ValueError:
        return "Invalid date format"


# def format_func(value, ticks):
#     if value >= 1000:
#         value = value / 1000
#         return f"{value:,.1f}K"  # Change this line to include commas
#     else:
#         return f"{value:,.0f}"
#
#
# def format_base(value):
#     if value >= 1000:
#         value = value / 1000
#         return f"{value:,.1f}K"
#     else:
#         return f"{value:,.0f}"
#
def format_func(value, ticks):
    if value >= 1_000_000:  # Check if the value is greater than or equal to 1 million
        value = value / 1_000_000
        return f"{value:,.1f}M"  # Format it in millions with 1 decimal place
    elif value >= 1000:  # Check if the value is greater than or equal to 1 thousand
        value = value / 1000
        return f"{value:,.1f}K"  # Format it in thousands with 1 decimal place
    else:
        return f"{value:,.0f}"  # For values less than 1000, show as is with no decimal

def format_base(value):
    if value >= 1_000_000:  # Check if the value is greater than or equal to 1 million
        value = value / 1_000_000
        return f"{value:,.1f}M"  # Format it in millions with 1 decimal place
    elif value >= 1000:  # Check if the value is greater than or equal to 1 thousand
        value = value / 1000
        return f"{value:,.1f}K"  # Format it in thousands with 1 decimal place
    else:
        return f"{value:,.0f}"  # For values less than 1000, show as is with no decimal


def format_qty(df, columns=None):
    if columns is not None:
        for col in columns:
            df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else '')
    return df


def format_percentages(df, percent_columns=None):
    if percent_columns is not None:
        for column in percent_columns:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: f"{x * 100:.1f}%" if pd.notnull(x) else '')

    return df

#
def format_and_calculate(df):
    # Convert original numbers to formatted strings with commas and 0 decimal places
    df['Low Variance'] = df['Low Variance'].map(lambda x: f"{int(x):,}")
    df['Medium Variance'] = df['Medium Variance'].map(lambda x: f"{int(x):,}")
    df['High Variance'] = df['High Variance'].map(lambda x: f"{int(x):,}")
    df['Total SKU'] = df['Total SKU'].map(lambda x: f"{int(x):,}")

    # Convert numeric columns for calculations
    df['Low Variance'] = pd.to_numeric(df['Low Variance'].str.replace(',', ''), errors='coerce')
    df['Medium Variance'] = pd.to_numeric(df['Medium Variance'].str.replace(',', ''), errors='coerce')
    df['High Variance'] = pd.to_numeric(df['High Variance'].str.replace(',', ''), errors='coerce')
    df['Total SKU'] = pd.to_numeric(df['Total SKU'].str.replace(',', ''), errors='coerce')

    # Calculate variance percentages
    df['Low Variance %'] = (df['Low Variance'] / df['Total SKU'] * 100).map(lambda x: f"{x:.1f}%")
    df['Medium Variance %'] = (df['Medium Variance'] / df['Total SKU'] * 100).map(lambda x: f"{x:.1f}%")
    df['High Variance %'] = (df['High Variance'] / df['Total SKU'] * 100).map(lambda x: f"{x:.1f}%")
    df['Total SKU %'] = (df['Total SKU'] / df['Total SKU'] * 100).map(lambda x: f"{x:.1f}%")  # This will always be 100%

    df['Low Variance'] = df['Low Variance'].map(lambda x: f"{int(x):,}")
    df['Medium Variance'] = df['Medium Variance'].map(lambda x: f"{int(x):,}")
    df['High Variance'] = df['High Variance'].map(lambda x: f"{int(x):,}")
    df['Total SKU'] = df['Total SKU'].map(lambda x: f"{int(x):,}")

    return df


# def export_sku_strat_affinity():
#     # Open a file dialog to choose a directory
#     folder_selected = filedialog.askdirectory(title="Select Folder to Save Files")
#
#     if folder_selected:  # Check if a folder was selected
#         # Define your file names
#         stratification_file = "sku_stratification.csv"
#         affinity_file = "sku_affinity.csv"
#
#         # Construct full file paths
#         stratification_path = os.path.join(folder_selected, stratification_file)
#         affinity_path = os.path.join(folder_selected, affinity_file)
#
#         # Save the DataFrames as CSV files
#         global_df_stratification.to_csv(stratification_path, index=False)
#         global_df_affinity.to_csv(affinity_path, index=False)
#
#         messagebox.showinfo("Success", f"Files saved to:\n{folder_selected}")
#         #print(f"Files saved to {folder_selected}")


def export_sku_strat_affinity_custom():
    # Open a file dialog to choose a directory
    folder_selected = filedialog.askdirectory(title="Select Folder to Save Files")

    if folder_selected:  # Check if a folder was selected
        # Define your file names
        stratification_file = "sku_stratification_custom.csv"
        affinity_file = "sku_affinity_custom.csv"

        # Construct full file paths
        stratification_path = os.path.join(folder_selected, stratification_file)
        affinity_path = os.path.join(folder_selected, affinity_file)

        # Save the DataFrames as CSV files
        try:
            global_df_stratification_custom.to_csv(stratification_path, index=False)
            messagebox.showinfo("Success", f"SKU Stratification file saved to:\n{folder_selected}")
        except NameError:
            messagebox.showinfo("Warning", f"No SKU Stratification table to save.")

        try:
            global_df_affinity_custom.to_csv(affinity_path, index=False)
            messagebox.showinfo("Success", f"SKU Affinity file saved to:\n{folder_selected}")
        except NameError:
            messagebox.showinfo("Warning", f"No SKU Affinity table to save.")


        # messagebox.showinfo("Success", f"Files saved to:\n{folder_selected}")
        #print(f"Files saved to {folder_selected}")


# First Tab Functions
def update_chart(selected_sku, selected_bu, selected_dc, ax, canvas, agg_data, dfmthlist, tree, is_sku_chart=False):
    ax.clear()

    # Filter the data based on selections
    filtered_agg = agg_data.loc[
                   (slice(selected_bu, selected_bu), slice(selected_dc, selected_dc), slice(None), selected_sku), :
                   ]
    agg_reset = filtered_agg.reset_index()

    # Convert EOM to datetime if it's not already
    agg_reset['EOM'] = pd.to_datetime(agg_reset['EOM'])

    width = 20

    if is_sku_chart:
        # Use SKUs for the second chart
        bar1 = ax.bar(agg_reset['EOM'], agg_reset.get(('SKUs', 'A'), pd.Series([0] * len(agg_reset))), label='A SKUs',
                      width=width, color='#118DFF')
        bar2 = ax.bar(agg_reset['EOM'], agg_reset.get(('SKUs', 'B'), pd.Series([0] * len(agg_reset))),
                      bottom=agg_reset.get(('SKUs', 'A'), pd.Series([0] * len(agg_reset))), label='B SKUs', width=width,
                      color='#12239E')
        bar3 = ax.bar(agg_reset['EOM'], agg_reset.get(('SKUs', 'C'), pd.Series([0] * len(agg_reset))),
                      bottom=agg_reset.get(('SKUs', 'B'), pd.Series([0] * len(agg_reset))) + agg_reset.get(
                          ('SKUs', 'A'), pd.Series([0] * len(agg_reset))), label='C SKUs', width=width, color='#E66C37')
    else:
        # Use Qty for the first chart
        bar1 = ax.bar(agg_reset['EOM'], agg_reset.get(('Qty', 'A'), pd.Series([0] * len(agg_reset))), label='A Units',
                      width=width, color='#118DFF')
        bar2 = ax.bar(agg_reset['EOM'], agg_reset.get(('Qty', 'B'), pd.Series([0] * len(agg_reset))),
                      bottom=agg_reset.get(('Qty', 'A'), pd.Series([0] * len(agg_reset))), label='B Units', width=width,
                      color='#12239E')
        bar3 = ax.bar(agg_reset['EOM'], agg_reset.get(('Qty', 'C'), pd.Series([0] * len(agg_reset))),
                      bottom=agg_reset.get(('Qty', 'B'), pd.Series([0] * len(agg_reset))) + agg_reset.get(('Qty', 'A'),
                                                                                                          pd.Series(
                                                                                                              [0] * len(
                                                                                                                  agg_reset))),
                      label='C Units', width=width, color='#E66C37')

        # Set y-axis limits
    # max_value = agg_reset[['Qty', ('Qty', 'A'), ('Qty', 'B'),
    #                           ('Qty', 'C')]].max().max()  # Find the maximum value from the relevant columns
    # ax.set_ylim(0, max_value * 1.1)

    ax.set_xticks(agg_reset['EOM'])
    #ax.set_xticklabels(agg_reset['EOM'].dt.strftime('%Y-%m'))  # Format the labels
    ax.set_xticklabels(agg_reset['EOM'].dt.strftime('%Y-%m'), rotation=45, ha='right')

    for i in range(len(agg_reset)):
        total = agg_reset['Total'].iloc[i]
        formatted_total = format_base(total)

        #Calculate y_pos safely
        y_pos = (agg_reset.get(('SKUs', 'A') if is_sku_chart else ('Qty', 'A'), pd.Series([0] * len(agg_reset))).iloc[i] +
                  agg_reset.get(('SKUs', 'B') if is_sku_chart else ('Qty', 'B'), pd.Series([0] * len(agg_reset))).iloc[i] +
                  agg_reset.get(('SKUs', 'C') if is_sku_chart else ('Qty', 'C'), pd.Series([0] * len(agg_reset))).iloc[i] + 0.2)

        skus_a = agg_reset.get(('SKUs', 'A') if is_sku_chart else ('Qty', 'A'), pd.Series([0] * len(agg_reset))).iloc[i]
        skus_b = agg_reset.get(('SKUs', 'B') if is_sku_chart else ('Qty', 'B'), pd.Series([0] * len(agg_reset))).iloc[i]
        skus_c = agg_reset.get(('SKUs', 'C') if is_sku_chart else ('Qty', 'C'), pd.Series([0] * len(agg_reset))).iloc[i]

        # Convert to float if necessary
        y_pos = (float(skus_a) + float(skus_b) + float(skus_c) + 0.2)

        x_pos = agg_reset['EOM'].iloc[i]
        ax.text(x_pos, y_pos, f"{formatted_total}", ha='center', va='bottom', rotation=0)

    ax.set_title(
        f'# {"SKUs" if is_sku_chart else "Units"} by Month (SKU: {selected_sku}, BU: {selected_bu}, DC: {selected_dc})')
    ax.legend()
    ax.yaxis.set_major_formatter(FuncFormatter(format_func))

    canvas.draw()

    # Load the pivoted DataFrame into the Treeview
    load_treeview(dfmthlist, tree)


def export_data(agg_data, df_mthllist):
    # Open a file dialog to save the files
    file_path_agg = filedialog.asksaveasfilename(defaultextension=".csv",
                                                 filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                                                 title="Save Agg Data as CSV")
    if file_path_agg:
        agg_data.reset_index(inplace=True)
        agg_data.to_csv(file_path_agg, index=False)  # Save agg_data as CSV

    file_path_mthllist = filedialog.asksaveasfilename(defaultextension=".csv",
                                                      filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                                                      title="Save Month List as CSV")
    if file_path_mthllist:
        df_mthllist.to_csv(file_path_mthllist, index=False)


def create_monthly_sku_strat_tab(notebook):

    def update_with_calculate():
        # Call the build function to recalculate data
        global dfmthlist
        dfmthlist = build()  # This will rebuild the dfmthlist and associated data

        # After calculating, update the chart and Treeview
        selected_sku = sku_combobox.get()

        # Filter dfmthlist based on the selected SKU
        filtered_dfmthlist = dfmthlist[dfmthlist['Segment'] == selected_sku]

        # Update the charts with the new data
        update_chart(selected_sku, "All", "All", ax1, canvas1, agg, filtered_dfmthlist, tree)
        update_chart(selected_sku, "All", "All", ax2, canvas2, agg2, filtered_dfmthlist, tree, is_sku_chart=True)

        # Load the filtered DataFrame into the Treeview
        filtered_columns = dfmthlist.columns[3:]  # Get the columns from index 4 to the end
        tree["columns"] = list(filtered_columns)

        # Configure headings and columns
        for col in filtered_columns:
            col_header_formatted = col
            if col != 'sku':  # Format month columns
                date_obj = datetime.strptime(col, "%Y-%m-%d")
                col_header_formatted = date_obj.strftime("%b '%y")
            col_header_formatted = col_header_formatted.upper()

            tree.heading(col, text=col_header_formatted)
            tree.column(col, anchor="center")

        load_treeview(filtered_dfmthlist, tree)

    monthly_sku_strat_tab = ttk.Frame(notebook)
    notebook.add(monthly_sku_strat_tab, text="Monthly SKU Strat")

    # Create frame for labels and filters
    filters_frame = ttk.Frame(monthly_sku_strat_tab)
    filters_frame.grid(row=0, column=0, columnspan=3, padx=(0, 0), pady=(0, 5), sticky='w')

    # Create labels and comboboxes
    sku_label = ttk.Label(filters_frame, text="Select Order Type:")
    sku_label.grid(row=0, column=0, padx=(250, 0), pady=(5, 5), sticky='e')

    # SKU options
    sku_options = Shared.channel_names_outbound
    sku_options = [item if item != "ALL" else "All" for item in sku_options]
    sku_options = [item for item in sku_options if item is not None]

    sku_combobox = ttk.Combobox(filters_frame, values=sku_options)
    sku_combobox.current(0)  # Select the first item
    sku_combobox.grid(row=0, column=1, padx=(0, 100), pady=(5, 5), sticky='w')

    # Create an Export button
    export_button = ttk.Button(filters_frame, text="Export", command=lambda: export_data(agg, dfmthlist))
    export_button.grid(row=0, column=4, padx=(10, 0), pady=(5, 5), sticky='e')

    # Create a Calculate button
    calculate_button = ttk.Button(filters_frame, text="Calculate", command=update_with_calculate)
    calculate_button.grid(row=0, column=3, padx=(10, 0), pady=(5, 5), sticky='w')

    # Create new Notebook to hold the charts and treeview on separate tabs
    monthly_sku_strat_tab_notebook = ttk.Notebook(monthly_sku_strat_tab)
    monthly_sku_strat_tab_notebook.grid(row=1, column=0, columnspan=3, padx=(0, 0), pady=(0, 5), sticky='nsew')

    # Create frame for charts
    charts_frame = ttk.Frame(monthly_sku_strat_tab_notebook)
    monthly_sku_strat_tab_notebook.add(charts_frame, text="Monthly SKU Stratification Charts")

    # Create figures for both charts
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    fig2, ax2 = plt.subplots(figsize=(10, 6))

    # Create canvases to display the figures
    canvas1 = FigureCanvasTkAgg(fig1, master=charts_frame)
    canvas1.draw()
    canvas1.get_tk_widget().grid(row=0, column=0, sticky='nsew')

    canvas2 = FigureCanvasTkAgg(fig2, master=charts_frame)
    canvas2.draw()
    canvas2.get_tk_widget().grid(row=0, column=1, sticky='nsew')

    # Create frame for treeview
    treeview_frame = ttk.Frame(monthly_sku_strat_tab_notebook)
    monthly_sku_strat_tab_notebook.add(treeview_frame, text="Monthly SKU Stratification Table")

    # Create a label for the Treeview
    label = ttk.Label(treeview_frame, text="SKUs by Month", font=("Helvetica", 16, 'bold'))
    label.grid(row=0, column=0, columnspan=3, pady=(10, 0), sticky='w')

    # Create Treeview for displaying pivoted DataFrame
    tree = ttk.Treeview(treeview_frame, show="headings")
    tree.grid(row=1, column=0, columnspan=2, sticky='nsew')

    # Configure scrollbar
    scrollbar = ttk.Scrollbar(treeview_frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scrollbar.set)
    scrollbar.grid(row=1, column=2, sticky='ns')

    # Set up the columns for the Treeview
    filtered_columns = []  # Placeholder for column names


    # Bind the combobox selections to the update_chart function
    def on_combobox_change(event):
        if 'dfmthlist' in globals() and isinstance(dfmthlist, pd.DataFrame):
            selected_sku = sku_combobox.get()

            # Filter dfmthlist based on the selected SKU
            filtered_dfmthlist = dfmthlist[dfmthlist['Segment'] == selected_sku]

            update_chart(selected_sku, "All", "All", ax1, canvas1, agg, filtered_dfmthlist, tree)
            update_chart(selected_sku, "All", "All", ax2, canvas2, agg2, filtered_dfmthlist, tree, is_sku_chart=True)

            # Load the filtered DataFrame into the Treeview
            load_treeview(filtered_dfmthlist, tree)
        else:
            print("Data is not loaded yet.")

    sku_combobox.bind("<<ComboboxSelected>>", on_combobox_change)

    ## Load the initial pivoted DataFrame into the Treeview
    #load_treeview(dfmthlist, tree)

    ## Initial plot for both charts
    #initial_sku = sku_options[0]
    #update_chart(initial_sku, "All", "All", ax1, canvas1, agg, dfmthlist, tree)
    #update_chart(initial_sku, "All", "All", ax2, canvas2, agg2, dfmthlist, tree, is_sku_chart=True)

#def calcbutton():
    # dfmthlist = build()
    #
    # sku_options = agg.index.get_level_values(3).unique().tolist()
    # initial_sku = sku_options[0]
    # update_chart(initial_sku, "All", "All", ax1, canvas1, agg, dfmthlist, tree)
    # update_chart(initial_sku, "All", "All", ax2, canvas2, agg2, dfmthlist, tree, is_sku_chart=True)



def load_treeview(dfmthlist, tree):
    # Clear existing entries in the Treeview
    for row in tree.get_children():
        tree.delete(row)

    filtered_columns = dfmthlist.columns[3:]

    # Load data into Treeview using the filtered columns
    for _, row in dfmthlist[filtered_columns].iterrows():
        tree.insert("", "end", values=list(row))


# Second Tab Functions
# def create_tab2(notebook):
#     tab2 = ttk.Frame(notebook)
#     notebook.add(tab2, text="SKU Stratification & Affinity")
#     # global global_df_stratification, global_df_variance, global_df_affinity
#     global global_df_stratification, global_df_affinity, tree_stratification, tree_affinity
#     # global bu_combobox, dc_combobox, ordertype_entry, tree_stratification, tree_variance, tree_affinity
#     global ordertype_selection, strat_label, affinity_label
#     global std_table_name
#     std_table_name = Shared.project
#
#     # Style customization
#     style = ttk.Style()
#     style.theme_use('clam')
#     style.configure("Treeview", rowheight=25, background="white", foreground="black")
#     style.configure("Treeview.Heading", font=('Helvetica', 12, 'bold'), background="#FF3B00", foreground="white")
#
#     scrollable_frame = ttk.Frame(tab2)
#     scrollable_frame.pack(fill='both', expand=True)
#
#     # Order Type Filter
#     ordertype_label = ttk.Label(scrollable_frame, text="Select Order Type:")
#     ordertype_label.grid(row=0, column=0, padx=(0, 5), pady=(0, 5), sticky='e')
#     ordertype_options = get_order_types(std_table_name)
#     ordertype_selection = ttk.Combobox(scrollable_frame, values=ordertype_options)
#     ordertype_selection.set(ordertype_options[0])
#     ordertype_selection.grid(row=0, column=1, padx=(0, 5), pady=(0, 5), sticky='w')
#
#     # Create Calculate Button to update treeviews based on input
#     update_button = ttk.Button(scrollable_frame, text="Calculate", command=update_treeviews)
#     update_button.grid(row=1, column=1, padx=(0, 0), pady=(0, 5), sticky='w')
#
#     # Create an Export button
#     export_button = ttk.Button(scrollable_frame, text="Export", command=export_sku_strat_affinity)
#     export_button.grid(row=2, column=1, padx=(0, 0), pady=(0, 5), sticky='w')
#
#     # SKU Stratification Label
#     strat_label = ttk.Label(scrollable_frame, text="SKU Stratification - (Order Type: All)",
#                             font=('Helvetica', 16, 'bold'))
#     strat_label.grid(row=3, columnspan=2, padx=(0, 5), pady=(10, 5), sticky='w')
#     #
#     # # Calculate SKU Stratification (no filters, default settings)
#     # skustrat_mysql_tbl_name = f"{std_table_name}_SKUSTRAT_All"
#     # engine = create_mysql_engine()
#     # global_df_stratification = pd.read_sql(f""" SELECT * FROM {skustrat_mysql_tbl_name}""", engine)
#     # # add_mysql_tbl_name_to_dict(skustrat_mysql_tbl_name, std_table_name, channel='ALL', start_date='', end_date='',
#     # #                            custom_input=default_ABC_split)
#     # # sku_type_mysql_tbl_name = f"{std_table_name}_SKUTYPES_All"
#     # # add_mysql_tbl_name_to_dict(sku_type_mysql_tbl_name, std_table_name, channel='ALL', start_date='', end_date='',
#     # #                            custom_input=default_ABC_split)
#     # # Format columns
#     # global_df_stratification = format_qty(global_df_stratification, columns=['SKUs', 'Orders', 'Lines', 'Units'])
#     # global_df_stratification = format_percentages(global_df_stratification,
#     #                                               percent_columns=['% SKUs', '% Orders', '% Lines', '% Units'])
#
#     # Create Treeview for SKU Stratification
#     strat_cols = ['SKU Types', 'SKUs', '% SKUs', 'Orders', '% Orders', 'Lines', '% Lines', 'Units',   '% Units']
#     tree_stratification = ttk.Treeview(scrollable_frame, columns=strat_cols,
#                                        show='headings')
#     for col in strat_cols:
#         tree_stratification.heading(col, text=col)
#         tree_stratification.column(col, anchor='center')
#     tree_stratification.grid(row=4, columnspan=2, sticky='nsew')
#     # # Update SKU Stratification Treeview
#     # for index, row in global_df_stratification.iterrows():
#     #     tree_stratification.insert('', 'end', values=list(row))
#
#     # SKU Affinity Label
#     affinity_label = ttk.Label(scrollable_frame, text="SKU Affinity - (Order Type: All)",
#                                font=('Helvetica', 16, 'bold'))
#     affinity_label.grid(row=7, columnspan=2, padx=(0, 5), pady=(10, 5), sticky='w')
#
#     # # Calculate SKU Affinity (no filters, default settings)
#     # skuaff_mysql_tbl_name = f"{std_table_name}_SKUAFF_All"
#     # global_df_affinity = pd.read_sql(f""" SELECT * FROM {skuaff_mysql_tbl_name}""", engine)
#     # # add_mysql_tbl_name_to_dict(skuaff_mysql_tbl_name, std_table_name, channel='ALL', start_date='', end_date='',
#     # #                            custom_input=default_ABC_split)
#     # #engine.disclose()
#     #
#     # # Format columns
#     # global_df_affinity = format_qty(global_df_affinity, columns=['SKUs', 'Orders', 'Lines', 'Units'])
#     # global_df_affinity = format_percentages(global_df_affinity,
#     #                                         percent_columns=['% SKUs', '% Orders', '% Lines', '% Units'])
#
#     # Create Treeview for SKU Affinity
#     aff_cols = ['Order Types', 'SKUs', 'Orders', 'Lines', 'Units','% SKUs', '% Orders', '% Lines', '% Units']
#     tree_affinity = ttk.Treeview(scrollable_frame, columns=aff_cols, show='headings')
#     for col in aff_cols:
#         tree_affinity.heading(col, text=col)
#         tree_affinity.column(col, anchor='center')
#     tree_affinity.grid(row=8, columnspan=2, sticky='nsew')
#
#     # # Update SKU Affinity Treeview
#     # for index, row in global_df_affinity.iterrows():
#     #     tree_affinity.insert('', 'end', values=list(row))
#
#     # Configure weights for rows and columns
#     scrollable_frame.grid_rowconfigure(4, weight=1)  # For SKU Stratification Treeview
#     # scrollable_frame.grid_rowconfigure(6, weight=1)  # For SKU Variance Treeview
#     # scrollable_frame.grid_rowconfigure(8, weight=1)  # For SKU Variance Treeview
#     scrollable_frame.grid_columnconfigure(0, weight=1)
#     scrollable_frame.grid_columnconfigure(1, weight=1)
#
#     # # Bind combobox selection changes to update the Treeviews
#     # bu_combobox.bind("<<ComboboxSelected>>", lambda e: update_treeviews())
#     # dc_combobox.bind("<<ComboboxSelected>>", lambda e: update_treeviews())
#     # ordertype_selection.bind("<<ComboboxSelected>>", lambda e: update_treeviews())
#
#     # Pack the scrollable frame
#     scrollable_frame.pack(fill='both', expand=True)
#
#
# def update_treeviews():
#     # global global_df_stratification, global_df_variance, global_df_affinity  # Reference the global DataFrames
#
#     # Clear existing entries in the Stratification Treeview
#     for item in tree_stratification.get_children():
#         tree_stratification.delete(item)
#         strat_label.config(text="SKU Stratification")
#
#     # Clear SKU Affinity Treeview
#     for item in tree_affinity.get_children():
#         tree_affinity.delete(item)
#         affinity_label.config(text="SKU Affinity")
#
#     # Get selected values
#     # selected_bu = bu_combobox.get()
#     # selected_dc = dc_combobox.get()
#     input_ordertype = ordertype_selection.get()
#
#     # Calculate SKU Stratification (no filters)
#     skustrat_mysql_tbl_name = f"{std_table_name}_SKUSTRAT_{input_ordertype}"
#     if check_mysql_table_exists_in_db(skustrat_mysql_tbl_name) == False:
#         tk.messagebox.showwarning("Warning", f"SKU Stratification and SKU Affinity tables for {std_table_name} have not been created in the database yet.")
#         return
#     engine = create_mysql_engine()
#     global_df_stratification = pd.read_sql(f""" SELECT * FROM {skustrat_mysql_tbl_name}""", engine)
#
#     # Format columns
#     global_df_stratification = format_qty(global_df_stratification, columns=['SKUs', 'Orders', 'Lines', 'Units'])
#     global_df_stratification = format_percentages(global_df_stratification,
#                                                   percent_columns=['% SKUs', '% Orders', '% Lines', '% Units'])
#
#     # Update SKU Stratification Label
#     applied_filters_label = f"SKU Stratification - (Order Type: {input_ordertype})"
#     strat_label.config(text=applied_filters_label)
#     # Update SKU Stratification Treeview
#     for index, row in global_df_stratification.iterrows():
#         tree_stratification.insert('', 'end', values=list(row))
#
#     # # Update SKU Variance Treeview
#     # filtered_variance = global_df_variance[
#     #     (global_df_variance['Business_Unit'] == selected_bu) &
#     #     (global_df_variance['DC'] == selected_dc) &
#     #     (global_df_variance['Segment'] == input_ordertype)
#     # ]
#     #
#     # # Clear existing entries in the Variance Treeview
#     # for item in tree_variance.get_children():
#     #     tree_variance.delete(item)
#     #
#     # # Insert filtered data into the Variance Treeview
#     # for index, row in filtered_variance.iterrows():
#     #     tree_variance.insert('', 'end', values=list(row[3:]))  # Adjust index as needed
#
#     # Calculate SKU Affinity
#     skuaff_mysql_tbl_name = f"{std_table_name}_SKUAFF_{input_ordertype}"
#     global_df_affinity = pd.read_sql(f""" SELECT * FROM {skuaff_mysql_tbl_name}""", engine)
#     #engine.disclose()
#     # Format columns
#     global_df_affinity = format_qty(global_df_affinity, columns=['SKUs', 'Units', 'Lines', 'Orders'])
#     global_df_affinity = format_percentages(global_df_affinity,
#                                             percent_columns=['% SKUs', '% Units', '% Lines', '% Orders'])
#     # Update SKU Affinity Label
#     applied_filters_label = f"SKU Affinity - (Order Type: {input_ordertype})"
#     affinity_label.config(text=applied_filters_label)
#     # Update SKU Affinity Treeview
#     for index, row in global_df_affinity.iterrows():
#         tree_affinity.insert('', 'end', values=list(row))

def validate_date_input(event):
    value = startdate_entry_custom.get()
    date_pattern = r'^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/\d{4}$'
    if value and not re.match(date_pattern, value):
        # You can choose to show a message or clear the entry
        tk.messagebox.showwarning("Invalid Date", "Please enter a valid date in mm/dd/yyyy format.")
        startdate_entry_custom.delete(0, tk.END)  # Clear the entry if invalid
        # Optionally, focus back on the entry
    earliest_date, latest_date = get_date_range(std_table_name)
    if value:
        try:
            datetime_val = datetime.strptime(value, '%m/%d/%Y').date()
        except ValueError:
            tk.messagebox.showwarning("Invalid Date", f"{value} is not a valid date.")
            enddate_entry_custom.delete(0, tk.END)  # Clear the entry if invalid
        else:
            if datetime_val < earliest_date or datetime_val > latest_date:
                # You can choose to show a message or clear the entry
                tk.messagebox.showwarning("Invalid Date", "Please enter a date within the data's date range.")
                startdate_entry_custom.delete(0, tk.END)  # Clear the entry if invalid
    #startdate_entry.focus_set()


def validate_date_input2(event):
    value = enddate_entry_custom.get()
    date_pattern = r'^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/\d{4}$'
    if value and not re.match(date_pattern, value):
        # You can choose to show a message or clear the entry
        tk.messagebox.showwarning("Invalid Date", "Please enter a valid date in mm/dd/yyyy format.")
        enddate_entry_custom.delete(0, tk.END)  # Clear the entry if invalid
        # Optionally, focus back on the entry
    earliest_date, latest_date = get_date_range(std_table_name)
    if value:
        try:
            datetime_val = datetime.strptime(value, '%m/%d/%Y').date()
        except ValueError:
            tk.messagebox.showwarning("Invalid Date", f"{value} is not a valid date.")
            enddate_entry_custom.delete(0, tk.END)  # Clear the entry if invalid
        else:
            if datetime_val < earliest_date or datetime_val > latest_date:
                # You can choose to show a message or clear the entry
                tk.messagebox.showwarning("Invalid Date", "Please enter a date within the data's date range.")
                enddate_entry_custom.delete(0, tk.END)  # Clear the entry if invalid
    # enddate_entry.focus_set()


def validate_date_input3(event):
    value = start_date_entry.get()
    date_pattern = r'^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/\d{4}$'
    if value and not re.match(date_pattern, value):
        # You can choose to show a message or clear the entry
        tk.messagebox.showwarning("Invalid Date", "Please enter a valid date in mm/dd/yyyy format.")
        start_date_entry.delete(0, tk.END)  # Clear the entry if invalid
        # Optionally, focus back on the entry
    # enddate_entry.focus_set()


def validate_date_input4(event):
    value = end_date_entry.get()
    date_pattern = r'^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/\d{4}$'
    if value and not re.match(date_pattern, value):
        # You can choose to show a message or clear the entry
        tk.messagebox.showwarning("Invalid Date", "Please enter a valid date in mm/dd/yyyy format.")
        end_date_entry.delete(0, tk.END)  # Clear the entry if invalid
        # Optionally, focus back on the entry
    # enddate_entry.focus_set()


def validate_percentage_input(event, entry):
    value = entry.get()
    # Check if the value is a valid number between 0 and 100
    if value == "":
        return  # Allow empty input
    try:
        num_value = float(value)
        if 0 <= num_value <= 100:
            return True
        else:
            raise ValueError
    except ValueError:
        tk.messagebox.showwarning("Invalid Input", "Please enter a number between 0 and 100.")
        entry.delete(0, tk.END)  # Clear the entry field


def toggle_custom_entries():
    mode = custom_mode.get()
    custom_split_labels = [aplus_label_custom, aonly_label_custom, bonly_label_custom, conly_label_custom]
    custom_split_entries = [aplus_entry_custom, aonly_entry_custom, bonly_entry_custom, conly_entry_custom]
    if mode == "top_skus":
        for label, entry in zip(custom_split_labels, custom_split_entries):
            label.grid_remove()
            entry.delete(0, tk.END)  # clear entry value
            entry.grid_remove()
        top_label_custom.grid(row=2, column=4, padx=(0, 0), pady=(5, 5), sticky='e')
        top_entry_custom.grid(row=2, column=5, padx=(0, 15), pady=(5, 5), sticky='w')
    if mode == "custom_split":
        top_label_custom.grid_remove()
        top_entry_custom.delete(0, tk.END)  # clear entry value
        top_entry_custom.grid_remove()
        aplus_label_custom.grid(row=2, column=5, padx=(0, 0), pady=(5, 5), sticky='e')
        aplus_entry_custom.grid(row=2, column=6, padx=(0, 15), pady=(5, 5), sticky='w')
        aonly_label_custom.grid(row=3, column=5, padx=(0, 0), pady=(5, 5), sticky='e')
        aonly_entry_custom.grid(row=3, column=6, padx=(0, 15), pady=(5, 5), sticky='w')
        bonly_label_custom.grid(row=4, column=5, padx=(0, 0), pady=(5, 5), sticky='e')
        bonly_entry_custom.grid(row=4, column=6, padx=(0, 15), pady=(5, 5), sticky='w')
        conly_label_custom.grid(row=5, column=5, padx=(0, 0), pady=(5, 5), sticky='e')
        conly_entry_custom.grid(row=5, column=6, padx=(0, 15), pady=(5, 5), sticky='w')

def toggle_modes():
    mode = selected_mode.get()
    if mode == "custom_mode":
        custom_mode_label.grid(row=1, column=4, padx=(0, 0), pady=(5, 5), sticky='w')
        top_radiobutton.grid(row=1, column=5, padx=(0, 15), pady=(5, 5), sticky='w')
        customsplit_radiobutton.grid(row=1, column=6, padx=(0, 100), pady=(5, 5), sticky='w')
        toggle_custom_entries()
    if mode == "regular_mode":
        custom_mode_label.grid_remove()
        top_radiobutton.grid_remove()
        customsplit_radiobutton.grid_remove()
        try:
            custom_split_labels = [aplus_label_custom, aonly_label_custom, bonly_label_custom, conly_label_custom]
            custom_split_entries = [aplus_entry_custom, aonly_entry_custom, bonly_entry_custom, conly_entry_custom]
            for label, entry in zip(custom_split_labels, custom_split_entries):
                label.grid_remove()
                entry.delete(0, tk.END)  # clear entry value
                entry.grid_remove()
            top_label_custom.grid_remove()
            top_entry_custom.delete(0, tk.END)  # clear entry value
            top_entry_custom.grid_remove()
        except NameError:
            return

def create_sku_strat_aff_tab(notebook):
    # Style customization
    style = ttk.Style()
    style.theme_use('clam')
    style.configure("Treeview", rowheight=25, background="white", foreground="black")
    style.configure("Treeview.Heading", font=('Helvetica', 12, 'bold'), background="#FF3B00", foreground="white")

    sku_strat_aff_tab = ttk.Frame(notebook)
    notebook.add(sku_strat_aff_tab, text="SKU Stratification & Affinity")
    global global_df_stratification_custom, global_df_affinity_custom, tree_stratification_custom, tree_affinity_custom
    global ordertype_entry_custom, bu_entry_custom, dc_entry_custom, startdate_entry_custom, enddate_entry_custom,\
        top_entry_custom, aplus_entry_custom, aonly_entry_custom, bonly_entry_custom, conly_entry_custom,\
        datecol_entry_custom, custom_mode, selected_mode
    global top_label_custom, aplus_label_custom, aonly_label_custom, bonly_label_custom, conly_label_custom, \
        strat_label_custom, affinity_label_custom, custom_mode_label, top_radiobutton, customsplit_radiobutton
    global std_table_name
    std_table_name = Shared.project

    # Create a frame for the input fields
    input_frame = ttk.Frame(sku_strat_aff_tab)
    input_frame.grid(row=0, column=0, columnspan=6, padx=(5, 5), pady=(5, 5), sticky='ew')

    # Input Fields
    # Order Type Filter Label and Entry
    ordertype_label = ttk.Label(input_frame, text="Select Order Type:")
    ordertype_label.grid(row=0, column=0, padx=(150, 0), pady=(5, 5), sticky='e')
    # ordertype_options = get_distinct_col_values(std_table_name, 'Order_Type')
    ordertype_options = Shared.channel_names_outbound # get_distinct_channel_filter_outbound()
    ordertype_entry_custom = ttk.Combobox(input_frame, values=ordertype_options)
    ordertype_entry_custom.set(ordertype_options[0])
    ordertype_entry_custom.grid(row=0, column=1, padx=(0, 100), pady=(5, 5), sticky='w')

    # Business Unit Filter Label and Entry
    bu_label = ttk.Label(input_frame, text="Select Business Unit:")
    bu_label.grid(row=1, column=0, padx=(150, 0), pady=(5, 5), sticky='e')
    # bu_options = get_distinct_col_values(std_table_name, 'Business_Unit')
    bu_options = Shared.bu_names_outbound # get_distinct_bu_filter_outbound()
    bu_entry_custom = ttk.Combobox(input_frame, values=bu_options)
    bu_entry_custom.set(bu_options[0])
    bu_entry_custom.grid(row=1, column=1, padx=(0, 100), pady=(5, 5), sticky='w')

    # DC Filter Label and Entry
    dc_label = ttk.Label(input_frame, text="Select DC:")
    dc_label.grid(row=2, column=0, padx=(150, 0), pady=(5, 5), sticky='e')
    # dc_options = get_distinct_col_values(std_table_name, 'DC_Name')
    dc_options = Shared.dc_names_outbound #get_distinct_dc_names_outbound()
    dc_entry_custom = ttk.Combobox(input_frame, values=dc_options)
    dc_entry_custom.set(dc_options[0])
    dc_entry_custom.grid(row=2, column=1, padx=(0, 100), pady=(5, 5), sticky='w')

    # # Date Column Label and Entry
    # datecol_label = ttk.Label(input_frame, text="Select Date Column:")
    # datecol_label.grid(row=0, column=2)

    # datecol_options = ['Order_Date', 'Order_Date', 'Delivery_Date']
    # datecol_entry = ttk.Combobox(input_frame, values=datecol_options)
    # datecol_entry.set(datecol_options[0])
    # datecol_entry.grid(row=0, column=3, padx=(0, 100), pady=(5, 5), sticky='w')

    # Date Range Label
    #earliest_date, latest_date = get_date_range(std_table_name, 'Order_Date')
    earliest_date = Shared.min_date_sql_outbound
    latest_date = Shared.max_date_sql_outbound
    daterange_label = ttk.Label(input_frame, text="Date Range: ")
    daterange_label.grid(row=0, column=2)
    earliest_date_formatted = earliest_date.strftime("%m/%d/%Y")
    latest_date_formatted = latest_date.strftime("%m/%d/%Y")
    daterange_label2 = ttk.Label(input_frame, text=f"{earliest_date_formatted} to {latest_date_formatted}")
    daterange_label2.grid(row=0, column=3, padx=(0, 100))


    # Start Date Label and Entry
    startdate_label = ttk.Label(input_frame, text="Start Date (mm/dd/yyyy):")
    startdate_label.grid(row=1, column=2, padx=(5, 0), pady=(5, 5), sticky='e')
    startdate_entry_custom = ttk.Entry(input_frame)
    startdate_entry_custom.grid(row=1, column=3, padx=(0, 100), pady=(5, 5), sticky='w')
    # Bind the Return key to validate the input
    startdate_entry_custom.bind('<FocusOut>', validate_date_input)

    # End Date Label and Entry
    enddate_label = ttk.Label(input_frame, text="End Date (mm/dd/yyyy):")
    enddate_label.grid(row=2, column=2, padx=(0, 0), pady=(5, 5), sticky='e')
    enddate_entry_custom = ttk.Entry(input_frame)
    enddate_entry_custom.grid(row=2, column=3, padx=(0, 0), pady=(5, 5), sticky='w')
    enddate_entry_custom.bind('<FocusOut>', validate_date_input2)

    # Select Mode Label and Radio Button
    selected_mode = tk.StringVar()
    select_mode_label = ttk.Label(input_frame, text="Select SKU Stratification Option:")
    select_mode_label.grid(row=0, column=4, padx=(0, 0), pady=(5, 5), sticky='w')
    regular_radiobutton = ttk.Radiobutton(input_frame, text="Regular", variable=selected_mode, value="regular_mode",
                                      command=toggle_modes)
    regular_radiobutton.grid(row=0, column=5, padx=(0, 15), pady=(5, 5), sticky='w')
    custom_radiobutton = ttk.Radiobutton(input_frame, text="Custom", variable=selected_mode,
                                              value="custom_mode", command=toggle_modes)
    custom_radiobutton.grid(row=0, column=6, padx=(0, 100), pady=(5, 5), sticky='w')

    # Custom Mode Label and Radio Buttons
    custom_mode = tk.StringVar()
    custom_mode_label = ttk.Label(input_frame, text="Select Custom Option:")
    top_radiobutton = ttk.Radiobutton(input_frame, text="Top # SKUs as A+", variable=custom_mode, value="top_skus",
                                      command=toggle_custom_entries)
    customsplit_radiobutton = ttk.Radiobutton(input_frame, text="Custom % Split", variable=custom_mode,
                                              value="custom_split", command=toggle_custom_entries)

    regular_radiobutton.invoke()

    # Top Label and Entry
    top_label_custom = ttk.Label(input_frame, text="Top # of SKUs as A+:")
    top_entry_custom = ttk.Entry(input_frame)

    # Top Label and Entry for A+
    aplus_label_custom = ttk.Label(input_frame, text="A+ (%):")
    aplus_entry_custom = ttk.Entry(input_frame)
    aplus_entry_custom.bind('<FocusOut>', lambda event: validate_percentage_input(event, aplus_entry_custom))

    # A Only Label and Entry
    aonly_label_custom = ttk.Label(input_frame, text="A (%):")
    aonly_entry_custom = ttk.Entry(input_frame)
    aonly_entry_custom.bind('<FocusOut>', lambda event: validate_percentage_input(event, aonly_entry_custom))

    # B Only Label and Entry
    bonly_label_custom = ttk.Label(input_frame, text="B (%):")
    bonly_entry_custom = ttk.Entry(input_frame)
    bonly_entry_custom.bind('<FocusOut>', lambda event: validate_percentage_input(event, bonly_entry_custom))

    # C Only Label and Entry
    conly_label_custom = ttk.Label(input_frame, text="C (%):")
    conly_entry_custom = ttk.Entry(input_frame)
    conly_entry_custom.bind('<FocusOut>', lambda event: validate_percentage_input(event, conly_entry_custom))

    # Button to update SKU Stratification treeview based on input
    update_sku_strat_button = ttk.Button(input_frame, text="Calculate SKU Stratification",
                                         command=update_sku_strat_treeviews_custom)
    update_sku_strat_button.grid(row=0, column=8, rowspan=2, columnspan=6, padx=(0, 5), pady=(10, 5))

    # Button to update SKU Affinity treeview based on input
    update_sku_affinity_button = ttk.Button(input_frame, text="Calculate SKU Affinity",
                                            command=update_sku_affinity_treeviews_custom)
    update_sku_affinity_button.grid(row=1, column=8, rowspan=2, columnspan=6, padx=(0, 5), pady=(10, 5))

    # Button to export treeviews based on input
    export_button = ttk.Button(input_frame, text="Export", command=export_sku_strat_affinity_custom)
    export_button.grid(row=2, column=8, rowspan=2, columnspan=6, padx=(0, 5), pady=(30, 5))

    # SKU Stratification Label
    strat_label_custom = ttk.Label(sku_strat_aff_tab, text="SKU Stratification", font=('Helvetica', 16, 'bold'))
    strat_label_custom.grid(row=1, column=0, columnspan=6, padx=(0, 5), pady=(10, 5), sticky='w')

    # Create Treeview for SKU Stratification
    strat_cols = ['SKU Types', 'SKUs', '% SKUs', 'Orders', '% Orders', 'Lines', '% Lines', 'Units', '% Units']
    tree_stratification_custom = ttk.Treeview(sku_strat_aff_tab, columns=strat_cols, show='headings')
    for col in strat_cols:
        tree_stratification_custom.heading(col, text=col)
        tree_stratification_custom.column(col, anchor='center')
    tree_stratification_custom.grid(row=2, column=0, columnspan=6, sticky='nsew')  # Use columnspan here

    # SKU Affinity Label
    affinity_label_custom = ttk.Label(sku_strat_aff_tab, text="SKU Affinity", font=('Helvetica', 16, 'bold'))
    affinity_label_custom.grid(row=4, column=0, columnspan=6, padx=(0, 5), pady=(10, 5), sticky='w')

    # Create Treeview for SKU Affinity
    aff_cols = ['Order Types', 'SKUs', '% SKUs', 'Orders', '% Orders', 'Lines', '% Lines', 'Units',   '% Units']
    tree_affinity_custom = ttk.Treeview(sku_strat_aff_tab, columns=aff_cols, show='headings')
    for col in aff_cols:
        tree_affinity_custom.heading(col, text=col)
        tree_affinity_custom.column(col, anchor='center')
    tree_affinity_custom.grid(row=5, column=0, columnspan=6, sticky='nsew')

    # Configure the grid weights
    for col in range(6):
        sku_strat_aff_tab.grid_columnconfigure(col, weight=1)  # Allows columns to expand equally


def update_sku_strat_treeviews_custom():
    global sku_type_mysql_tbl_name_strat_custom, global_df_stratification_custom

    # Clear SKU Stratification Treeview
    for item in tree_stratification_custom.get_children():
        tree_stratification_custom.delete(item)
    # Clear SKU Affinity Treeview
    for item in tree_affinity_custom.get_children():
        tree_affinity_custom.delete(item)
        affinity_label_custom.config(text="SKU Affinity")

    # Get input values
    # input_datecol = datecol_entry.get()
    input_datecol = 'Order_Date'
    input_startdate = startdate_entry_custom.get()
    input_enddate = enddate_entry_custom.get()
    input_ordertype = ordertype_entry_custom.get()
    input_bu = bu_entry_custom.get()
    input_dc = dc_entry_custom.get()
    if custom_mode.get() == "top_skus":
        input_top = top_entry_custom.get()
    if custom_mode.get() == "custom_split":
        input_aplus = aplus_entry_custom.get()
        input_aonly = aonly_entry_custom.get()
        input_bonly = bonly_entry_custom.get()
        input_conly = conly_entry_custom.get()
    input_uom = 'lines'  # TODO: add option for sku strat based on units?

    # Validation - Check if both startdate and enddate inputs have been entered
    if (input_startdate != '' and input_enddate == '') or (input_startdate == '' and input_enddate != ''):
        tk.messagebox.showwarning("Invalid Input",
                                  "Please enter dates for both Start Date and End Date.")
        return


    # Format date inputs from 'MM/DD/YYYY' to 'YYYY-MM-DD'
    if input_startdate != '':
        input_startdate = convert_date_format(input_startdate)
    if input_enddate != '':
        input_enddate = convert_date_format(input_enddate)
    # Format Top N SKUs input
    if custom_mode.get() == "top_skus" and input_top != '':
        input_top = int(input_top)

    # Validation - Check % split totals and store custom percentage split inputs in a dictionary
    if custom_mode.get() == "custom_split":
        pct_split_inputs = [input_aplus, input_aonly, input_bonly, input_conly]
        num_blank_inputs = sum(pct_input == '' for pct_input in pct_split_inputs)
        try:
            # if no inputs for custom % split, use the default {'A': 80%, 'B': 15%, 'C': 5%}
            if num_blank_inputs == 4:
                strat_pct_split = default_ABC_split
            # if all inputs have been entered, create dictionary for the custom % split
            elif num_blank_inputs == 0:
                strat_pct_split = {'A+': float(input_aplus), 'A': float(input_aonly), 'B': float(input_bonly),
                                   'C': float(input_conly)}
            # if missing some inputs, raise error
            else:
                raise ValueError
        except ValueError:
            tk.messagebox.showwarning("Invalid Input",
                                      "Please enter values for all SKU Types if using custom percentage splits.")
            return
        # Validate that the percentage inputs add up to 100%
        try:
            if sum(strat_pct_split.values()) != 100:
                raise ValueError
        except ValueError:
            tk.messagebox.showwarning("Invalid Input",
                                      f"The entered percentage splits {strat_pct_split} do not add up to 100%.")
            return
    else:
        strat_pct_split = default_ABC_split

    # Calculate SKU Stratification
    if custom_mode.get() == "top_skus" and input_top != '':
        try:
            global_df_stratification_custom, sku_type_mysql_tbl_name_strat_custom = sku_stratification_top_N(
                std_table_name, number_of_skus=input_top, split=strat_pct_split, channel=input_ordertype, bu=input_bu,
                dc=input_dc, date_column=input_datecol, start_date=input_startdate, end_date=input_enddate)
        except ValueError:
            tk.messagebox.showwarning("Invalid Input",
                                      f"The top {input_top} SKUs make up over 80% of the total {input_uom}. Please enter a lower number.")
            strat_label_custom.config(text='SKU Stratification')
            return
    else:
        (global_df_stratification_custom,
         sku_type_mysql_tbl_name_strat_custom) = sku_stratification(std_table_name,
                                                                    split=strat_pct_split,
                                                                    channel=input_ordertype,
                                                                    bu=input_bu,
                                                                    dc=input_dc,
                                                                    date_column=input_datecol,
                                                                    start_date=input_startdate,
                                                                    end_date=input_enddate)
    # Format columns
    global_df_stratification_custom = format_qty(global_df_stratification_custom,
                                                 columns=['SKUs', 'Units', 'Lines', 'Orders'])
    global_df_stratification_custom = format_percentages(global_df_stratification_custom,
                                                         percent_columns=['% SKUs', '% Units', '% Lines', '% Orders'])

    # Update SKU Stratification Label
    date_str = ""
    if input_startdate != '' and input_enddate != '':
        date_str = f", Date Range: {input_startdate} to {input_enddate}"
    custom_str = ""
    if custom_mode.get() == "top_skus" and input_top != '':
        custom_str = f", Top {input_top} SKUs as A+"
    if custom_mode.get() == "custom_split" and strat_pct_split != default_ABC_split:
        split_formatted = str(strat_pct_split).replace("'", "").replace(',', '%,').replace('}', '%}')
        custom_str = f", Custom % Split: {split_formatted}"
    applied_filters_label = (f"SKU Stratification - (Order Type: {input_ordertype}, Business Unit: {input_bu}, DC: {input_dc}"
                             f"{date_str}{custom_str})")
    strat_label_custom.config(text=applied_filters_label)

    # Update SKU Stratification Treeview
    for index, row in global_df_stratification_custom.iterrows():
        tree_stratification_custom.insert('', 'end', values=list(row))


def update_sku_affinity_treeviews_custom():
    # Clear SKU Affinity Treeview
    for item in tree_affinity_custom.get_children():
        tree_affinity_custom.delete(item)
        affinity_label_custom.config(text="SKU Affinity")

    # Get input values
    #input_datecol = datecol_entry.get()
    input_datecol = 'Order_Date'
    input_startdate = startdate_entry_custom.get()
    input_enddate = enddate_entry_custom.get()
    input_ordertype = ordertype_entry_custom.get()
    input_bu = bu_entry_custom.get()
    input_dc = dc_entry_custom.get()
    custom_input = default_ABC_split
    if custom_mode.get() == "top_skus":
        input_top = top_entry_custom.get()
        if input_top != "":
            custom_input = int(input_top)
    if custom_mode.get() == "custom_split":
        input_aplus = aplus_entry_custom.get()
        input_aonly = aonly_entry_custom.get()
        input_bonly = bonly_entry_custom.get()
        input_conly = conly_entry_custom.get()
        pct_split_inputs = [input_aplus, input_aonly, input_bonly, input_conly]
        num_blank_inputs = sum(pct_input == '' for pct_input in pct_split_inputs)
        if num_blank_inputs == 0:
            strat_pct_split = {'A+': float(input_aplus), 'A': float(input_aonly), 'B': float(input_bonly),
                               'C': float(input_conly)}
            custom_input = strat_pct_split
        else:
            strat_pct_split = default_ABC_split

    # Check if both startdate and enddate inputs have been entered
    if (input_startdate != '' and input_enddate == '') or (input_startdate == '' and input_enddate != ''):
        tk.messagebox.showwarning("Invalid Input",
                                  "Please enter dates for both Start Date and End Date.")
        return
    # Format date inputs from mm/dd/yyyy to yyyy-mm-dd
    if input_startdate != '':
        input_startdate = input_startdate[6:] + '-' + input_startdate[:2] + '-' + input_startdate[3:5]
    if input_enddate != '':
        input_enddate = input_enddate[6:] + '-' + input_enddate[:2] + '-' + input_enddate[3:5]

    # Calculate SKU Affinity
    # if custom_mode.get() == "top_skus" and input_top != "":
    #     custom_input = int(input_top)
    # elif custom_mode.get() == "custom_split":
    #     custom_input = strat_pct_split
    # else:
    #     custom_input = default_ABC_split

    sku_type_mysql_tbl_name_aff = get_mysql_tbl_name('SKUTYPE', std_table_name, channel=input_ordertype,
                                                     bu=input_bu, dc=input_dc, start_date=input_startdate,
                                                     end_date=input_enddate, custom_input=custom_input)
    # print(f"sku_type_mysql_tbl_name_aff : {sku_type_mysql_tbl_name_aff}")
    try:
        if sku_type_mysql_tbl_name_aff != sku_type_mysql_tbl_name_strat_custom:
            raise ValueError
        else:
            global_df_affinity_custom = sku_affinity(global_df_stratification_custom, sku_type_mysql_tbl_name_aff,
                                                     std_table_name, channel=input_ordertype, bu=input_bu, dc=input_dc,
                                                     date_column=input_datecol, start_date=input_startdate,
                                                     end_date=input_enddate, custom_input=custom_input)
    except NameError as e:
        # print(e)
        tk.messagebox.showwarning("Error", "SKU Stratification must be calculated before SKU Affinity.")
        return
    except ValueError as e:
        tk.messagebox.showwarning("Error",
                                  "Filters and settings have changed, please recalculate SKU Stratification first.")
        return

    # Format columns
    global_df_affinity_custom = format_qty(global_df_affinity_custom, columns=['SKUs', 'Units', 'Lines', 'Orders'])
    global_df_affinity_custom = format_percentages(global_df_affinity_custom,
                                                   percent_columns=['% SKUs', '% Units', '% Lines', '% Orders'])
    # Update SKU Affinity Label
    date_str = ""
    if input_startdate != '' and input_enddate != '':
        date_str = f", Date Range: {input_startdate} to {input_enddate}"
    custom_str = ""
    if custom_mode.get() == "top_skus" and input_top != '':
        custom_str = f", Top {input_top} SKUs as A+"
    if custom_mode.get() == "custom_split" and strat_pct_split != {'A': 80, 'B': 15, 'C': 5}:
        split_formatted = str(strat_pct_split).replace("'", "").replace(',', '%,').replace('}', '%}')
        custom_str = f", Custom % Split: {split_formatted}"
    applied_filters_label = (f"SKU Affinity - (Order Type: {input_ordertype}, Business Unit: {input_bu}, DC: {input_dc}"
                             f"{date_str}{custom_str})")
    affinity_label_custom.config(text=applied_filters_label)
    # Update SKU Affinity Treeview
    for index, row in global_df_affinity_custom.iterrows():
        tree_affinity_custom.insert('', 'end', values=list(row))

# ##### Create Tab 4 ##################

def create_pick_aff_tab(notebook):
    pick_aff_tab = ttk.Frame(notebook)
    notebook.add(pick_aff_tab, text="Pick Affinity")

    global global_df_pick, global_df_ib_pick
    global ordertype_entry, dc_combobox, tree_pick_data, tree_pick_variance
    global start_date_entry, end_date_entry

    # Assuming dfpick is already formatted
    # global_df_pick = format_qty(dfpick, columns=['Pallet_Picks', 'Orders', 'SKUs', 'Lines', 'Units', 'Layer_Picks',
    #                                              'Case_Picks', 'Each_Picks'])
    # global_df_pick = format_percentages(global_df_pick, percent_columns=['% of Orders'])

    # if dfpickIB is not None:
    #     global_df_ib_pick = format_qty(dfpickIB,
    #                                    columns=['Pallet_Picks', 'Orders', 'SKUs', 'Lines', 'Units', 'Layer_Picks',
    #                                             'Case_Picks', 'Each_Picks'])
    #     global_df_ib_pick = format_percentages(dfpickIB, percent_columns=['% of Orders'])

    # Style customization
    style = ttk.Style()
    style.theme_use('clam')
    style.configure("Treeview", rowheight=25, background="white", foreground="black")
    style.configure("Treeview.Heading", font=('Helvetica', 12, 'bold'), background="#FF3B00", foreground="white")

    # Main scrollable frame
    scrollable_frame = ttk.Frame(pick_aff_tab)
    scrollable_frame.pack(fill='both', expand=True)

    # Frame for filters and date entries
    filter_frame = ttk.Frame(scrollable_frame)
    filter_frame.grid(row=0, column=0, padx=10, pady=10, sticky='ew')

    # Configure weights for the filter frame's columns
    for i in range(10):
        filter_frame.grid_columnconfigure(i, weight=1)

    # Order Type Filter
    ordertype_label = ttk.Label(filter_frame, text="Select Order Type:")
    ordertype_label.grid(row=0, column=0, padx=(200, 5), pady=(0, 5), sticky='e')
    #ordertype_options = global_df_pick['order_type'].unique().tolist()
    ordertype_options = Shared.channel_names_outbound
    #ordertype_options = [item if item != "ALL" else "All" for item in ordertype_options]
    ordertype_options = [item for item in ordertype_options if item is not None]
    ordertype_entry = ttk.Combobox(filter_frame, values=ordertype_options)
    ordertype_entry.set('ALL')  # Default to 'ALL'
    ordertype_entry.grid(row=0, column=1, padx=(0, 5), pady=(0, 5), sticky='w')

    # DC Filter
    dc_label = ttk.Label(filter_frame, text="Select DC:")
    dc_label.grid(row=1, column=0, padx=(200, 5), pady=(0, 5), sticky='e')
    #dc_options = global_df_pick['dc_name'].unique().tolist()
    dc_options = Shared.dc_names_outbound
    #dc_options = [item if item != "ALL" else "All" for item in dc_options]
    dc_options = [item for item in dc_options if item is not None]
    dc_combobox = ttk.Combobox(filter_frame, values=dc_options)
    dc_combobox.set('ALL')  # Default to 'ALL'
    dc_combobox.grid(row=1, column=1, padx=(0, 5), pady=(0, 5), sticky='w')

    # Start Date Entry
    start_date_label = ttk.Label(filter_frame, text="Start Date:")
    start_date_label.grid(row=0, column=4, padx=(10, 5), pady=(0, 5), sticky='e')
    start_date_entry = ttk.Entry(filter_frame)
    start_date_entry.grid(row=0, column=5, padx=(0, 5), pady=(0, 5), sticky='w')
    start_date_entry.bind('<FocusOut>', validate_date_input3)

    # End Date Entry
    end_date_label = ttk.Label(filter_frame, text="End Date:")
    end_date_label.grid(row=1, column=4, padx=(10, 5), pady=(0, 5), sticky='e')
    end_date_entry = ttk.Entry(filter_frame)
    end_date_entry.grid(row=1, column=5, padx=(0, 5), pady=(0, 5), sticky='w')
    end_date_entry.bind('<FocusOut>', validate_date_input4)

    # Calculate Button
    calculate_button = ttk.Button(filter_frame, text="Calculate", command=lambda: calculate_pick_data())
    calculate_button.grid(row=0, column=8, padx=(10, 5), pady=(0, 5))

    # Export Button
    export_button = ttk.Button(filter_frame, text="Export", command=lambda: export_pick_data())
    export_button.grid(row=0, column=9, padx=(5, 0), pady=(0, 5))

    # SKU Pick Data Label
    pick_data_label = ttk.Label(scrollable_frame, text="Outbound Pick Affinity", font=('Helvetica', 16, 'bold'))
    pick_data_label.grid(row=1, columnspan=2, padx=(0, 5), pady=(10, 5), sticky='w')

    # Date Range Label
    # earliest_date, latest_date = get_date_range(std_table_name, 'Order_Date')
    earliest_date = Shared.min_date_sql_outbound
    latest_date = Shared.max_date_sql_outbound
    daterange_label = ttk.Label(filter_frame, text="Date Range: ")
    daterange_label.grid(row=2, column=4)
    earliest_date_formatted = earliest_date.strftime("%m/%d/%Y")
    latest_date_formatted = latest_date.strftime("%m/%d/%Y")
    daterange_label2 = ttk.Label(filter_frame, text=f"{earliest_date_formatted} to {latest_date_formatted}")
    daterange_label2.grid(row=2, column=5, padx=(0, 100))

    # Create Treeview for SKU Pick Data
    columns_list = [
        "Pick Affinity", "Orders", "% of Orders", "Lines", "% of Lines",
        "SKUs", "Units","% of Units", "Pallet Picks", "Layer Picks", "Case Picks", "Each Picks"
    ]

    tree_pick_data = ttk.Treeview(scrollable_frame, columns=columns_list, show='headings')
    for col in columns_list:
        tree_pick_data.heading(col, text=col)
        tree_pick_data.column(col, anchor='center')

    # for col in global_df_pick.columns[2:]:
    #     tree_pick_data.heading(col, text=col)
    #     tree_pick_data.column(col, anchor='center')
    tree_pick_data.grid(row=2, columnspan=2, sticky='nsew')

    # # SKU Variance Label
    # variance_label = ttk.Label(scrollable_frame, text="Inbound Pick Affinity", font=('Helvetica', 16, 'bold'))
    # variance_label.grid(row=3, columnspan=2, padx=(0, 5), pady=(10, 5), sticky='w')

    # # Create Treeview for SKU Variance
    # tree_pick_variance = ttk.Treeview(scrollable_frame, columns=list(global_df_ib_pick.columns[1:]),
    #                                   show='headings')  # Using from column 2
    # for col in global_df_ib_pick.columns[1:]:
    #     tree_pick_variance.heading(col, text=col)
    #     tree_pick_variance.column(col, anchor='center')
    # tree_pick_variance.grid(row=4, columnspan=2, sticky='nsew')

    # Configure weights for rows and columns
    scrollable_frame.grid_rowconfigure(2, weight=1)  # For SKU Pick Data Treeview
    scrollable_frame.grid_rowconfigure(4, weight=1)  # For SKU Variance Treeview
    scrollable_frame.grid_columnconfigure(0, weight=1)
    scrollable_frame.grid_columnconfigure(1, weight=1)

    # Bind combobox selection changes to update the Treeviews
    ordertype_entry.bind("<<ComboboxSelected>>", lambda e: update_pick_treeviews())
    dc_combobox.bind("<<ComboboxSelected>>", lambda e: update_pick_treeviews())

    # Initial data display
    #update_pick_treeviews()

    # Pack the scrollable frame
    scrollable_frame.pack(fill='both', expand=True)


def export_pick_data():
    # Open a file dialog to save the DataFrame
    file_path = filedialog.asksaveasfilename(defaultextension=".csv",
                                             filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                                             title="Save Pick Data as CSV")
    if file_path:
        global_df_pick.to_csv(file_path, index=False)  # Save dfpick as CSV


def calculate_pick_data():
    global global_df_pick, global_stdt, global_enddt  # Reference the global DataFrame
    projtbl = f"client_data.{Shared.project}"
    # Create a new DataFrame with the updated data
    # Get values from entry boxes
    st_date_value = start_date_entry.get()
    end_date_value = end_date_entry.get()

    # Convert and set shared dates
    global_stdt = convert_date_format(st_date_value)
    global_enddt = convert_date_format(end_date_value)

    # Debugging: Print values to confirm
    #print(f"Start Date: {Shared.stdt}, End Date: {Shared.enddt}")
    PickAffinDates = f"""

     with Pickbase AS (
        SELECT
    	a.Order_Type,
        a.DC_Name,
        a.Order_Number,
        a.sku,
    	count(distinct(a.Order_Number)) Orders,
        -- count(distinct(a.order_number)) / SUM(count(distinct(a.order_number))) OVER (partition by a.order_type) AS '% of Orders',
        count(*) 'Lines',
        count(distinct(a.sku)) 'SKUs',
        sum(a.qty) 'Units',
        coalesce(sum(a.Pallet_Picks),0) 'Pallet_Picks',
        coalesce(sum(a.Layer_Picks),0) 'Layer_Picks',
        coalesce(sum(a.Case_Picks),0) 'Case_Picks',
        coalesce(sum(a.Each_Picks),0) 'Each_Picks'


    FROM
        -- ALPARGATAS_STD_OB_V1_09_11_24 a
         -- Haleon_OB_STD_090324 a
         -- Kent_Water_Sports_STD_OB_V1_071024 a
         {projtbl} a
     WHERE
     a.Order_Date between '{global_stdt}' and '{global_enddt}'

     GROUP BY
      a.order_type
      ,a.order_number
      ,a.DC_Name
      ,a.sku
      )

      SELECT
      a.order_type,
      a.dc_name,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
    	WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type, a.dc_name),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (partition by a.order_type, a.dc_name),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (partition by a.order_type, a.dc_name),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    GROUP BY
      a.order_type,
      a.dc_name,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END

      union

      SELECT
      a.order_type,
      'ALL' dc_name,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (partition by a.order_type),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (partition by a.order_type),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    GROUP BY
      a.order_type,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END

      union

      SELECT
      'ALL' order_type,
       a.dc_name,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.dc_name),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (partition by a.dc_name),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (partition by a.dc_name),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    GROUP BY
      a.dc_name,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END

       union

      SELECT
      'ALL' order_type,
       'ALL' dc_name,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    GROUP BY
    --   a.dc_name,
      CASE
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Each Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks = 0 THEN 'Case Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer & Case'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet Only'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer Only'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks = 0 THEN 'Pallet, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks = 0 AND a.Layer_Picks > 0 THEN 'Layer, Case & Each'
        WHEN a.Each_Picks > 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Each'
        WHEN a.Each_Picks = 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Layer, Pallet & Case'
        WHEN a.Each_Picks > 0 AND a.Case_Picks > 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Each, Case, Pallet & Layer'
        WHEN a.Each_Picks = 0 AND a.Case_Picks = 0 AND a.Pallet_Picks > 0 AND a.Layer_Picks > 0 THEN 'Pallet & Layer'
        ELSE 'No Picks'
      END

    union

    SELECT
      a.order_type,
      a.dc_name,
      'Total' AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type, a.dc_name),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (partition by a.order_type, a.dc_name),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (partition by a.order_type, a.dc_name),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    GROUP BY
      a.order_type,
      a.dc_name

      union

      SELECT
      a.order_type,
      'ALL' dc_name,
      'Total' AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (partition by a.order_type),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (partition by a.order_type),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    GROUP BY
      a.order_type

      union

      SELECT
      'ALL' order_type,
       a.dc_name,
      'Total' AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.dc_name),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (partition by a.dc_name),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (partition by a.dc_name),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    GROUP BY
      a.dc_name

       union

      SELECT
      'ALL' order_type,
       'ALL' dc_name,
      'Total' AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders',
      count(*) AS `Lines`,
      round(count(*)  / SUM(count(*) ) OVER (),3) AS '% of Lines',
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      round(SUM(a.units)  / SUM(SUM(a.units) ) OVER (),3) AS '% of Units',
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM
      Pickbase a
    """

    #print(PickAffinDates)

    # Update the global_df_pick with the new DataFrame
    global_df_pick = ExecuteQrys.DB_Pull5(PickAffinDates)

    global_df_pick = format_qty(global_df_pick,
                                columns=['Pallet_Picks', 'Orders', 'SKUs', 'Lines', 'Units', 'Layer_Picks',
                                         'Case_Picks', 'Each_Picks'])
    global_df_pick = format_percentages(global_df_pick, percent_columns=['% of Orders', '% of Lines', '% of Units'])

    # Refresh the Treeviews to reflect the new data
    update_pick_treeviews()

    # Optionally, refresh the ComboBoxes if their values depend on the updated DataFrame
    ordertype_entry['values'] = global_df_pick['order_type'].unique().tolist()
    dc_combobox['values'] = global_df_pick['dc_name'].unique().tolist()

    # Set default values if needed
    ordertype_entry.set('ALL')
    dc_combobox.set('ALL')


def update_pick_treeviews():
    global global_df_pick, global_df_ib_pick  # Reference the global DataFrames

    # Get selected values
    selected_ordertype = ordertype_entry.get()
    selected_dc = dc_combobox.get()

    # Update SKU Pick Data Treeview
    filtered_pick_data = global_df_pick[
        (global_df_pick['order_type'] == selected_ordertype) &
        (global_df_pick['dc_name'] == selected_dc)
        ]

    # Clear existing entries in the Pick Data Treeview
    for item in tree_pick_data.get_children():
        tree_pick_data.delete(item)



    # Insert filtered data into the Pick Data Treeview
    for index, row in filtered_pick_data.iterrows():
        tree_pick_data.insert('', 'end', values=list(row[2:]))  # Adjust index as needed

    # # Update SKU Variance Treeview
    # filtered_variance = global_df_ib_pick[
    #     (global_df_ib_pick['dc_name'] == selected_dc)
    # ]
    #
    # # Clear existing entries in the Variance Treeview
    # for item in tree_pick_variance.get_children():
    #     tree_pick_variance.delete(item)
    #
    # # Insert filtered data into the Variance Treeview
    # for index, row in filtered_variance.iterrows():
    #     tree_pick_variance.insert('', 'end', values=list(row[1:]))



# Delete all MySQL tables created for sku strat & affinity during the user's session before closing the window
def delete_window(root):
    delete_mysql_tables_from_db()
    root.destroy()
