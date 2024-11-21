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
    export_button.grid(row=0, column=4, padx=(250, 0), pady=(5, 5), sticky='w')

    # Create a Calculate button
    calculate_button = ttk.Button(filters_frame, text="Calculate", command=update_with_calculate)
    calculate_button.grid(row=0, column=3, padx=(0, 0), pady=(5, 5), sticky='w')

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

    # def update_with_calculate():
    #     # Call the build function to recalculate data
    #     global dfmthlist
    #     dfmthlist = build()  # This will rebuild the dfmthlist and associated data
    #
    #     # After calculating, update the chart and Treeview
    #     selected_sku = sku_combobox.get()
    #
    #     # Filter dfmthlist based on the selected SKU
    #     filtered_dfmthlist = dfmthlist[dfmthlist['Segment'] == selected_sku]
    #
    #     # Update the charts with the new data
    #     update_chart(selected_sku, "All", "All", ax1, canvas1, agg, filtered_dfmthlist, tree)
    #     update_chart(selected_sku, "All", "All", ax2, canvas2, agg2, filtered_dfmthlist, tree, is_sku_chart=True)
    #
    #     # Load the filtered DataFrame into the Treeview
    #     load_treeview(filtered_dfmthlist, tree)

    # Bind the combobox selections to the update_chart function
    def on_combobox_change(event):
        selected_sku = sku_combobox.get()

        # Filter dfmthlist based on the selected SKU
        filtered_dfmthlist = dfmthlist[dfmthlist['Segment'] == selected_sku]

        update_chart(selected_sku, "All", "All", ax1, canvas1, agg, filtered_dfmthlist, tree)
        update_chart(selected_sku, "All", "All", ax2, canvas2, agg2, filtered_dfmthlist, tree, is_sku_chart=True)

        # Load the filtered DataFrame into the Treeview
        load_treeview(filtered_dfmthlist, tree)

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