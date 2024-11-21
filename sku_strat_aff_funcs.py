import pandas as pd
import numpy as np
import sqlalchemy
from urllib.parse import quote_plus
import Shared
from sqlalchemy import text
import datetime
from tkinter import messagebox
from helper_functions import get_asset_path

# ========================= HELPER FUNCTIONS =========================
user_id = Shared.userid
password = Shared.password


# Create engine for MySQL connection
def create_mysql_engine():
    try:
        db_user = user_id
        db_password = password
        db_password_updated = quote_plus(db_password)  # my password has an '@' character, this fixes any issues with it
        db_host = '10.216.252.8'
        db_port = 3306
        db_name = 'client_data'
        ssl_ca = get_asset_path('server-ca.pem')
        ssl_key = get_asset_path('client-key.pem')
        ssl_cert = get_asset_path('client-cert.pem')
        engine = sqlalchemy.create_engine(
        f"mysql+pymysql://{db_user}:{db_password_updated}@{db_host}:{db_port}/{db_name}",
            connect_args={
                "ssl_ca": ssl_ca,
                "ssl_cert": ssl_cert,
                "ssl_key":ssl_key
            }
        )
        return engine
    except Exception as err:
        messagebox.showerror("Connection Error", f"Error: {err}")
        return None


def get_date_range(std_table_name, date_column='Order_Date'):
    engine = create_mysql_engine()
    # write the query
    query = f"""
        SELECT
            MIN({date_column}) AS earliest_date,
            MAX({date_column}) AS latest_date
        FROM {std_table_name};
        """
    date_range = pd.read_sql(query, engine)
    engine.dispose()
    earliest_date = date_range['earliest_date'].iloc[0]
    latest_date = date_range['latest_date'].iloc[0]
    return earliest_date, latest_date


# def get_order_types(std_table_name):
#     engine = create_mysql_engine()
#     query = f"""
#             SELECT
#                DISTINCT Order_Type
#             FROM {std_table_name};
#             """
#     result = pd.read_sql(query, engine)
#     engine.dispose()
#     order_types = result['Order_Type'].values.tolist()
#     order_types.insert(0, 'ALL')
#     return order_types

def get_distinct_col_values(std_table_name, col_name):
    engine = create_mysql_engine()
    query = f"""
            SELECT
               DISTINCT {col_name}
            FROM {std_table_name};
            """
    result = pd.read_sql(query, engine)
    engine.dispose()
    distinct_vals = result[col_name].values.tolist()
    distinct_vals.insert(0, 'ALL')
    return distinct_vals


def get_data_pull_query(std_table_name, channel='ALL', bu='ALL', dc='ALL', date_column='Order_Date',
                        start_date='', end_date=''):
    filters = []
    if channel != 'ALL':
        filters.append(f""" Order_Type = "{channel}" """)
    if bu != 'ALL':
        filters.append(f""" Business_Unit = "{bu}" """)
    if dc != 'ALL':
        filters.append(f""" DC_Name = "{dc}" """)
    if start_date != '' and end_date != '':
        filters.append(f""" {date_column} BETWEEN "{start_date}" AND "{end_date}" """)

    combined_filters_query = " AND ".join(filters)
    query = f"""
            SELECT
                {date_column}, Order_Number, SKU, Qty, Order_Type
            FROM {std_table_name}
            """
    if filters:
        query += 'WHERE' + combined_filters_query
    return query


def check_mysql_table_exists_in_db(table_name):
    engine = create_mysql_engine()
    exists = False
    query = f"""
        SHOW TABLES LIKE '{table_name}';
        """
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        if len(rows) > 0:
            exists = True
    #print(f'Check if {table_name} exists in database : {exists}')
    return exists


created_mysql_tables_dict = {}
def get_mysql_tbl_name(tbl_name_prefix, std_table_name, channel, bu, dc, start_date, end_date, custom_input):
    global created_mysql_tables_dict

    settings = {'std_table_name': std_table_name, 'channel': channel, 'bu': bu, 'dc': dc,
                'start_date': start_date, 'end_date': end_date, 'custom_input': custom_input}
    # print(f'Retrieving / Creating MySQL table name for following settings: {settings}')
    # print(f'Dictionary: {created_mysql_tables_dict}')

    # first, go through existing tables and search for matching settings. return existing table name if there's a match
    for existing_tbl_name, existing_tbl_settings in created_mysql_tables_dict.items():
        if tbl_name_prefix in existing_tbl_name and existing_tbl_settings == settings:
            # print(f'Returned table name {existing_tbl_name} which matched settings above.')
            return existing_tbl_name

    # else, create a new table name by appending the prefix, user ID, standard outbound table name, and timestamp
    # mysql table names have a 64 char limit, so replace unnecessary underscores and words
    timestamp = datetime.datetime.now().strftime('%m %d %y %H %M %S')
    timestamp = timestamp.lstrip("0").replace(" 0", " ").replace(" ", "")
    replace_list = ['.', 'STD', 'OB', 'OUTBOUND', 'DATA']
    std_table_name_formatted = std_table_name.replace('_', '')
    for string in replace_list:
        std_table_name_formatted = std_table_name_formatted.replace(string, '')
    mysql_tbl_name = f"{tbl_name_prefix}_{user_id}_{std_table_name_formatted}_{timestamp}"
    # print(f'No existing tables match the settings above. Returned new table name: {mysql_tbl_name}')
    return mysql_tbl_name


def add_mysql_tbl_name_to_dict(mysql_tbl_name, std_table_name, channel, bu, dc, start_date, end_date, custom_input):
    # Add the table to the dictionary of tables, and store its settings as a dictionary
    settings_dict = dict.fromkeys(['std_table_name', 'channel', 'bu', 'dc', 'start_date', 'end_date', 'custom_input'])
    created_mysql_tables_dict[mysql_tbl_name] = settings_dict
    created_mysql_tables_dict[mysql_tbl_name]['std_table_name'] = std_table_name
    created_mysql_tables_dict[mysql_tbl_name]['channel'] = channel
    created_mysql_tables_dict[mysql_tbl_name]['bu'] = bu
    created_mysql_tables_dict[mysql_tbl_name]['dc'] = dc
    created_mysql_tables_dict[mysql_tbl_name]['start_date'] = start_date
    created_mysql_tables_dict[mysql_tbl_name]['end_date'] = end_date
    created_mysql_tables_dict[mysql_tbl_name]['custom_input'] = custom_input
    # print(f"Added {mysql_tbl_name} to dictionary of tables")
    # print(f" Dictionary keys: {created_mysql_tables_dict.keys()}")


def delete_mysql_tables_from_db():
    engine = create_mysql_engine()
    # print(f"List of created tables: {created_mysql_tables_dict.keys()}")
    for table in created_mysql_tables_dict.keys():
        if check_mysql_table_exists_in_db(table) == True:
            query = f"""DROP TABLE {table};"""
            with engine.connect() as connection:
                connection.execute(text(query))
                # print(f'Dropped {table} from MySQL database')


# ========================= SKU STRATIFICATION =========================
def sku_stratification_summary_table(data_with_sku_strat):
    filtered_data = data_with_sku_strat[['Order_Number', 'SKU', 'Qty', 'SKU_Type']]

    # group by the ABC flag and get the totals
    sku_strat_df = filtered_data.groupby('SKU_Type').agg(SKUs=('SKU', 'nunique'),
                                                         Orders=('Order_Number', 'nunique'),
                                                         Lines=('SKU_Type', 'size'),
                                                         Units=('Qty', 'sum')
                                                         ).sort_values(by='SKU_Type')

    # create total percentage columns for each column
    for col in sku_strat_df.columns:
        col_index = sku_strat_df.columns.tolist().index(col)
        pct_col_name = '% ' + col
        if pct_col_name == '% Orders':
            col_total = filtered_data['Order_Number'].nunique()
        else:
            col_total = sku_strat_df[col].sum()
        pct_values = pd.Series(sku_strat_df[col] / col_total)
        sku_strat_df.insert(col_index + 1, pct_col_name, pct_values)

    # create grand total row
    totals = ['Total']
    for col in sku_strat_df.columns:
        if 'Orders' in col:
            col_total = np.nan
        else:
            col_total = sku_strat_df[col].sum()
        totals.append(col_total)
    sku_strat_df = sku_strat_df.reset_index()
    totals_row = pd.DataFrame([totals], columns=sku_strat_df.columns)
    sku_strat_df = pd.concat([sku_strat_df, totals_row])
    sku_strat_df = sku_strat_df.rename(columns={'SKU_Type': 'SKU Type'})
    return sku_strat_df


global default_ABC_split, default_AABC_split
default_ABC_split = {'A': 80, 'B': 15, 'C': 5}
default_AABC_split = {'A+': 50, 'A': 30, 'B': 15, 'C': 5}


def sku_stratification(std_table_name, split=default_ABC_split, UOM='lines', channel='ALL', bu='ALL', dc='ALL',
                       date_column='Order_Date', start_date='', end_date=''):
    engine = create_mysql_engine()

    sku_strat_mysql_tbl_name = get_mysql_tbl_name('SKUSTRAT', std_table_name, channel, bu, dc,
                                                  start_date, end_date, split)

    # print(f'sku_strat_mysql_tbl_name : {sku_strat_mysql_tbl_name}')
    sku_type_mysql_tbl_name = get_mysql_tbl_name('SKUTYPES', std_table_name, channel, bu, dc,
                                                 start_date, end_date, split)
    # print(f'sku_type_mysql_tbl_name : {sku_type_mysql_tbl_name}')

    if check_mysql_table_exists_in_db(sku_strat_mysql_tbl_name) == True:
        sku_strat_df = pd.read_sql(f"SELECT * FROM {sku_strat_mysql_tbl_name}", engine)
    else:
        # pull the data from the database based on filters
        data_pull_query = get_data_pull_query(std_table_name, channel=channel, bu=bu, dc=dc, date_column=date_column,
                                              start_date=start_date, end_date=end_date)
        filtered_data = pd.read_sql(data_pull_query, engine)

        df = filtered_data.groupby('SKU').agg(orders=('Order_Number', 'nunique'),
                                              lines=('SKU', 'size'),
                                              units=('Qty', 'sum')
                                              ).reset_index()
        # sort by UOM in descending order
        df = df.sort_values(by=UOM, ascending=False)

        # create UOM percentage column
        UOM_pct_col_name = UOM + ' %'
        df[UOM_pct_col_name] = (df[UOM] / df[UOM].sum()) * 100

        # rank each SKU by UOM (rank #1 = top SKU by UOM)
        df['rank'] = df[UOM].rank(method='min', ascending=False)
        # create a separate df grouping by rank and calculating the cumulative UOM percentage
        rank_cumulative_pct_df = df.groupby('rank').agg(cumulative_pct=(UOM_pct_col_name, 'sum')).cumsum().reset_index()
        cumulative_UOM_pct_col_name = 'cumulative ' + UOM + ' %'
        rank_cumulative_pct_df = rank_cumulative_pct_df.rename(columns={'cumulative_pct': cumulative_UOM_pct_col_name})
        # join the dfs to get the cumulative percent column
        df = df.merge(rank_cumulative_pct_df, how='left')
        # create new dictionary with calculated cutoffs based on the A,B,C or A+,A,B,C split (ex: {A: 80, B: 15: C: 5} --> {A: 80, B: 95, C: 100})
        split_cutoffs = dict(zip(split.keys(), np.cumsum(list(split.values()))))
        # create column assigning each SKU as A, B, C or A+, A, B, C based on cutoffs
        sku_type_flag = []
        for cumulative_pct in df[cumulative_UOM_pct_col_name]:
            for flag in split_cutoffs: #TODO: optimize
                if np.round(cumulative_pct,4) <= split_cutoffs[flag]:
                    sku_type_flag.append(flag)
                    break
        df['SKU_Type'] = sku_type_flag
        # append ABC column to data
        data_with_sku_strat = filtered_data.merge(df[['SKU', 'SKU_Type']], on='SKU')

        # create table with just columns SKU and SKU_Type
        sku_type_table = data_with_sku_strat[['SKU', 'SKU_Type']].drop_duplicates()
        # upload to mysql
        sku_type_table.to_sql(sku_type_mysql_tbl_name, engine, if_exists='replace', index=False)
        print(f'Created new table in database: {sku_type_mysql_tbl_name}')
        add_mysql_tbl_name_to_dict(sku_type_mysql_tbl_name, std_table_name, channel, bu, dc, start_date, end_date, split)

        # create sku strat summary table
        sku_strat_df = sku_stratification_summary_table(data_with_sku_strat)
        # upload sku strat summary table to mysql
        sku_strat_df.to_sql(sku_strat_mysql_tbl_name, engine, if_exists='replace', index=False)
        print(f'Created new table in database: {sku_strat_mysql_tbl_name}')
        add_mysql_tbl_name_to_dict(sku_strat_mysql_tbl_name, std_table_name, channel, bu, dc, start_date, end_date, split)

    return sku_strat_df, sku_type_mysql_tbl_name

def sku_stratification_top_N(std_table_name, number_of_skus, split=default_AABC_split, UOM='lines', channel='ALL',
                             bu='ALL', dc='ALL', date_column='Order_Date', start_date='', end_date=''):
    engine = create_mysql_engine()

    sku_strat_mysql_tbl_name = get_mysql_tbl_name('SKUSTRAT', std_table_name, channel, bu, dc,
                                                  start_date, end_date, number_of_skus)
    sku_type_mysql_tbl_name = get_mysql_tbl_name('SKUTYPE', std_table_name, channel, bu, dc,
                                                  start_date, end_date, number_of_skus)
    if check_mysql_table_exists_in_db(sku_strat_mysql_tbl_name) == True:
        sku_strat_summary_df = pd.read_sql(f"SELECT * FROM {sku_strat_mysql_tbl_name}", engine)
    else:
        # pull the data from the database based on filters
        data_pull_query = get_data_pull_query(std_table_name, channel=channel, bu=bu, dc=dc, date_column=date_column,
                                              start_date=start_date, end_date=end_date)
        filtered_data = pd.read_sql(data_pull_query, engine)

        df = filtered_data.groupby('SKU').agg(orders=('Order_Number', 'nunique'),
                                              lines=('SKU', 'size'),
                                              units=('Qty', 'sum')
                                              ).reset_index()
        # sort by UOM in descending order
        df = df.sort_values(by=UOM, ascending=False)

        # create UOM percentage column
        UOM_pct_col_name = UOM + ' %'
        df[UOM_pct_col_name] = (df[UOM] / df[UOM].sum()) * 100

        # rank each SKU by UOM (rank #1 = top SKU by UOM)
        df['rank'] = df[UOM].rank(method='min', ascending=False)
        # create a separate df grouping by rank and calculating the cumulative UOM percentage
        rank_cumulative_pct_df = df.groupby('rank').agg(cumulative_pct=(UOM_pct_col_name, 'sum')).cumsum().reset_index()
        cumulative_UOM_pct_col_name = 'cumulative ' + UOM + ' %'
        rank_cumulative_pct_df = rank_cumulative_pct_df.rename(columns={'cumulative_pct': cumulative_UOM_pct_col_name})
        # join the dfs to get the cumulative percent column
        df = df.merge(rank_cumulative_pct_df, how='left')

        # get the cumulative percentage cutoff for the top N SKU where N = number_of_skus
        top_N_cutoff_pct = df.loc[df['rank'] <= number_of_skus, cumulative_UOM_pct_col_name].values[-1]
        try:
            if top_N_cutoff_pct > 80:
                raise ValueError
        except ValueError:
            raise ValueError

        # create dict with calculated cutoffs based on the A,B,C or A+,A,B,C split (ex: {A: 80, B: 15: C: 5} --> {A: 80, B: 95, C: 100})
        split_cutoffs = dict(zip(split.keys(), np.cumsum(list(split.values()))))
        # create column assigning each SKU as A+, A, B, C based on cutoffs
        sku_type_flag = []
        for cumulative_pct in df[cumulative_UOM_pct_col_name]:
            if np.round(cumulative_pct,4) <= top_N_cutoff_pct:
                sku_type_flag.append('A+')
            else:
                for flag in split_cutoffs:  # TODO: optimize
                    if np.round(cumulative_pct,4) <= split_cutoffs[flag]:
                        sku_type_flag.append(flag)
                        break

        df['SKU_Type'] = sku_type_flag
        # append ABC column to data
        data_with_sku_strat = filtered_data.merge(df[['SKU', 'SKU_Type']], on='SKU')

        # create table with just columns SKU and SKU_Type
        sku_type_table = data_with_sku_strat[['SKU', 'SKU_Type']].drop_duplicates()
        # upload to mysql
        sku_type_table.to_sql(sku_type_mysql_tbl_name, engine, if_exists='replace', index=False)
        add_mysql_tbl_name_to_dict(sku_type_mysql_tbl_name, std_table_name, channel, bu, dc,
                                   start_date, end_date, number_of_skus)

        # create sku strat summary table
        sku_strat_summary_df = sku_stratification_summary_table(data_with_sku_strat)
        # upload sku strat summary table to mysql
        sku_strat_summary_df.to_sql(sku_strat_mysql_tbl_name, engine, if_exists='replace', index=False)
        add_mysql_tbl_name_to_dict(sku_strat_mysql_tbl_name, std_table_name, channel, bu, dc,
                                   start_date, end_date, number_of_skus)

    return sku_strat_summary_df, sku_type_mysql_tbl_name


# ========================= SKU AFFINITY =========================
def sku_affinity_table(sku_strat_summary_df, data_sku_affinity):
    data_sku_affinity = data_sku_affinity.set_index('SKU_Types')
    # create total percentage columns for each column
    for col in data_sku_affinity.columns:
        col_index = data_sku_affinity.columns.tolist().index(col)
        pct_col_name = '% ' + col
        if pct_col_name == '% SKUs':
            total_skus = sku_strat_summary_df.loc[sku_strat_summary_df['SKU Type'] == 'Total', 'SKUs'].values[0]
            total_skus = int(total_skus.replace(',', ''))
            col_total = total_skus
        else:
            col_total = data_sku_affinity[col].sum()
        pct_values = pd.Series(data_sku_affinity[col] / col_total)
        data_sku_affinity.insert(col_index + 1, pct_col_name, pct_values)

    # create grand total row
    totals = ['Total']
    for col in data_sku_affinity.columns:
        if 'SKU' in col:
            col_total = np.nan
        else:
            col_total = data_sku_affinity[col].sum()
        totals.append(col_total)
    data_sku_affinity = data_sku_affinity.reset_index()
    totals_row = pd.DataFrame([totals], columns=data_sku_affinity.columns)
    sku_affinity_df = pd.concat([data_sku_affinity, totals_row])
    sku_affinity_df = sku_affinity_df.rename(columns={'SKU_Types': 'Order Types', 'Line': 'Lines', '% Line': '% Lines'})
    return sku_affinity_df


def sku_affinity(sku_strat_summary_df, sku_type_mysql_tbl_name, std_table_name, channel='ALL', bu='ALL', dc='ALL',
                 date_column='Order_Date', start_date='', end_date='', custom_input=''):
    engine = create_mysql_engine()
    # print('Calculating SKU Affinity...')
    try:
        if check_mysql_table_exists_in_db(sku_type_mysql_tbl_name) == False:
            raise ValueError
    except ValueError:
        delete_mysql_tables_from_db()
        raise ValueError

    sku_affinity_mysql_tbl_name = get_mysql_tbl_name('SKUAFF', std_table_name, channel, bu, dc,
                                                     start_date, end_date, custom_input)
    # print(f'sku_affinity_mysql_tbl_name : {sku_affinity_mysql_tbl_name}')
    if check_mysql_table_exists_in_db(sku_affinity_mysql_tbl_name) == False:
        # pull the line level data from the database based on filters
        data_pull_query = get_data_pull_query(std_table_name, channel=channel, bu=bu, dc=dc, date_column=date_column,
                                              start_date=start_date, end_date=end_date)

        # merge data table and sku type table to append SKU_Type column to outbound data
        merged_mysql_tbl_name = get_mysql_tbl_name('MERGED', std_table_name, channel, bu, dc,
                                                   start_date, end_date, custom_input)
        merge_query = f"""CREATE TABLE IF NOT EXISTS {merged_mysql_tbl_name} AS
                            (SELECT
                                OB.*,
                                SKU_STRAT.SKU_Type
                            FROM ({data_pull_query}) AS OB
                            LEFT JOIN {sku_type_mysql_tbl_name} AS SKU_STRAT
                            ON OB.SKU = SKU_STRAT.SKU);"""
        with engine.connect() as connection:
            connection.execute(text(merge_query))
            print(f'Created table in database: {merged_mysql_tbl_name}')
        add_mysql_tbl_name_to_dict(merged_mysql_tbl_name, std_table_name, channel, bu, dc, start_date, end_date,
                                   custom_input)
        # affinity
        sku_affinity_query = f"""CREATE TABLE IF NOT EXISTS {sku_affinity_mysql_tbl_name} AS
                            (SELECT
                                c.SKU_Types AS SKU_Types,
                                COUNT(DISTINCT c.SKU) AS SKUs,
                                COUNT(DISTINCT c.Order_Number) AS Orders,
                                COUNT(*) AS Line,
                                SUM(c.Qty) AS Units
                            FROM (  
                                SELECT 
                                    OB.*, 
                                    b.SKU_Types 
                                FROM 
                                    {merged_mysql_tbl_name} AS OB
                                    LEFT JOIN
                                    (SELECT 
                                        Order_Number, 
                                        GROUP_CONCAT(DISTINCT SKU_Type) AS SKU_Types
                                     FROM 
                                        {merged_mysql_tbl_name}
                                     GROUP BY 
                                        Order_Number
                                    ) AS b
                                ON
                                OB.Order_Number = b.Order_Number
                            ) AS c
                            GROUP BY
                                c.SKU_Types);
                            """
        with engine.connect() as connection:
            connection.execute(text(sku_affinity_query))
            print(f'Created table in database: {sku_affinity_mysql_tbl_name}')
        add_mysql_tbl_name_to_dict(sku_affinity_mysql_tbl_name, std_table_name, channel, bu, dc,
                                   start_date, end_date, custom_input)

    # read sku affinity summary table as df
    sku_affinity_df = pd.read_sql(f""" SELECT * FROM {sku_affinity_mysql_tbl_name}""", engine)
    # format sku affinity summary table
    sku_affinity_summary_df = sku_affinity_table(sku_strat_summary_df, sku_affinity_df)

    return sku_affinity_summary_df
