
import pymysql
import pandas as pd
import time

from helper_functions import get_asset_path


from select import select

import Shared
import Queries
# from Engineering_metrics_python.ExecuteQrys import DB_Pull1
# from Merging_29_10_2024.ExecuteQrys import DB_Pull1
# tbl = Shared.projtbl
# tblib = Shared.ibtbl
# projtbl = f"client_data.{Shared.project}"
# ibtbl = f"client_data.{Shared.project_inbound}"


sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')


# print(Shared.project)
# df = DB_Pull1()
start_time = time.time()
def DB_Pull1():
    # tbl = Shared.projtbl
    # tblib = Shared.ibtbl
    # projtbl = f"client_data.{Shared.project}"
    # ibtbl = f"client_data.{Shared.project_inbound}"
    tbl = f"client_data.{Shared.project}"
    ibtbl = f"client_data.{Shared.project_inbound}"
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
        # select_query = Queries.sku_all_data()
        select_query = f"""  
    WITH MaxDate AS (
        SELECT MAX(a.Order_Date) AS Max_Order_Date
        FROM {tbl} a
    ),
    skustrat AS (
        SELECT
            a.SKU SKU,
            LAST_DAY(a.Order_Date) EOM,
            COUNT(DISTINCT a.Order_Number) Orders,
            COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
            count(*) 'Line2',
            SUM(a.Qty) AS Qty,
            FORMAT(COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY LAST_DAY(a.Order_Date)), 0), 10) AS percent
        FROM
            {tbl} a
            JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
        -- WHERE
            
        GROUP BY
            a.SKU, LAST_DAY(a.Order_Date)
        ORDER BY
            3 DESC
    ),
    affinbase AS (
        SELECT
            a.SKU SKU,
            MONTH(a.Order_Date) Mth,
            COUNT(DISTINCT a.Order_Number) Orders,
            COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
            SUM(a.Qty) AS Qty,
            SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        FROM
            {tbl} a
            JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
        -- WHERE
            
        GROUP BY
            a.SKU, MONTH(a.Order_Date)
        ORDER BY
            1, 4 DESC
    ),
    
    finalsku AS (
        SELECT
            *,
            SUM(a.percent) OVER (partition by `EOM` order BY `Line2` DESC) AS CumPercQty
        FROM
            skustrat a
    ),
    tbl AS (
        SELECT 
            *, 
            CASE 
                WHEN CumPercQty <= 0.80 THEN 'A'
                WHEN CumPercQty > 0.80 AND CumPercQty <= 0.95 THEN 'B'
                WHEN CumPercQty > 0.95 THEN 'C' 
            END AS SkuStrat
        FROM 
            finalsku
    )
    
    SELECT
        "All" Business_Unit,
        "All" DC,
        'All' Segment,
        -- a.sku,
        LAST_DAY(a.Order_Date) AS EOM,
        b.SkuStrat,
        a.sku,
        COUNT(DISTINCT a.SKU) AS SKUs,
        COUNT(DISTINCT a.Order_Number) AS Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        count(*) "Line2",
        SUM(a.Qty) AS Qty
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    LEFT JOIN 
        tbl b ON b.SKU = a.SKU and LAST_DAY(a.Order_Date) =b.EOM
    -- WHERE 
        
    GROUP BY 
        LAST_DAY(a.Order_Date), b.SkuStrat  , a.sku;
    """
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df1 = pd.DataFrame(rows, columns=columns)

        cursor = connection.cursor()
        # select_query = Queries.split_sku_data
        select_query = f""" WITH MaxDate AS (
                SELECT MAX(a.Order_Date) AS Max_Order_Date
                FROM {tbl} a
            ),
            skustrat AS (
                SELECT
                    a.SKU SKU,
                    a.Order_Type,
                    LAST_DAY(a.Order_Date) EOM,
                    COUNT(DISTINCT a.Order_Number) Orders,
                    COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
                    COUNT(*) AS "Line2",
                    SUM(a.Qty) AS Qty,
                    -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
                    format(COUNT(*) / SUM(COUNT(*)) OVER (partition by LAST_DAY(a.Order_Date), a.order_type),10) AS percent
                FROM
                    {tbl} a
                    JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
                -- WHERE

                GROUP BY
                    a.SKU, a.order_type, LAST_DAY(a.Order_Date) 
                ORDER BY
                    3 DESC
            ),


            finalsku AS (
                SELECT
                    *,
                     SUM(a.percent) OVER (partition by `EOM`, a.order_type ORDER BY `Line2` DESC) AS CumPercQty
                    -- sUM(a.percent) OVER (ORDER BY qty DESC) AS CumPercQty
                FROM
                    skustrat a


            ),
            tbl AS (
                SELECT 
                    *, 
                    CASE 
                        WHEN CumPercQty <= 0.80 THEN 'A'
                        WHEN CumPercQty > 0.80 AND CumPercQty <= 0.95 THEN 'B'
                        WHEN CumPercQty > 0.95 THEN 'C' 
                    END AS SkuStrat
                FROM 
                    finalsku
            )

            SELECT
                "All" Business_Unit,
                "All" DC,
                a.order_type Segment,
                LAST_DAY(a.Order_Date) AS EOM,
                b.SkuStrat,
                a.sku,
                COUNT(DISTINCT a.SKU) AS SKUs,
                COUNT(DISTINCT a.Order_Number) AS Orders,
                COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
                COUNT(*) AS "Line2",
                SUM(a.Qty) AS Qty
            FROM
                {tbl} a
                JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
            LEFT JOIN 
                tbl b ON b.SKU = a.SKU and a.Order_Type=b.order_type and LAST_DAY(a.Order_Date) =b.EOM
            -- WHERE 

            GROUP BY 
                a.order_type, LAST_DAY(a.Order_Date), b.SkuStrat, a.sku;
            """
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df2 = pd.DataFrame(rows, columns=columns)

        dfall = pd.concat([df1, df2], ignore_index=True)

        print("Data fetched successfully!")
        return dfall

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()


def DB_Pull2():
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
        select_query = Queries.Strat_All_All_Split
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df1 = pd.DataFrame(rows, columns=columns)

        cursor = connection.cursor()
        select_query = Queries.Strat_All_All_All
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df2 = pd.DataFrame(rows, columns=columns)

        dfall = pd.concat([df1, df2], ignore_index=True)

        print("Data fetched successfully!")
        return dfall

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()


def DB_Pull3():
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
        select_query = Queries.Var_All_All_Split
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df1 = pd.DataFrame(rows, columns=columns)

        cursor = connection.cursor()
        select_query = Queries.Var_All_All_All
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df2 = pd.DataFrame(rows, columns=columns)

        dfall = pd.concat([df1, df2], ignore_index=True)

        print("Data fetched successfully!")
        return dfall

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()

def DB_Pull_Base():
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
        select_query = Queries.Base
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        dfall = pd.DataFrame(rows, columns=columns)

        #dfall = pd.concat([df1, df2], ignore_index=True)

        print("Data fetched successfully!")
        return dfall

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()


def DB_Pull4():
    tbl = f"client_data.{Shared.project}"
    ibtbl = f"client_data.{Shared.project_inbound}"
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
        # select_query = Queries.sku_pickAffin_data()
        select_query = f""" 
    
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
         {tbl} a
    -- WHERE 
    
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
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
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
      'All' dc_name,
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
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
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
      'All' order_type,
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
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
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
      'All' order_type,
       'All' dc_name,
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
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
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
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
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
      'All' dc_name,
      'Total' AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type),3) AS '% of Orders',
      count(*) AS `Lines`,
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
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
      'All' order_type,
       a.dc_name,
      'Total' AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.dc_name),3) AS '% of Orders',
      count(*) AS `Lines`,
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
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
      'All' order_type,
       'All' dc_name,
      'Total' AS 'Pick Affinity',
      count(distinct(a.order_number)) AS Orders,
      round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders',
      count(*) AS `Lines`,
      count(distinct(a.sku)) AS SKUs,
      SUM(a.units) AS Units,
      SUM(a.pallet_picks) AS Pallet_Picks,
      SUM(a.Layer_picks) AS Layer_Picks,
      SUM(a.case_picks) AS Case_Picks,
      SUM(a.each_picks) AS Each_Picks
    FROM 
      Pickbase a
    """
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        dfall = pd.DataFrame(rows, columns=columns)

        #dfall = pd.concat([df1, df2], ignore_index=True)

        print("Data fetched successfully!")
        return dfall

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()


def DB_Pull5(query):
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
        select_query = query
        #print(select_query)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        dfall = pd.DataFrame(rows, columns=columns)

        #dfall = pd.concat([df1, df2], ignore_index=True)

        print("Data fetched successfully!")
        return dfall

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()

def DB_Pull6():
    tbl = f"client_data.{Shared.project}"
    tblib = f"client_data.{Shared.project_inbound}"
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
        # select_query = Queries.PickAffinIB
        select_query = f""" 
with Pickbase AS (
    SELECT
    a. Destination_DC DC_Name,
    a.PO_Number order_number,
    a.sku,
	count(distinct(a.PO_Number)) Orders,
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
     {tblib} a
-- WHERE 

 GROUP BY 
  a. Destination_DC,
    a.PO_Number, 
    a.sku
  )
  
  SELECT 
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
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
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
  'All' dc_name,
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
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
  SUM(a.pallet_picks) AS Pallet_Picks,
  SUM(a.Layer_picks) AS Layer_Picks,
  SUM(a.case_picks) AS Case_Picks,
  SUM(a.each_picks) AS Each_Picks
FROM 
  Pickbase a
GROUP BY
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
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
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
   'All' dc_name,
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
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
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
  a.dc_name,
  'Total' AS 'Pick Affinity',
  count(distinct(a.order_number)) AS Orders,
  round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.dc_name),3) AS '% of Orders',
  count(*) AS `Lines`,
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
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
  'All' dc_name,
  'Total' AS 'Pick Affinity',
  count(distinct(a.order_number)) AS Orders,
  round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders',
  count(*) AS `Lines`,
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
  SUM(a.pallet_picks) AS Pallet_Picks,
  SUM(a.Layer_picks) AS Layer_Picks,
  SUM(a.case_picks) AS Case_Picks,
  SUM(a.each_picks) AS Each_Picks
FROM 
  Pickbase a
-- GROUP BY

  
  union
  
  SELECT 
   a.dc_name,
  'Total' AS 'Pick Affinity',
  count(distinct(a.order_number)) AS Orders,
  round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.dc_name),3) AS '% of Orders',
  count(*) AS `Lines`,
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
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
   'All' dc_name,
  'Total' AS 'Pick Affinity',
  count(distinct(a.order_number)) AS Orders,
  round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders',
  count(*) AS `Lines`,
  count(distinct(a.sku)) AS SKUs,
  SUM(a.units) AS Units,
  SUM(a.pallet_picks) AS Pallet_Picks,
  SUM(a.Layer_picks) AS Layer_Picks,
  SUM(a.case_picks) AS Case_Picks,
  SUM(a.each_picks) AS Each_Picks
FROM 
  Pickbase a
"""

        #print(select_query)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        dfall = pd.DataFrame(rows, columns=columns)

        #dfall = pd.concat([df1, df2], ignore_index=True)

        print("Data fetched successfully!")
        return dfall

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

    finally:
        connection.close()