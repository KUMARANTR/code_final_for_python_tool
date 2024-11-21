

password = None
userid = None
# password = 'DataAnalytics@12345'
# userid = 'kumarantr'
#password = 'GXO_Analytics_1234'
#userid = 'alhassan'
# password = 'DataAnalytics@12345'
# userid = 'kumarantr'
project = None
project_inbound = None
#project = 'GOOGLECORNHUSKER_STD_OB_V1_10142024'
# project = 'ALPARGATAS_STD_OB_V1_091824'
# project='ALPARGATAS_STD_OB_V1_09_11_24'
# project = 'Kent_Water_Sports_STD_OB_V1_071024'
# project ='SPANX_REPRICING_STD_OB_V1_080624'
# project_inbound = 'INBOUND_STD_SAMPLE'
projtbl = f"client_data.{project}"
inboundtbl = f"client_data.{project_inbound}"
# ibtbl = 'client_data.INBOUND_STD_SAMPLE'

dc_names = None
dc_names_outbound = None
bu_names_outbound = None
channel_names_outbound = None
min_date_sql_outbound = None
max_date_sql_outbound = None
max_date_sql_outbound = None
min_date_sql_inbound = None
max_date_sql_inbound = None




# def get_asset_path(filename):
#     """
#     This function wraps a directory or folder so that pyinstaller can access the contents
#     """
#     if hasattr(sys, '_MEIPASS'):
#         return os.path.join(sys._MEIPASS, 'assets', filename)
#     return os.path.join('assets', filename)
#
#



stdt = None
enddt = None




# # Column Configuration
#     tab3.grid_columnconfigure(0, weight=0)  # Segment label column
#     tab3.grid_columnconfigure(1, weight=0)  # Segment combobox column
#     tab3.grid_columnconfigure(2, weight=0)  # Start date label column
#     tab3.grid_columnconfigure(3, weight=0)  # Start date entry column
#     tab3.grid_columnconfigure(4, weight=0)  # End date label column
#     tab3.grid_columnconfigure(5, weight=0)  # End date entry column




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
 a.ship_date between '{stdt}' and '{enddt}'

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