import Shared

tbl = Shared.projtbl
tblib = Shared.inboundtbl
projtbl = f"client_data.{Shared.project}"
ibtbl = f"client_data.{Shared.project_inbound}"


def sku_all_data():
    tbl = Shared.projtbl
    tblib = Shared.ibtbl
    projtbl = f"client_data.{Shared.project}"
    ibtbl = f"client_data.{Shared.project_inbound}"
    Sku_All_All_All = f"""  
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
    return Sku_All_All_All


sku_All_All_All2 = f"""  
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
skuaffin AS (
    SELECT
        a.SKU,
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU
    ORDER BY
        2 DESC
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
),
skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
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
    skuall b ON b.SKU = a.SKU and LAST_DAY(a.Order_Date) =b.EOM
-- WHERE 

GROUP BY 
    LAST_DAY(a.Order_Date), b.SkuStrat  , a.sku;
"""

OG_Sku_All_All_All = f"""
WITH skustrat AS (
    SELECT
        a.SKU SKU,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        SUM(a.Qty) AS Qty,
        SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
    FROM
        {tbl} a
    -- WHERE
        
    GROUP BY
        a.SKU
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
    -- WHERE
    
    GROUP BY
        a.SKU, MONTH(a.Order_Date)
    ORDER BY
        1, 4 DESC
),
skuaffin AS (
    SELECT
        a.SKU,
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU
    ORDER BY
        2 DESC
),
finalsku AS (
    SELECT
        *,
        SUM(a.percent) OVER (ORDER BY qty DESC) AS CumPercQty
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
),
skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
)
SELECT
    "All" Business_Unit,
    "All" DC,
    'All' Segment,
    LAST_DAY(a.Order_Date) AS EOM,
    b.SkuStrat,
    COUNT(DISTINCT a.SKU) AS SKUs,
    COUNT(DISTINCT a.Order_Number) AS Orders,
    COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
    COUNT(*) AS "Line2",
    SUM(a.Qty) AS Qty
FROM
    {tbl} a
LEFT JOIN 
    skuall b ON b.SKU = a.SKU
-- WHERE 
    
GROUP BY 
    LAST_DAY(a.Order_Date), b.SkuStrat;
"""


def split_sku_data():
    tbl = Shared.projtbl
    tblib = Shared.ibtbl
    projtbl = f"client_data.{Shared.project}"
    ibtbl = f"client_data.{Shared.project_inbound}"
    SKU_All_All_Split = f""" WITH MaxDate AS (
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
    return SKU_All_All_Split


SKU_All_All_Split2 = f""" WITH MaxDate AS (
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
affinbase AS (
    SELECT
        a.SKU SKU, a.order_type,
        MONTH(a.Order_Date) Mth,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE

    GROUP BY
        a.SKU, a.Order_Type, MONTH(a.Order_Date)
    ORDER BY
        1, 4 DESC
),
skuaffin AS (
    SELECT
        a.SKU, a.Order_Type,
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        2 DESC
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
),


skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
        and a.order_type=b.order_type
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
    skuall b ON b.SKU = a.SKU and a.Order_Type=b.order_type and LAST_DAY(a.Order_Date) =b.EOM
-- WHERE 

GROUP BY 
    a.order_type, LAST_DAY(a.Order_Date), b.SkuStrat, a.sku;
"""

OG_SKU_All_All_Split = f""" WITH skustrat AS (
    SELECT
        a.SKU SKU,
        a.Order_Type,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (partition by a.order_type) AS percent
    FROM
        {tbl} a
    WHERE
        a.Destination_Country = 'USA'
        AND a.SHIPTOSTATE NOT IN ('AK', 'HI', 'PR', 'VI')
        -- and a.order_type = 'B2C'
    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        3 DESC
),
affinbase AS (
    SELECT
        a.SKU SKU, a.order_type,
        MONTH(a.Order_Date) Mth,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
    FROM
        {tbl} a
    WHERE
        a.Destination_Country = 'USA'
        AND a.SHIPTOSTATE NOT IN ('AK', 'HI', 'PR', 'VI')
        -- and a.order_type = 'B2C'
    GROUP BY
        a.SKU, a.Order_Type, MONTH(a.Order_Date)
    ORDER BY
        1, 4 DESC
),
skuaffin AS (
    SELECT
        a.SKU, a.Order_Type,
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        2 DESC
),

finalsku AS (
    SELECT
        *,
         SUM(a.percent) OVER (partition by a.order_type ORDER BY qty DESC) AS CumPercQty
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
),


skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
        and a.order_type=b.order_type
)
SELECT
	"All" Business_Unit,
    "All" DC,
    a.order_type Segment,
    LAST_DAY(a.Order_Date) AS EOM,
    b.SkuStrat,
    COUNT(DISTINCT a.SKU) AS SKUs,
    COUNT(DISTINCT a.Order_Number) AS Orders,
    COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
    COUNT(*) AS "Line2",
    SUM(a.Qty) AS Qty
FROM
    {tbl} a
LEFT JOIN 
    skuall b ON b.SKU = a.SKU and a.Order_Type=b.order_type
WHERE 
    a.Destination_Country = 'USA'
    AND a.SHIPTOSTATE NOT IN ('AK', 'HI', 'PR', 'VI')
    -- and a.order_type = 'B2C'
GROUP BY 
    a.order_type, LAST_DAY(a.Order_Date), b.SkuStrat;
"""

####################### SKU Strat and Variance #######################################

Strat_All_All_Split = f""" WITH MaxDate AS (
    SELECT MAX(a.Order_Date) AS Max_Order_Date
    FROM {tbl} a
),
skustrat AS (
    SELECT
        a.SKU SKU,
        a.Order_Type,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        format(COUNT(*) / SUM(COUNT(*)) OVER (partition by a.order_type),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE
        
    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        3 DESC
),

finalsku AS (
    SELECT
        *,
         SUM(a.percent) OVER (partition by a.order_type ORDER BY `Line2` DESC) AS CumPercQty
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
    -- LAST_DAY(a.Order_Date) AS EOM,
    b.SkuStrat 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (partition by a.order_type),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (partition by a.order_type),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (partition by a.order_type),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type),3) AS '% of Orders'
FROM
    {tbl} a
    JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
LEFT JOIN 
    tbl b ON b.SKU = a.SKU and a.Order_Type=b.order_type
-- WHERE 
    
GROUP BY 
    a.order_type, b.SkuStrat
    
    union all
    SELECT
	"All" Business_Unit,
    "All" DC,
    a.order_type Segment,
    -- LAST_DAY(a.Order_Date) AS EOM,
    "Total" as 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (partition by a.order_type),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (partition by a.order_type),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (partition by a.order_type),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type),3) AS '% of Orders'
FROM
    {tbl} a
    JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
LEFT JOIN 
    tbl b ON b.SKU = a.SKU and a.Order_Type=b.order_type
-- WHERE 
   
 GROUP BY 
      a.order_type
    ;
"""

Strat_All_All_Split2 = f""" WITH MaxDate AS (
    SELECT MAX(a.Order_Date) AS Max_Order_Date
    FROM {tbl} a
),
skustrat AS (
    SELECT
        a.SKU SKU,
        a.Order_Type,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        format(COUNT(*) / SUM(COUNT(*)) OVER (partition by a.order_type),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE

    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        3 DESC
),
affinbase AS (
    SELECT
        a.SKU SKU, a.order_type,
        MONTH(a.Order_Date) Mth,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        COUNT(*) / SUM(COUNT(*)) OVER () AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE

    GROUP BY
        a.SKU, a.Order_Type, MONTH(a.Order_Date)
    ORDER BY
        1, 4 DESC
),
skuaffin AS (
    SELECT
        a.SKU, a.Order_Type,
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        2 DESC
),

finalsku AS (
    SELECT
        *,
         SUM(a.percent) OVER (partition by a.order_type ORDER BY `Line2` DESC) AS CumPercQty
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
),


skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
        and a.order_type=b.order_type
)
SELECT
	"All" Business_Unit,
    "All" DC,
    a.order_type Segment,
    -- LAST_DAY(a.Order_Date) AS EOM,
    b.SkuStrat 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (partition by a.order_type),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (partition by a.order_type),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (partition by a.order_type),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type),3) AS '% of Orders'
FROM
    {tbl} a
    JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
LEFT JOIN 
    skuall b ON b.SKU = a.SKU and a.Order_Type=b.order_type
-- WHERE 

GROUP BY 
    a.order_type, b.SkuStrat

    union all
    SELECT
	"All" Business_Unit,
    "All" DC,
    a.order_type Segment,
    -- LAST_DAY(a.Order_Date) AS EOM,
    "Total" as 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (partition by a.order_type),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (partition by a.order_type),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (partition by a.order_type),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (partition by a.order_type),3) AS '% of Orders'
FROM
    {tbl} a
    JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
LEFT JOIN 
    skuall b ON b.SKU = a.SKU and a.Order_Type=b.order_type
-- WHERE 

 GROUP BY 
      a.order_type
    ;
"""

Strat_All_All_All = f""" WITH MaxDate AS (
    SELECT MAX(a.Order_Date) AS Max_Order_Date
    FROM {tbl} a
),
 skustrat AS (
    SELECT
        a.SKU SKU,
        -- a.Order_Type,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        format(COUNT(*) / SUM(COUNT(*)) OVER (),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE
       
    GROUP BY
        a.SKU -- , a.order_type
    ORDER BY
        4 DESC
),

finalsku AS (
    SELECT
        *,
         SUM(a.percent) OVER (ORDER BY `Line2` DESC) AS CumPercQty
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
    "All" Segment,
    -- LAST_DAY(a.Order_Date) AS EOM,
    b.SkuStrat 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders'
     
    
   
FROM
    {tbl} a
	JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
     
LEFT JOIN 
    tbl b ON b.SKU = a.SKU -- and a.Order_Type=b.order_type
-- WHERE 

GROUP BY 
     b.SkuStrat
    
    union all
    SELECT
	"All" Business_Unit,
    "All" DC,
    "All" Segment,
    -- LAST_DAY(a.Order_Date) AS EOM,
    "Total" as 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders'
    
FROM
    {tbl} a
	JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
     
LEFT JOIN 
    tbl b ON b.SKU = a.SKU -- and a.Order_Type=b.order_type
-- WHERE 
   
    

    ;
"""

Strat_All_All_All2 = f""" WITH MaxDate AS (
    SELECT MAX(a.Order_Date) AS Max_Order_Date
    FROM {tbl} a
),
 skustrat AS (
    SELECT
        a.SKU SKU,
        -- a.Order_Type,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        format(COUNT(*) / SUM(COUNT(*)) OVER (),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE

    GROUP BY
        a.SKU -- , a.order_type
    ORDER BY
        4 DESC
),
affinbase AS (
    SELECT
        a.SKU SKU, -- a.order_type,
        MONTH(a.Order_Date) Mth,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        COUNT(*) / SUM(COUNT(*)) OVER () AS percent
    FROM
        {tbl} a
		JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE

    GROUP BY
        a.SKU  -- , a.Order_Type
        , MONTH(a.Order_Date)
    ORDER BY
        1, 4 DESC
),
skuaffin AS (
    SELECT
        a.SKU, -- a.Order_Type,
        COUNT(*) 'Lines2',
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU -- , a.order_type
    ORDER BY
        2 DESC
),

finalsku AS (
    SELECT
        *,
         SUM(a.percent) OVER (ORDER BY `Line2` DESC) AS CumPercQty
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
),


skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
        -- and a.order_type=b.order_type
)
SELECT
	"All" Business_Unit,
    "All" DC,
    "All" Segment,
    -- LAST_DAY(a.Order_Date) AS EOM,
    b.SkuStrat 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders'



FROM
    {tbl} a
	JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')

LEFT JOIN 
    skuall b ON b.SKU = a.SKU -- and a.Order_Type=b.order_type
-- WHERE 

GROUP BY 
     b.SkuStrat

    union all
    SELECT
	"All" Business_Unit,
    "All" DC,
    "All" Segment,
    -- LAST_DAY(a.Order_Date) AS EOM,
    "Total" as 'SKU Type',
    COUNT(DISTINCT a.SKU) AS 'SKU Count',
    round(COUNT(DISTINCT a.SKU)/sum(COUNT(DISTINCT a.SKU)) over (),2) '% of SKUs',
    SUM(a.Qty) AS Qty,
     round(SUM(a.Qty) / SUM(SUM(a.Qty)) OVER (),2) AS '% of Qty',
     count(*) 'Lines',
     round(count(*) / SUM(count(*)) OVER (),3) AS '% of Lines',
     count(distinct(a.order_number)) 'Orders',
     round(count(distinct(a.order_number))  / SUM(count(distinct(a.order_number)) ) OVER (),3) AS '% of Orders'

FROM
    {tbl} a
	JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')

LEFT JOIN 
    skuall b ON b.SKU = a.SKU -- and a.Order_Type=b.order_type
-- WHERE 



    ;
"""

Var_All_All_Split = f""" WITH MaxDate AS (
    SELECT MAX(a.Order_Date) AS Max_Order_Date
    FROM {tbl} a
),
skustrat AS (
    SELECT
        a.SKU SKU,
        a.Order_Type,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        format(COUNT(*) / SUM(COUNT(*)) OVER (partition by a.order_type),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE
        
    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        3 DESC
),
affinbase AS (
    SELECT
        a.SKU SKU, a.order_type,
        MONTH(a.Order_Date) Mth,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        format(COUNT(*) / SUM(COUNT(*)) OVER (),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE
        
    GROUP BY
        a.SKU, a.Order_Type, MONTH(a.Order_Date)
    ORDER BY
        1, 4 DESC
),
skuaffin AS (
    SELECT
        a.SKU, a.Order_Type,
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU, a.order_type
    ORDER BY
        2 DESC
),

finalsku AS (
    SELECT
        *,
         SUM(a.percent) OVER (partition by a.order_type ORDER BY `Line2` DESC) AS CumPercQty
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
),


skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
        and a.order_type=b.order_type
)
SELECT
	"All" Business_Unit,
    "All" DC,
    a.order_type Segment,
    b.SkuStrat 'SKU Type', 
    COUNT(DISTINCT CASE WHEN b.xyz = 'x' THEN a.SKU END) AS 'Low Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'y' THEN a.SKU END) AS 'Medium Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'z' THEN a.SKU END) AS 'High Variance',
    COUNT(DISTINCT (a.SKU )) AS 'Total SKU'

FROM
    {tbl} a
LEFT JOIN 
    skuall b ON b.SKU = a.SKU and a.Order_Type=b.order_type
 -- WHERE 
    
   
GROUP BY 
    a.order_type, b.SkuStrat
    
    union all
    SELECT
	"All" Business_Unit,
    "All" DC,
    a.order_type Segment,
    "Total" as 'SKU Type',
    COUNT(DISTINCT CASE WHEN b.xyz = 'x' THEN a.SKU END) AS 'Low Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'y' THEN a.SKU END) AS 'Medium Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'z' THEN a.SKU END) AS 'High Variance',
    COUNT(DISTINCT (a.SKU )) AS 'Total SKU'

FROM
    {tbl} a
LEFT JOIN 
    skuall b ON b.SKU = a.SKU and a.Order_Type=b.order_type
-- WHERE 
   
    -- and a.order_type = 'B2C'
 GROUP BY 
      a.order_type
    ;
"""

Var_All_All_All = f""" WITH MaxDate AS (
    SELECT MAX(a.Order_Date) AS Max_Order_Date
    FROM {tbl} a
),
skustrat AS (
    SELECT
        a.SKU SKU,
        -- a.Order_Type,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        -- SUM(a.Qty) / SUM(SUM(a.Qty)) OVER () AS percent
        format(COUNT(*) / SUM(COUNT(*)) OVER (),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE
        
    GROUP BY
        a.SKU
    ORDER BY
        3 DESC
),
affinbase AS (
    SELECT
        a.SKU SKU, 
        MONTH(a.Order_Date) Mth,
        COUNT(DISTINCT a.Order_Number) Orders,
        COUNT(DISTINCT CONCAT(a.Order_Number, a.SKU)) AS "Line",
        COUNT(*) AS "Line2",
        SUM(a.Qty) AS Qty,
        format(COUNT(*) / SUM(COUNT(*)) OVER (),10) AS percent
    FROM
        {tbl} a
        JOIN MaxDate md ON a.Order_Date >= DATE_FORMAT(DATE_SUB(md.Max_Order_Date, INTERVAL 12 MONTH), '%Y-%m-01')
    -- WHERE
        
    GROUP BY
        a.SKU, MONTH(a.Order_Date)
    ORDER BY
        1, 4 DESC
),
skuaffin AS (
    SELECT
        a.SKU, 
        SUM(a.Qty) AS qty,
        AVG(a.Qty) AS average,
        STDDEV_POP(a.Qty) AS StdDev,
        STDDEV_POP(a.Qty) / AVG(a.Qty) AS Coefficient,
        CASE 
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) <= 0.1 THEN 'X'
            WHEN STDDEV_POP(a.Qty) / AVG(a.Qty) < 0.25 THEN 'Y'
            ELSE 'Z'
        END AS XYZ
    FROM
        affinbase a
    GROUP BY
        a.SKU
    ORDER BY
        2 DESC
),

finalsku AS (
    SELECT
        *,
         SUM(a.percent) OVER (ORDER BY `Line2` DESC) AS CumPercQty
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
),


skuall AS (
    SELECT 
        a.*, 
        b.average, 
        b.StdDev, 
        b.Coefficient, 
        b.XYZ  
    FROM 
        tbl a
    LEFT JOIN 
        skuaffin b ON a.SKU = b.SKU
        -- and a.order_type=b.order_type
)
SELECT
	"All" Business_Unit,
    "All" DC,
    "All" Segment,
    b.SkuStrat 'SKU Type', 
    COUNT(DISTINCT CASE WHEN b.xyz = 'x' THEN a.SKU END) AS 'Low Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'y' THEN a.SKU END) AS 'Medium Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'z' THEN a.SKU END) AS 'High Variance',
    COUNT(DISTINCT (a.SKU )) AS 'Total SKU'

FROM
    {tbl} a
LEFT JOIN 
    skuall b ON b.SKU = a.SKU 
 -- WHERE 
    
   
GROUP BY 
     b.SkuStrat
    
    union all
    SELECT
	"All" Business_Unit,
    "All" DC,
    "All" Segment,
    "Total" as 'SKU Type',
    COUNT(DISTINCT CASE WHEN b.xyz = 'x' THEN a.SKU END) AS 'Low Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'y' THEN a.SKU END) AS 'Medium Variance',
    COUNT(DISTINCT CASE WHEN b.xyz = 'z' THEN a.SKU END) AS 'High Variance',
    COUNT(DISTINCT (a.SKU )) AS 'Total SKU'

FROM
    {tbl} a
LEFT JOIN 
    skuall b ON b.SKU = a.SKU 
-- WHERE 
    
        
    -- ORDER BY
     --   1,5 DESC
        ;
"""


def sku_pickAffin_data():
    tbl = Shared.projtbl
    tblib = Shared.ibtbl
    projtbl = f"client_data.{Shared.project}"
    ibtbl = f"client_data.{Shared.project_inbound}"
    PickAffin = f""" 
    
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
    return PickAffin


##start_date, end_date = Shared.get_dates()
##print(start_date,end_date)


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
     {tbl} a
 WHERE 
 a.Order_Date between '{Shared.stdt}' and '{Shared.enddt}'

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

PickAffin = f""" 

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
##start_date, end_date = Shared.get_dates()
##print(start_date,end_date)


PickAffinIB = f""" 
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
