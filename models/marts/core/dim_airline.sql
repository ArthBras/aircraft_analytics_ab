WITH source AS (
    SELECT * FROM {{ source('Aircraft_Analytics', 'airlines') }}
),

best_year_growth AS (
    SELECT
        "Airline_Code",
        Best_Year_Growth,
        Avg_ASM_Domestic
    FROM {{ ref('fct_best_year_growth') }}
),

best_year_rpm AS (
    SELECT
        "Airline_Code",
        Best_Year_Domestic,
        Best_Year_International,
        Best_Year_Total
    FROM {{ ref('fct_best_year_rpm') }}
)

SELECT
    s."index",
    s."Airline_Code",
    s."Airline_Name",
    s."Description",
    s."Market_Cap",
    s."Employees",
    s."Age",
    g.Best_Year_Growth,
    g.Avg_ASM_Domestic AS Best_Avg_ASM_Domestic,
    r.Best_Year_Domestic,
    r.Best_Year_International,
    r.Best_Year_Total
FROM source s
LEFT JOIN best_year_growth g
ON s."Airline_Code" = g."Airline_Code"
LEFT JOIN best_year_rpm r
ON s."Airline_Code" = r."Airline_Code"
