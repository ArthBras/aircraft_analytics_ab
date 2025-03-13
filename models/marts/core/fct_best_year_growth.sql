WITH annual_asm AS (
    SELECT
        "Airline_Code",
        EXTRACT(YEAR FROM CAST("Date" AS DATE)) AS Year,
        AVG("ASM_Domestic") AS Avg_ASM_Domestic
    FROM {{ ref('stg_flight_summary_data') }}
    GROUP BY "Airline_Code", Year
),

best_year_growth AS (
    SELECT
        "Airline_Code",
        Year,
        Avg_ASM_Domestic,
        ROW_NUMBER() OVER (PARTITION BY "Airline_Code" ORDER BY Avg_ASM_Domestic DESC) AS rank
    FROM annual_asm
)

SELECT
    "Airline_Code",
    Year AS Best_Year_Growth,
    Avg_ASM_Domestic
FROM best_year_growth
WHERE rank = 1
