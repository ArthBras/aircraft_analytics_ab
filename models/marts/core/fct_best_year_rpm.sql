WITH rpm_data AS (
    SELECT
        "Date",
        "Airline_Code",
        COALESCE("RPM_Domestic", 0) AS RPM_Domestic,
        COALESCE("RPM_International", 0) AS RPM_International,
        COALESCE("RPM_Domestic", 0) + COALESCE(RPM_International, 0) AS RPM_Total
    FROM {{ ref('stg_flight_summary_data') }}
),

annual_rpm AS (
    SELECT
        "Airline_Code",
        EXTRACT(YEAR FROM CAST("Date" AS DATE)) AS Year,
        SUM(RPM_Domestic) AS Total_RPM_Domestic,
        SUM(RPM_International) AS Total_RPM_International,
        SUM(RPM_Total) AS Total_RPM
    FROM rpm_data
    GROUP BY "Airline_Code", Year
),

best_year_rpm AS (
    SELECT
        "Airline_Code",
        Year,
        Total_RPM_Domestic,
        Total_RPM_International,
        Total_RPM,
        ROW_NUMBER() OVER (PARTITION BY "Airline_Code" ORDER BY Total_RPM_Domestic DESC) AS rank_domestic,
        ROW_NUMBER() OVER (PARTITION BY "Airline_Code" ORDER BY Total_RPM_International DESC) AS rank_international,
        ROW_NUMBER() OVER (PARTITION BY "Airline_Code" ORDER BY Total_RPM DESC) AS rank_total
    FROM annual_rpm
)

SELECT
    "Airline_Code",
    MAX(CASE WHEN rank_domestic = 1 THEN Year END) AS Best_Year_Domestic,
    MAX(CASE WHEN rank_international = 1 THEN Year END) AS Best_Year_International,
    MAX(CASE WHEN rank_total = 1 THEN Year END) AS Best_Year_Total
FROM best_year_rpm
GROUP BY "Airline_Code"
