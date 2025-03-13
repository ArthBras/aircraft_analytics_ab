WITH flight_passengers AS (
    SELECT
        "Departure_Airport_Code",
        "Destination_Airport_Code",
        SUM(a."Capacity") AS total_passengers
    FROM {{ ref('stg_individual_flights') }} f
    JOIN {{ ref('stg_aircraft') }} a
    ON f."Aircraft_Id" = a."Aircraft_Id"
    GROUP BY "Departure_Airport_Code", "Destination_Airport_Code"
),

airport_passengers AS (
    SELECT
        "Departure_Airport_Code" AS "Airport_Code",
        SUM(total_passengers) / 2 AS total_passengers
    FROM flight_passengers
    GROUP BY "Departure_Airport_Code"

    UNION ALL

    SELECT
        "Destination_Airport_Code" AS "Airport_Code",
        SUM(total_passengers) / 2 AS total_passengers
    FROM flight_passengers
    GROUP BY "Destination_Airport_Code"
)

SELECT
    ap."index",
    ap."Airport_Code",
    ap."Airport_Name",
    ap."Airport_Employees",
    ap."Airport_Size",
    COALESCE(SUM(ap_pass.total_passengers), 0) AS total_passengers
FROM {{ ref('stg_airports') }} ap
LEFT JOIN airport_passengers ap_pass
ON ap."Airport_Code" = ap_pass."Airport_Code"
GROUP BY ap."index", ap."Airport_Code", ap."Airport_Name", ap."Airport_Employees", ap."Airport_Size"
