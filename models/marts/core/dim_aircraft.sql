WITH flight_counts AS (
    SELECT
        "Aircraft_Id",
        COUNT(*) AS flight_count
    FROM {{ ref('stg_individual_flights') }}
    GROUP BY "Aircraft_Id"
),

aircraft_details AS (
    SELECT
        a."index",
        a."Aircraft_Id",
        a."Aircraft_Type",
        a."Mass",
        a."Length",
        a."Cost",
        a."Capacity",
        fc.flight_count
    FROM {{ ref('stg_aircraft') }} as a
    LEFT JOIN flight_counts as fc
    ON a."Aircraft_Id" = fc."Aircraft_Id"
)

SELECT * FROM aircraft_details
