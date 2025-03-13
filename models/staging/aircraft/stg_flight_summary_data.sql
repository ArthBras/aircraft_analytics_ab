WITH source AS (
    SELECT * FROM {{ source('Aircraft_Analytics', 'flight_summary_data') }}
)

SELECT
    "index",
    "Date",
    "ASM_Domestic",
    "ASM_International",
    "Flights_Domestic",
    "Flights_International",
    "Passengers_Domestic",
    "Passengers_International",
    "RPM_Domestic",
    "RPM_International",
    "Airline_Code",
    "Airport_Code"
FROM source
