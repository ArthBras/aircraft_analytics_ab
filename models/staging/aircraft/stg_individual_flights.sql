WITH source AS (
    SELECT * FROM {{ source('Aircraft_Analytics', 'individual_flights') }}
)

SELECT
    "index",
    "Flight_Id",
    "Airline_Code",
    "Departure_Airport_Code",
    "Destination_Airport_Code",
    "Aircraft_Id"
FROM source
