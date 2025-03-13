WITH source AS (
    SELECT * FROM {{ source('Aircraft_Analytics', 'airlines') }}
)

SELECT
    "index",
    "Airline_Code",
    "Airline_Name",
    "Description",
    "Market_Cap",
    "Employees",
    "Age"
FROM source
