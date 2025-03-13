WITH source AS (
    SELECT * FROM {{ source('Aircraft_Analytics', 'aircraft') }}
)

SELECT
    "index",
    "Aircraft_Id",
    "Aircraft_Type",
    "Mass",
    "Length",
    "Cost",
    "Capacity"
FROM source
