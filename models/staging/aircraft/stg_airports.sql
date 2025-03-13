WITH source AS (
    SELECT * FROM {{ source('Aircraft_Analytics', 'airports') }}
)

SELECT
    "index",
    "Airport_Code",
    "Airport_Name",
    "Airport_Employees",
    "Airport_Size"
FROM source
