{{ config(materialized='table') }}

SELECT
    JSONExtractString(data, 'name')   AS name,
    JSONExtractString(data, 'craft')  AS craft,
    _inserted_at
FROM {{ source('demo', 'raw_astros') }}
