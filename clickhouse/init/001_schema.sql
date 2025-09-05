CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.raw_astros
(
    data String,                                 
    payload_hash FixedString(64),                
    _inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY (payload_hash);

CREATE TABLE IF NOT EXISTS demo.people
(
    craft String,
    name String,
    _inserted_at DateTime
)
ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY (craft, name);

CREATE MATERIALIZED VIEW IF NOT EXISTS demo.mv_raw_to_people
TO demo.people
AS
SELECT
    JSONExtractString(person, 'craft') AS craft,
    JSONExtractString(person, 'name')  AS name,
    _inserted_at
FROM demo.raw_astros
ARRAY JOIN JSONExtractArrayRaw(data, 'people') AS person;
