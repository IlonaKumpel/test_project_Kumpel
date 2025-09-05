

SELECT
    JSONExtractString(data, 'name')   AS name,
    JSONExtractString(data, 'craft')  AS craft,
    _inserted_at
FROM `demo`.`raw_astros`