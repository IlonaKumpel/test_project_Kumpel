
  
    
    
    
        
         


        insert into `demo`.`people__dbt_backup`
        ("name", "craft", "_inserted_at")

SELECT
    JSONExtractString(data, 'name')   AS name,
    JSONExtractString(data, 'craft')  AS craft,
    _inserted_at
FROM `demo`.`raw_astros`
  