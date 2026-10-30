EXEC(N'CREATE TYPE __SCHEMA__.job_item_inputs AS TABLE(id nvarchar(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,item_key nvarchar(1024),fingerprint nvarchar(64) COLLATE Latin1_General_100_BIN2)');
UPDATE __SCHEMA__.job_schema_version SET version=3 WHERE singleton=1;
