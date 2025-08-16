-- Run inside spark-sql if you prefer interactive queries
SELECT * FROM local.airline.flights LIMIT 10;

CALL local.system.snapshots('local.airline.flights');

SELECT * FROM local.airline.flights VERSION AS OF <snapshot_id>;
