from pyspark.sql import functions as F
from common import spark_session

spark = spark_session("step3_merge_upsert")

# Simulate a late correction for LH9001 (earlier actual departure)
spark.sql("""MERGE INTO local.airline.flights t
USING (
  SELECT 'LH9001' AS flight_id, 'LH' AS carrier, 'FRA' AS origin, 'BER' AS dest,
         timestamp('2025-08-13 09:10:00') AS scheduled_dep_ts,
         timestamp('2025-08-13 09:20:00') AS actual_dep_ts,
         DATE('2025-08-13') AS dep_date
) s
ON t.flight_id = s.flight_id AND DATE(t.scheduled_dep_ts) = s.dep_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

df = spark.sql("""SELECT flight_id, scheduled_dep_ts, actual_dep_ts
FROM local.airline.flights
WHERE flight_id='LH9001'
""")
df.show(truncate=False)
print("Step 3 done: MERGE upsert executed.")
