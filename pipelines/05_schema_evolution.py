from common import spark_session

spark = spark_session("step5_schema_evolution")

# Add a column, rename a column, and drop a column
spark.sql("ALTER TABLE local.airline.flights ADD COLUMN aircraft_tail STRING")
spark.sql("ALTER TABLE local.airline.flights RENAME COLUMN dest TO destination")
spark.sql("ALTER TABLE local.airline.flights DROP COLUMN carrier")

spark.sql("INSERT INTO local.airline.flights VALUES ('LH3333','FRA','LHR',timestamp('2025-08-15 07:30:00'),timestamp('2025-08-15 07:37:00'), DATE('2025-08-15'), 'D-AIAB')")

df = spark.sql("SELECT * FROM local.airline.flights ORDER BY scheduled_dep_ts")
df.show(truncate=False)
print("Step 5 done: schema evolution (add/rename/drop) executed.")
