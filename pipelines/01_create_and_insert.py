from pyspark.sql import functions as F
from common import spark_session

spark = spark_session("step1_create_insert")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.airline")

spark.sql("""CREATE TABLE IF NOT EXISTS local.airline.flights (
  flight_id STRING,
  carrier   STRING,
  origin    STRING,
  dest      STRING,
  scheduled_dep_ts TIMESTAMP,
  actual_dep_ts    TIMESTAMP
) USING iceberg
""")

spark.sql("""INSERT INTO local.airline.flights VALUES
('LH1234','LH','FRA','BOM',timestamp('2025-08-13 10:30:00'),timestamp('2025-08-13 10:29:00')),
('LH5678','LH','MUC','LHR',timestamp('2025-08-13 12:00:00'),timestamp('2025-08-13 12:07:00')),
('LH9001','LH','FRA','BER',timestamp('2025-08-13 09:10:00'),timestamp('2025-08-13 09:35:00'))
""")

df = spark.sql("SELECT * FROM local.airline.flights ORDER BY scheduled_dep_ts")
df.show(truncate=False)
print("Step 1 done: created table and inserted 3 rows.")
