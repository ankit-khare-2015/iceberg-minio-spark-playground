from pyspark.sql import functions as F
from common import spark_session

spark = spark_session("step2_partition_evolve")

# Add a date column and evolve partition spec
spark.sql("ALTER TABLE local.airline.flights ADD COLUMN dep_date DATE")
spark.sql("UPDATE local.airline.flights SET dep_date = DATE(scheduled_dep_ts)")

# Add partition field (partition evolution). Affects new data going forward.
spark.sql("ALTER TABLE local.airline.flights ADD PARTITION FIELD dep_date")

# Insert some more rows (will be written under new partition spec)
spark.sql("""INSERT INTO local.airline.flights VALUES
('LH1111','LH','TXL','CDG',timestamp('2025-08-14 08:00:00'),timestamp('2025-08-14 07:55:00'), DATE('2025-08-14')),
('LH2222','LH','CDG','TXL',timestamp('2025-08-14 19:20:00'),timestamp('2025-08-14 19:44:00'), DATE('2025-08-14'))
""")

df = spark.sql("SELECT flight_id, origin, dest, dep_date FROM local.airline.flights ORDER BY dep_date, flight_id")
df.show(truncate=False)
print("Step 2 done: partition evolution applied and new rows inserted.")
